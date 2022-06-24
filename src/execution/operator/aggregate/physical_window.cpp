#include "duckdb/execution/operator/aggregate/physical_window.hpp"

#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/comparison_operators.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/windows_undefs.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/window_segment_tree.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/main/config.hpp"

#include <algorithm>
#include <cmath>
#include <numeric>

namespace duckdb {

using counts_t = std::vector<size_t>;

class WindowGlobalHashGroup {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	using LocalSortStatePtr = unique_ptr<LocalSortState>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	WindowGlobalHashGroup(BufferManager &buffer_manager, const Orders &orders, const Types &payload_types,
	                      idx_t max_mem)
	    : memory_per_thread(max_mem) {
		Orders orders_copy;
		for (const auto &order : orders) {
			sort_types.emplace_back(order.expression->return_type);
			orders_copy.emplace_back(order.Copy());
		}

		RowLayout payload_layout;
		payload_layout.Initialize(payload_types);
		global_sort = make_unique<GlobalSortState>(buffer_manager, orders_copy, payload_layout);
	}

	void Sink(DataChunk &sort_buffer, DataChunk &payload_buffer) {
		lock_guard<mutex> guard(lock);
		if (!local_sort) {
			local_sort = make_unique<LocalSortState>();
			local_sort->Initialize(*global_sort, global_sort->buffer_manager);
		}

		local_sort->SinkChunk(sort_buffer, payload_buffer);
	}

	void Sort() {
		if (local_sort) {
			global_sort->AddLocalState(*local_sort);
			local_sort.reset();
		}

		global_sort->PrepareMergePhase();
		while (global_sort->sorted_blocks.size() > 1) {
			global_sort->InitializeMergeRound();
			MergeSorter merge_sorter(*global_sort, global_sort->buffer_manager);
			merge_sorter.PerformInMergeRound();
			global_sort->CompleteMergeRound(true);
		}
	}

	Types sort_types;
	mutex lock;
	GlobalSortStatePtr global_sort;
	LocalSortStatePtr local_sort;
	idx_t memory_per_thread;

private:
};

//	Global sink state
class WindowGlobalSinkState : public GlobalSinkState {
public:
	using HashGroupPtr = unique_ptr<WindowGlobalHashGroup>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	WindowGlobalSinkState(const PhysicalWindow &op_p, ClientContext &context)
	    : op(op_p), buffer_manager(BufferManager::GetBufferManager(context)), memory_per_thread(0),
	      mode(DBConfig::GetConfig(context).window_mode) {

		D_ASSERT(op.select_list[0]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[0].get());

		// we sort by both 1) partition by expression list and 2) order by expressions
		payload_types = op.children[0]->types;

		for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
			auto &pexpr = wexpr->partitions[prt_idx];
			payload_types.push_back(pexpr->return_type);

			if (wexpr->partitions_stats.empty() || !wexpr->partitions_stats[prt_idx]) {
				orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(), nullptr);
			} else {
				orders.emplace_back(OrderType::ASCENDING, OrderByNullType::NULLS_FIRST, pexpr->Copy(),
				                    wexpr->partitions_stats[prt_idx]->Copy());
			}
		}

		for (const auto &order : wexpr->orders) {
			auto &oexpr = order.expression;
			payload_types.push_back(oexpr->return_type);
			orders.emplace_back(order.Copy());
		}

		// Memory usage per thread should scale with max mem / num threads
		// We take 1/4th of this, to be conservative
		idx_t max_memory = buffer_manager.GetMaxMemory();
		idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		memory_per_thread = (max_memory / num_threads) / 4;
	}

	WindowGlobalHashGroup *GetHashGroup(idx_t group) {
		lock_guard<mutex> guard(lock);
		if (group >= hash_groups.size()) {
			hash_groups.resize(group + 1);
		}

		auto &hash_group = hash_groups[group];
		if (!hash_group) {
			hash_group = make_unique<WindowGlobalHashGroup>(buffer_manager, orders, payload_types, memory_per_thread);
		}

		return hash_group.get();
	}

	const PhysicalWindow &op;
	BufferManager &buffer_manager;
	mutex lock;
	Orders orders;
	Types payload_types;
	vector<HashGroupPtr> hash_groups;
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> strings;
	idx_t memory_per_thread;
	WindowAggregationMode mode;
};

//	Per-thread hash group
class WindowLocalHashGroup {
public:
	using LocalSortStatePtr = unique_ptr<LocalSortState>;
	using DataChunkPtr = unique_ptr<DataChunk>;

	explicit WindowLocalHashGroup(WindowGlobalHashGroup &global_group_p) : global_group(global_group_p) {
	}

	LocalSortState *GetLocalSortState();

	void Flush();
	void Sink(DataChunk &sort_chunk, DataChunk &payload_chunk, SelectionVector *sel, const idx_t count);
	void Combine();

	WindowGlobalHashGroup &global_group;
	LocalSortStatePtr local_sort;
	DataChunkPtr sort_buffer;
	DataChunkPtr payload_buffer;
};

LocalSortState *WindowLocalHashGroup::GetLocalSortState() {
	if (!local_sort) {
		local_sort = make_unique<LocalSortState>();
		local_sort->Initialize(*global_group.global_sort, global_group.global_sort->buffer_manager);
	}

	return local_sort.get();
}

void WindowLocalHashGroup::Flush() {
	if (!payload_buffer || payload_buffer->size() == 0) {
		return;
	}

	GetLocalSortState()->SinkChunk(*sort_buffer, *payload_buffer);
	sort_buffer->Reset();
	payload_buffer->Reset();

	if (local_sort->SizeInBytes() >= global_group.memory_per_thread) {
		local_sort->Sort(*global_group.global_sort, true);
	}
}

void WindowLocalHashGroup::Sink(DataChunk &sort_chunk, DataChunk &payload_chunk, SelectionVector *sel,
                                const idx_t count) {

	if (payload_buffer && payload_buffer->size() + count > STANDARD_VECTOR_SIZE) {
		Flush();
	}

	if (sel) {
		if (!sort_buffer) {
			sort_buffer = make_unique<DataChunk>();
			sort_buffer->Initialize(global_group.sort_types);
		}
		sort_buffer->Append(sort_chunk, false, sel, count);

		if (!payload_buffer) {
			payload_buffer = make_unique<DataChunk>();
			payload_buffer->Initialize(global_group.global_sort->payload_layout.GetTypes());
		}
		payload_buffer->Append(payload_chunk, false, sel, count);
	} else {
		GetLocalSortState()->SinkChunk(sort_chunk, payload_chunk);
	}
}

void WindowLocalHashGroup::Combine() {
	//	Merge this group into the global group
	if (local_sort) {
		//	We had enough data for a local sort block
		Flush();
		global_group.global_sort->AddLocalState(*local_sort);
	} else if (payload_buffer && payload_buffer->size()) {
		//	Only buffers, so pass them along
		global_group.Sink(*sort_buffer, *payload_buffer);
	}
}

//	Per-thread sink state
class WindowLocalSinkState : public LocalSinkState {
public:
	using LocalHashGroupPtr = unique_ptr<WindowLocalHashGroup>;

	WindowLocalSinkState(const PhysicalWindow &op_p, BoundWindowExpression *wexpr, const unsigned partition_bits = 10)
	    : op(op_p), partition_cols(wexpr->partitions.size()), partition_count(size_t(1) << partition_bits),
	      hash_vector(LogicalTypeId::UBIGINT), sel(STANDARD_VECTOR_SIZE) {

		// we sort by both 1) partition by expression list and 2) order by expressions
		vector<LogicalType> payload_types = op.children[0]->types;
		vector<LogicalType> over_types;
		for (idx_t prt_idx = 0; prt_idx < wexpr->partitions.size(); prt_idx++) {
			auto &pexpr = wexpr->partitions[prt_idx];
			over_types.push_back(pexpr->return_type);
			payload_types.push_back(pexpr->return_type);
			executor.AddExpression(*pexpr);
		}

		for (const auto &order : wexpr->orders) {
			auto &oexpr = order.expression;
			over_types.push_back(oexpr->return_type);
			payload_types.push_back(oexpr->return_type);
			executor.AddExpression(*oexpr);
		}

		payload_chunk.Initialize(payload_types);
		payload_layout.Initialize(payload_types);

		if (!over_types.empty()) {
			over_chunk.Initialize(over_types);
			local_groups.resize(wexpr->partitions.empty() ? 1 : partition_count);
		}
	}

	const PhysicalWindow &op;

	// Over
	ExpressionExecutor executor;
	DataChunk over_chunk;

	// Partitioning
	const idx_t partition_cols;
	const size_t partition_count;
	counts_t counts;
	counts_t offsets;
	Vector hash_vector;
	SelectionVector sel;
	vector<LocalHashGroupPtr> local_groups;

	// Sorting
	RowLayout payload_layout;
	DataChunk payload_chunk;

	// OVER() (no sorting)
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> strings;

	void Over(DataChunk &input_chunk);
	void Hash();
	void Sink(DataChunk &input_chunk, WindowGlobalSinkState &gstate);
	idx_t FlushRagged();
	void Combine(WindowGlobalSinkState &gstate);
};

void WindowLocalSinkState::Over(DataChunk &input_chunk) {
	if (over_chunk.ColumnCount() > 0) {
		executor.Execute(input_chunk, over_chunk);
		over_chunk.Verify();
	}
}

void WindowLocalSinkState::Hash() {
	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)
	if (over_chunk.ColumnCount() == 0) {
		return;
	}

	const auto count = over_chunk.size();
	auto hashes = FlatVector::GetData<hash_t>(hash_vector);
	if (partition_cols) {
		// First pass: count bins sizes
		counts.resize(0);
		counts.resize(partition_count, 0);

		VectorOperations::Hash(over_chunk.data[0], hash_vector, count);
		for (idx_t prt_idx = 1; prt_idx < partition_cols; ++prt_idx) {
			VectorOperations::CombineHash(hash_vector, over_chunk.data[prt_idx], count);
		}

		const auto group_mask = hash_t(counts.size() - 1);
		if (hash_vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			const auto group = (hashes[0] & group_mask);
			counts[group] = count;
			for (idx_t i = 0; i < count; ++i) {
				sel.set_index(i, i);
			}
		} else {
			for (idx_t i = 0; i < count; ++i) {
				const auto bin = (hashes[i] & group_mask);
				++counts[bin];
			}

			// Second pass: Build sequential selections
			offsets.resize(counts.size());
			size_t offset = 0;
			for (size_t c = 0; c < counts.size(); ++c) {
				offsets[c] = offset;
				offset += counts[c];
			}

			for (idx_t i = 0; i < count; ++i) {
				const auto group = (hashes[i] & group_mask);
				auto &group_idx = offsets[group];
				sel.set_index(group_idx++, i);
			}
		}
	} else {
		counts.resize(1, count);
	}
}

void WindowLocalSinkState::Sink(DataChunk &input_chunk, WindowGlobalSinkState &gstate) {
	for (idx_t col_idx = 0; col_idx < input_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[col_idx].Reference(input_chunk.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < over_chunk.ColumnCount(); ++col_idx) {
		payload_chunk.data[input_chunk.ColumnCount() + col_idx].Reference(over_chunk.data[col_idx]);
	}
	payload_chunk.SetCardinality(input_chunk);

	if (over_chunk.ColumnCount() == 0) {
		//	No sorts, so build row chunks
		if (!rows) {
			const auto entry_size = payload_layout.GetRowWidth();
			const auto capacity = MaxValue<idx_t>(STANDARD_VECTOR_SIZE, (Storage::BLOCK_SIZE / entry_size) + 1);
			rows = make_unique<RowDataCollection>(gstate.buffer_manager, capacity, entry_size);
			strings = make_unique<RowDataCollection>(gstate.buffer_manager, (idx_t)Storage::BLOCK_SIZE, 1, true);
		}
		const auto row_count = payload_chunk.size();
		const auto row_sel = FlatVector::IncrementalSelectionVector();
		Vector addresses(LogicalType::POINTER);
		auto key_locations = FlatVector::GetData<data_ptr_t>(addresses);
		auto handles = rows->Build(row_count, key_locations, nullptr, row_sel);
		vector<VectorData> payload_data;
		payload_data.reserve(payload_chunk.ColumnCount());
		for (idx_t i = 0; i < payload_chunk.ColumnCount(); i++) {
			VectorData pdata;
			payload_chunk.data[i].Orrify(row_count, pdata);
			payload_data.emplace_back(move(pdata));
		}
		RowOperations::Scatter(payload_chunk, payload_data.data(), payload_layout, addresses, *strings, *row_sel,
		                       row_count);
		return;
	}

	idx_t group_offset = 0;
	for (size_t group = 0; group < counts.size(); ++group) {
		const auto group_size = counts[group];
		if (group_size) {
			auto &local_group = local_groups[group];
			if (!local_group) {
				auto global_group = gstate.GetHashGroup(group);
				local_group = make_unique<WindowLocalHashGroup>(*global_group);
			}

			if (counts.size() == 1) {
				local_group->Sink(over_chunk, payload_chunk, nullptr, group_size);
			} else {
				SelectionVector psel(sel.data() + group_offset);
				local_group->Sink(over_chunk, payload_chunk, &psel, group_size);
				group_offset += group_size;
			}
		}
	}
}

void WindowLocalSinkState::Combine(WindowGlobalSinkState &gstate) {
	lock_guard<mutex> glock(gstate.lock);

	// OVER()
	if (over_chunk.ColumnCount() == 0) {
		if (gstate.rows) {
			gstate.rows->Merge(*rows);
			gstate.strings->Merge(*strings);
			rows.reset();
			strings.reset();
		} else {
			gstate.rows = move(rows);
			gstate.strings = move(strings);
		}
		return;
	}

	// Binned data
	for (size_t group = 0; group < local_groups.size(); ++group) {
		auto &local_group = local_groups[group];
		if (local_group) {
			local_group->Combine();
			local_group.reset();
		}
	}
}

// Per-thread read state
class WindowLocalSourceState : public LocalSourceState {
public:
	using HashGroupPtr = unique_ptr<WindowGlobalHashGroup>;

	WindowLocalSourceState(const PhysicalWindow &op, ExecutionContext &context) : position(0) {
	}

	HashGroupPtr hash_group;

	//! The generated input chunks
	ChunkCollection chunks;
	//! The generated output chunks
	ChunkCollection window_results;
	//! The read partition
	idx_t hash_bin;
	//! The read cursor
	idx_t position;
};

// this implements a sorted window functions variant
PhysicalWindow::PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list,
                               idx_t estimated_cardinality, PhysicalOperatorType type)
    : PhysicalOperator(type, move(types), estimated_cardinality), select_list(move(select_list)) {
}

template <typename INPUT_TYPE>
struct ChunkIterator {

	ChunkIterator(ChunkCollection &collection, const idx_t col_idx)
	    : collection(collection), col_idx(col_idx), chunk_begin(0), chunk_end(0), ch_idx(0), data(nullptr),
	      validity(nullptr) {
		Update(0);
	}

	inline void Update(idx_t r) {
		if (r >= chunk_end) {
			ch_idx = collection.LocateChunk(r);
			auto &ch = collection.GetChunk(ch_idx);
			chunk_begin = ch_idx * STANDARD_VECTOR_SIZE;
			chunk_end = chunk_begin + ch.size();
			auto &vector = ch.data[col_idx];
			data = FlatVector::GetData<INPUT_TYPE>(vector);
			validity = &FlatVector::Validity(vector);
		}
	}

	inline bool IsValid(idx_t r) {
		return validity->RowIsValid(r - chunk_begin);
	}

	inline INPUT_TYPE GetValue(idx_t r) {
		return data[r - chunk_begin];
	}

private:
	ChunkCollection &collection;
	idx_t col_idx;
	idx_t chunk_begin;
	idx_t chunk_end;
	idx_t ch_idx;
	const INPUT_TYPE *data;
	ValidityMask *validity;
};

template <typename INPUT_TYPE>
static void MaskTypedColumn(ValidityMask &mask, ChunkCollection &over_collection, const idx_t c) {
	ChunkIterator<INPUT_TYPE> ci(over_collection, c);

	//	Record the first value
	idx_t r = 0;
	auto prev_valid = ci.IsValid(r);
	auto prev = ci.GetValue(r);

	//	Process complete blocks
	const auto count = over_collection.Count();
	const auto entry_count = mask.EntryCount(count);
	for (idx_t entry_idx = 0; entry_idx < entry_count; ++entry_idx) {
		auto validity_entry = mask.GetValidityEntry(entry_idx);

		//	Skip the block if it is all boundaries.
		idx_t next = MinValue<idx_t>(r + ValidityMask::BITS_PER_VALUE, count);
		if (ValidityMask::AllValid(validity_entry)) {
			r = next;
			continue;
		}

		//	Scan the rows in the complete block
		idx_t start = r;
		for (; r < next; ++r) {
			//	Update the chunk for this row
			ci.Update(r);

			auto curr_valid = ci.IsValid(r);
			auto curr = ci.GetValue(r);
			if (!ValidityMask::RowIsValid(validity_entry, r - start)) {
				if (curr_valid != prev_valid || (curr_valid && !Equals::Operation(curr, prev))) {
					mask.SetValidUnsafe(r);
				}
			}
			prev_valid = curr_valid;
			prev = curr;
		}
	}
}

static void MaskColumn(ValidityMask &mask, ChunkCollection &over_collection, const idx_t c) {
	auto &vector = over_collection.GetChunk(0).data[c];
	switch (vector.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		MaskTypedColumn<int8_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT16:
		MaskTypedColumn<int16_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT32:
		MaskTypedColumn<int32_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT64:
		MaskTypedColumn<int64_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT8:
		MaskTypedColumn<uint8_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT16:
		MaskTypedColumn<uint16_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT32:
		MaskTypedColumn<uint32_t>(mask, over_collection, c);
		break;
	case PhysicalType::UINT64:
		MaskTypedColumn<uint64_t>(mask, over_collection, c);
		break;
	case PhysicalType::INT128:
		MaskTypedColumn<hugeint_t>(mask, over_collection, c);
		break;
	case PhysicalType::FLOAT:
		MaskTypedColumn<float>(mask, over_collection, c);
		break;
	case PhysicalType::DOUBLE:
		MaskTypedColumn<double>(mask, over_collection, c);
		break;
	case PhysicalType::VARCHAR:
		MaskTypedColumn<string_t>(mask, over_collection, c);
		break;
	case PhysicalType::INTERVAL:
		MaskTypedColumn<interval_t>(mask, over_collection, c);
		break;
	default:
		throw NotImplementedException("Type for comparison");
		break;
	}
}

static idx_t FindNextStart(const ValidityMask &mask, idx_t l, const idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = MinValue(l + n - 1, r);
		n -= MinValue(n, r - l);
		return start;
	}

	while (l < r) {
		//	If l is aligned with the start of a block, and the block is blank, then skip forward one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(l, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && !shift) {
			l += ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop over the block
		for (; shift < ValidityMask::BITS_PER_VALUE && l < r; ++shift, ++l) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MinValue(l, r);
			}
		}
	}

	//	Didn't find a start so return the end of the range
	return r;
}

static idx_t FindPrevStart(const ValidityMask &mask, const idx_t l, idx_t r, idx_t &n) {
	if (mask.AllValid()) {
		auto start = (r <= l + n) ? l : r - n;
		n -= r - start;
		return start;
	}

	while (l < r) {
		// If r is aligned with the start of a block, and the previous block is blank,
		// then skip backwards one block.
		idx_t entry_idx;
		idx_t shift;
		mask.GetEntryIndex(r - 1, entry_idx, shift);

		const auto block = mask.GetValidityEntry(entry_idx);
		if (mask.NoneValid(block) && (shift + 1 == ValidityMask::BITS_PER_VALUE)) {
			// r is nonzero (> l) and word aligned, so this will not underflow.
			r -= ValidityMask::BITS_PER_VALUE;
			continue;
		}

		// Loop backwards over the block
		// shift is probing r-1 >= l >= 0
		for (++shift; shift-- > 0; --r) {
			if (mask.RowIsValid(block, shift) && --n == 0) {
				return MaxValue(l, r - 1);
			}
		}
	}

	//	Didn't find a start so return the start of the range
	return l;
}

static void PrepareInputExpressions(Expression **exprs, idx_t expr_count, ChunkCollection &output,
                                    ExpressionExecutor &executor, DataChunk &chunk) {
	if (expr_count == 0) {
		return;
	}

	vector<LogicalType> types;
	for (idx_t expr_idx = 0; expr_idx < expr_count; ++expr_idx) {
		types.push_back(exprs[expr_idx]->return_type);
		executor.AddExpression(*exprs[expr_idx]);
	}

	if (!types.empty()) {
		chunk.Initialize(types);
	}
}

static void PrepareInputExpression(Expression *expr, ChunkCollection &output, ExpressionExecutor &executor,
                                   DataChunk &chunk) {
	PrepareInputExpressions(&expr, 1, output, executor, chunk);
}

struct WindowInputExpression {
	explicit WindowInputExpression(Expression *expr_p) : expr(expr_p), scalar(false) {
		if (expr) {
			PrepareInputExpression(expr, collection, executor, chunk);
			scalar = expr->IsScalar();
		}
	}

	void Execute(DataChunk &input_chunk) {
		if (expr && (!scalar || collection.Count() == 0)) {
			chunk.Reset();
			executor.Execute(input_chunk, chunk);
			chunk.Verify();
			collection.Append(chunk);
		}
	}

	Expression *expr;
	bool scalar;
	ChunkCollection collection;
	ExpressionExecutor executor;
	DataChunk chunk;
};

static void SortCollectionForPartition(WindowLocalSourceState &state, WindowGlobalSinkState &gstate,
                                       const hash_t hash_bin) {
	state.hash_group = move(gstate.hash_groups[hash_bin]);
	state.hash_group->Sort();
}

static void ScanRowCollection(RowDataCollection &rows, ChunkCollection &cols, const vector<LogicalType> &types) {
	const auto tuple_size = rows.entry_size;

	Vector addresses(LogicalType::POINTER);
	auto data_pointers = FlatVector::GetData<data_ptr_t>(addresses);

	RowLayout layout;
	layout.Initialize(types);

	DataChunk result;
	result.Initialize(types);
	for (idx_t block_idx = 0; block_idx < rows.blocks.size(); ++block_idx) {
		auto &block = rows.blocks[block_idx];
		auto handle = rows.buffer_manager.Pin(block.block);
		auto read_ptr = handle->Ptr();

		for (idx_t block_pos = 0; block_pos < block.count;) {
			const auto remaining = block.count - block_pos;
			const auto this_n = MinValue((idx_t)STANDARD_VECTOR_SIZE, remaining);

			auto row_offset = block_pos * tuple_size;
			D_ASSERT(row_offset + tuple_size <= Storage::BLOCK_SIZE);
			for (idx_t i = 0; i < this_n; i++) {
				data_pointers[i] = read_ptr + row_offset;
				row_offset += tuple_size;
			}

			result.Reset();
			result.SetCardinality(this_n);
			for (idx_t i = 0; i < result.ColumnCount(); i++) {
				auto &column = result.data[i];
				const auto col_offset = layout.GetOffsets()[i];
				RowOperations::Gather(addresses, *FlatVector::IncrementalSelectionVector(), column,
				                      *FlatVector::IncrementalSelectionVector(), result.size(), col_offset, i);
			}
			cols.Append(result);

			block_pos += this_n;
		}
	}
}

static void ScanSortedPartition(WindowLocalSourceState &state, ChunkCollection &input,
                                const vector<LogicalType> &input_types, ChunkCollection &over,
                                const vector<LogicalType> &over_types) {
	auto &global_sort_state = *state.hash_group->global_sort;
	if (global_sort_state.sorted_blocks.empty()) {
		return;
	}

	auto payload_types = input_types;
	payload_types.insert(payload_types.end(), over_types.begin(), over_types.end());

	// scan the sorted row data
	D_ASSERT(global_sort_state.sorted_blocks.size() == 1);
	PayloadScanner scanner(*global_sort_state.sorted_blocks[0]->payload_data, global_sort_state);
	for (;;) {
		DataChunk payload_chunk;
		payload_chunk.Initialize(payload_types);
		payload_chunk.SetCardinality(0);
		scanner.Scan(payload_chunk);
		if (payload_chunk.size() == 0) {
			break;
		}

		// split into two
		DataChunk over_chunk;
		payload_chunk.Split(over_chunk, input_types.size());

		// append back to collection
		input.Append(payload_chunk);
		over.Append(over_chunk);
	}

	state.hash_group.reset();
}

static inline bool BoundaryNeedsPeer(const WindowBoundary &boundary) {
	switch (boundary) {
	case WindowBoundary::CURRENT_ROW_RANGE:
	case WindowBoundary::EXPR_PRECEDING_RANGE:
	case WindowBoundary::EXPR_FOLLOWING_RANGE:
		return true;
	default:
		return false;
	}
}

struct WindowBoundariesState {
	static inline bool IsScalar(const unique_ptr<Expression> &expr) {
		return expr ? expr->IsScalar() : true;
	}

	explicit WindowBoundariesState(BoundWindowExpression *wexpr)
	    : type(wexpr->type), start_boundary(wexpr->start), end_boundary(wexpr->end),
	      partition_count(wexpr->partitions.size()), order_count(wexpr->orders.size()),
	      range_sense(wexpr->orders.empty() ? OrderType::INVALID : wexpr->orders[0].type),
	      scalar_start(IsScalar(wexpr->start_expr)), scalar_end(IsScalar(wexpr->end_expr)),
	      has_preceding_range(wexpr->start == WindowBoundary::EXPR_PRECEDING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_PRECEDING_RANGE),
	      has_following_range(wexpr->start == WindowBoundary::EXPR_FOLLOWING_RANGE ||
	                          wexpr->end == WindowBoundary::EXPR_FOLLOWING_RANGE),
	      needs_peer(BoundaryNeedsPeer(wexpr->end) || wexpr->type == ExpressionType::WINDOW_CUME_DIST) {
	}

	// Cached lookups
	const ExpressionType type;
	const WindowBoundary start_boundary;
	const WindowBoundary end_boundary;
	const idx_t partition_count;
	const idx_t order_count;
	const OrderType range_sense;
	const bool scalar_start;
	const bool scalar_end;
	const bool has_preceding_range;
	const bool has_following_range;
	const bool needs_peer;

	idx_t partition_start = 0;
	idx_t partition_end = 0;
	idx_t peer_start = 0;
	idx_t peer_end = 0;
	idx_t valid_start = 0;
	idx_t valid_end = 0;
	int64_t window_start = -1;
	int64_t window_end = -1;
	bool is_same_partition = false;
	bool is_peer = false;
};

static bool WindowNeedsRank(BoundWindowExpression *wexpr) {
	return wexpr->type == ExpressionType::WINDOW_PERCENT_RANK || wexpr->type == ExpressionType::WINDOW_RANK ||
	       wexpr->type == ExpressionType::WINDOW_RANK_DENSE || wexpr->type == ExpressionType::WINDOW_CUME_DIST;
}

template <typename T>
static T GetCell(ChunkCollection &collection, idx_t column, idx_t index) {
	D_ASSERT(collection.ColumnCount() > column);
	auto &chunk = collection.GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	const auto data = FlatVector::GetData<T>(source);
	return data[source_offset];
}

static bool CellIsNull(ChunkCollection &collection, idx_t column, idx_t index) {
	D_ASSERT(collection.ColumnCount() > column);
	auto &chunk = collection.GetChunkForRow(index);
	auto &source = chunk.data[column];
	const auto source_offset = index % STANDARD_VECTOR_SIZE;
	return FlatVector::IsNull(source, source_offset);
}

template <typename T>
struct ChunkCollectionIterator {
	using iterator = ChunkCollectionIterator<T>;
	using iterator_category = std::forward_iterator_tag;
	using difference_type = std::ptrdiff_t;
	using value_type = T;
	using reference = T;
	using pointer = idx_t;

	ChunkCollectionIterator(ChunkCollection &coll_p, idx_t col_no_p, pointer pos_p = 0)
	    : coll(&coll_p), col_no(col_no_p), pos(pos_p) {
	}

	inline reference operator*() const {
		return GetCell<T>(*coll, col_no, pos);
	}
	inline explicit operator pointer() const {
		return pos;
	}

	inline iterator &operator++() {
		++pos;
		return *this;
	}
	inline iterator operator++(int) {
		auto result = *this;
		++(*this);
		return result;
	}

	friend inline bool operator==(const iterator &a, const iterator &b) {
		return a.pos == b.pos;
	}
	friend inline bool operator!=(const iterator &a, const iterator &b) {
		return a.pos != b.pos;
	}

private:
	ChunkCollection *coll;
	idx_t col_no;
	pointer pos;
};

template <typename T, typename OP>
struct OperationCompare : public std::function<bool(T, T)> {
	inline bool operator()(const T &lhs, const T &val) const {
		return OP::template Operation(lhs, val);
	}
};

template <typename T, typename OP, bool FROM>
static idx_t FindTypedRangeBound(ChunkCollection &over, const idx_t order_col, const idx_t order_begin,
                                 const idx_t order_end, ChunkCollection &boundary, const idx_t boundary_row) {
	D_ASSERT(!CellIsNull(boundary, 0, boundary_row));
	const auto val = GetCell<T>(boundary, 0, boundary_row);

	OperationCompare<T, OP> comp;
	ChunkCollectionIterator<T> begin(over, order_col, order_begin);
	ChunkCollectionIterator<T> end(over, order_col, order_end);
	if (FROM) {
		return idx_t(std::lower_bound(begin, end, val, comp));
	} else {
		return idx_t(std::upper_bound(begin, end, val, comp));
	}
}

template <typename OP, bool FROM>
static idx_t FindRangeBound(ChunkCollection &over, const idx_t order_col, const idx_t order_begin,
                            const idx_t order_end, ChunkCollection &boundary, const idx_t expr_idx) {
	const auto &over_types = over.Types();
	D_ASSERT(over_types.size() > order_col);
	D_ASSERT(boundary.Types().size() == 1);
	D_ASSERT(boundary.Types()[0] == over_types[order_col]);

	switch (over_types[order_col].InternalType()) {
	case PhysicalType::INT8:
		return FindTypedRangeBound<int8_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT16:
		return FindTypedRangeBound<int16_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT32:
		return FindTypedRangeBound<int32_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT64:
		return FindTypedRangeBound<int64_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT8:
		return FindTypedRangeBound<uint8_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT16:
		return FindTypedRangeBound<uint16_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT32:
		return FindTypedRangeBound<uint32_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::UINT64:
		return FindTypedRangeBound<uint64_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INT128:
		return FindTypedRangeBound<hugeint_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::FLOAT:
		return FindTypedRangeBound<float, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::DOUBLE:
		return FindTypedRangeBound<double, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case PhysicalType::INTERVAL:
		return FindTypedRangeBound<interval_t, OP, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported column type for RANGE");
	}
}

template <bool FROM>
static idx_t FindOrderedRangeBound(ChunkCollection &over, const idx_t order_col, const OrderType range_sense,
                                   const idx_t order_begin, const idx_t order_end, ChunkCollection &boundary,
                                   const idx_t expr_idx) {
	switch (range_sense) {
	case OrderType::ASCENDING:
		return FindRangeBound<LessThan, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	case OrderType::DESCENDING:
		return FindRangeBound<GreaterThan, FROM>(over, order_col, order_begin, order_end, boundary, expr_idx);
	default:
		throw InternalException("Unsupported ORDER BY sense for RANGE");
	}
}

static void UpdateWindowBoundaries(WindowBoundariesState &bounds, const idx_t input_size, const idx_t row_idx,
                                   ChunkCollection &over_collection, ChunkCollection &boundary_start_collection,
                                   ChunkCollection &boundary_end_collection, const ValidityMask &partition_mask,
                                   const ValidityMask &order_mask) {

	// RANGE sorting parameters
	const auto order_col = bounds.partition_count;

	if (bounds.partition_count + bounds.order_count > 0) {

		// determine partition and peer group boundaries to ultimately figure out window size
		bounds.is_same_partition = !partition_mask.RowIsValidUnsafe(row_idx);
		bounds.is_peer = !order_mask.RowIsValidUnsafe(row_idx);

		// when the partition changes, recompute the boundaries
		if (!bounds.is_same_partition) {
			bounds.partition_start = row_idx;
			bounds.peer_start = row_idx;

			// find end of partition
			bounds.partition_end = input_size;
			if (bounds.partition_count) {
				idx_t n = 1;
				bounds.partition_end = FindNextStart(partition_mask, bounds.partition_start + 1, input_size, n);
			}

			// Find valid ordering values for the new partition
			// so we can exclude NULLs from RANGE expression computations
			bounds.valid_start = bounds.partition_start;
			bounds.valid_end = bounds.partition_end;

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_preceding_range) {
				// Exclude any leading NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_start)) {
					idx_t n = 1;
					bounds.valid_start = FindNextStart(order_mask, bounds.valid_start + 1, bounds.valid_end, n);
				}
			}

			if ((bounds.valid_start < bounds.valid_end) && bounds.has_following_range) {
				// Exclude any trailing NULLs
				if (CellIsNull(over_collection, order_col, bounds.valid_end - 1)) {
					idx_t n = 1;
					bounds.valid_end = FindPrevStart(order_mask, bounds.valid_start, bounds.valid_end, n);
				}
			}

		} else if (!bounds.is_peer) {
			bounds.peer_start = row_idx;
		}

		if (bounds.needs_peer) {
			bounds.peer_end = bounds.partition_end;
			if (bounds.order_count) {
				idx_t n = 1;
				bounds.peer_end = FindNextStart(order_mask, bounds.peer_start + 1, bounds.partition_end, n);
			}
		}

	} else {
		bounds.is_same_partition = false;
		bounds.is_peer = true;
		bounds.partition_end = input_size;
		bounds.peer_end = bounds.partition_end;
	}

	// determine window boundaries depending on the type of expression
	bounds.window_start = -1;
	bounds.window_end = -1;

	switch (bounds.start_boundary) {
	case WindowBoundary::UNBOUNDED_PRECEDING:
		bounds.window_start = bounds.partition_start;
		break;
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_start = row_idx;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_start = bounds.peer_start;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS: {
		bounds.window_start =
		    (int64_t)row_idx - GetCell<int64_t>(boundary_start_collection, 0, bounds.scalar_start ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_ROWS: {
		bounds.window_start =
		    row_idx + GetCell<int64_t>(boundary_start_collection, 0, bounds.scalar_start ? 0 : row_idx);
		break;
	}
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = bounds.scalar_start ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start =
			    FindOrderedRangeBound<true>(over_collection, order_col, bounds.range_sense, bounds.valid_start, row_idx,
			                                boundary_start_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = bounds.scalar_start ? 0 : row_idx;
		if (CellIsNull(boundary_start_collection, 0, expr_idx)) {
			bounds.window_start = bounds.peer_start;
		} else {
			bounds.window_start = FindOrderedRangeBound<true>(over_collection, order_col, bounds.range_sense, row_idx,
			                                                  bounds.valid_end, boundary_start_collection, expr_idx);
		}
		break;
	}
	default:
		throw InternalException("Unsupported window start boundary");
	}

	switch (bounds.end_boundary) {
	case WindowBoundary::CURRENT_ROW_ROWS:
		bounds.window_end = row_idx + 1;
		break;
	case WindowBoundary::CURRENT_ROW_RANGE:
		bounds.window_end = bounds.peer_end;
		break;
	case WindowBoundary::UNBOUNDED_FOLLOWING:
		bounds.window_end = bounds.partition_end;
		break;
	case WindowBoundary::EXPR_PRECEDING_ROWS:
		bounds.window_end =
		    (int64_t)row_idx - GetCell<int64_t>(boundary_end_collection, 0, bounds.scalar_end ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_FOLLOWING_ROWS:
		bounds.window_end = row_idx + GetCell<int64_t>(boundary_end_collection, 0, bounds.scalar_end ? 0 : row_idx) + 1;
		break;
	case WindowBoundary::EXPR_PRECEDING_RANGE: {
		const auto expr_idx = bounds.scalar_end ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end =
			    FindOrderedRangeBound<false>(over_collection, order_col, bounds.range_sense, bounds.valid_start,
			                                 row_idx, boundary_end_collection, expr_idx);
		}
		break;
	}
	case WindowBoundary::EXPR_FOLLOWING_RANGE: {
		const auto expr_idx = bounds.scalar_end ? 0 : row_idx;
		if (CellIsNull(boundary_end_collection, 0, expr_idx)) {
			bounds.window_end = bounds.peer_end;
		} else {
			bounds.window_end = FindOrderedRangeBound<false>(over_collection, order_col, bounds.range_sense, row_idx,
			                                                 bounds.valid_end, boundary_end_collection, expr_idx);
		}
		break;
	}
	default:
		throw InternalException("Unsupported window end boundary");
	}

	// clamp windows to partitions if they should exceed
	if (bounds.window_start < (int64_t)bounds.partition_start) {
		bounds.window_start = bounds.partition_start;
	}
	if (bounds.window_start > (int64_t)bounds.partition_end) {
		bounds.window_start = bounds.partition_end;
	}
	if (bounds.window_end < (int64_t)bounds.partition_start) {
		bounds.window_end = bounds.partition_start;
	}
	if (bounds.window_end > (int64_t)bounds.partition_end) {
		bounds.window_end = bounds.partition_end;
	}

	if (bounds.window_start < 0 || bounds.window_end < 0) {
		throw InternalException("Failed to compute window boundaries");
	}
}

static void ComputeWindowExpression(BoundWindowExpression *wexpr, ChunkCollection &input, ChunkCollection &output,
                                    ChunkCollection &over, const ValidityMask &partition_mask,
                                    const ValidityMask &order_mask, WindowAggregationMode mode) {

	// TODO we could evaluate those expressions in parallel

	// Single pass over the input to produce the payload columns.
	// Vectorisation for the win...

	// evaluate inner expressions of window functions, could be more complex
	ChunkCollection payload_collection;
	vector<Expression *> exprs;
	for (auto &child : wexpr->children) {
		exprs.push_back(child.get());
	}
	// TODO: child may be a scalar, don't need to materialize the whole collection then
	ExpressionExecutor payload_executor;
	DataChunk payload_chunk;
	PrepareInputExpressions(exprs.data(), exprs.size(), payload_collection, payload_executor, payload_chunk);

	WindowInputExpression leadlag_offset(wexpr->offset_expr.get());
	WindowInputExpression leadlag_default(wexpr->default_expr.get());

	// evaluate the FILTER clause and stuff it into a large mask for compactness and reuse
	ValidityMask filter_mask;
	vector<validity_t> filter_bits;
	ExpressionExecutor filter_executor;
	SelectionVector filter_sel;
	if (wexpr->filter_expr) {
		// 	Start with all invalid and set the ones that pass
		filter_bits.resize(ValidityMask::ValidityMaskSize(input.Count()), 0);
		filter_mask.Initialize(filter_bits.data());
		filter_executor.AddExpression(*wexpr->filter_expr);
		filter_sel.Initialize(STANDARD_VECTOR_SIZE);
	}

	// evaluate boundaries if present. Parser has checked boundary types.
	WindowInputExpression boundary_start(wexpr->start_expr.get());
	WindowInputExpression boundary_end(wexpr->end_expr.get());

	// Set up a validity mask for IGNORE NULLS
	ValidityMask ignore_nulls;
	bool check_nulls = false;
	if (wexpr->ignore_nulls) {
		switch (wexpr->type) {
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG:
		case ExpressionType::WINDOW_FIRST_VALUE:
		case ExpressionType::WINDOW_LAST_VALUE:
		case ExpressionType::WINDOW_NTH_VALUE:
			check_nulls = true;
			break;
		default:
			break;
		}
	}

	// Single pass over the input to produce the payload columns.
	// Vectorisation for the win...
	idx_t input_idx = 0;
	for (auto &input_chunk : input.Chunks()) {
		const auto count = input_chunk->size();

		if (!exprs.empty()) {
			payload_chunk.Reset();
			payload_executor.Execute(*input_chunk, payload_chunk);
			payload_chunk.Verify();
			payload_collection.Append(payload_chunk);

			// process payload chunks while they are still piping hot
			if (check_nulls) {
				VectorData vdata;
				payload_chunk.data[0].Orrify(count, vdata);
				if (!vdata.validity.AllValid()) {
					//	Lazily materialise the contents when we find the first NULL
					if (ignore_nulls.AllValid()) {
						ignore_nulls.Initialize(payload_collection.Count());
					}
					// Write to the current position
					// Chunks in a collection are full, so we don't have to worry about raggedness
					auto dst = ignore_nulls.GetData() + ignore_nulls.EntryCount(input_idx);
					auto src = vdata.validity.GetData();
					for (auto entry_count = vdata.validity.EntryCount(count); entry_count-- > 0;) {
						*dst++ = *src++;
					}
				}
			}
		}

		leadlag_offset.Execute(*input_chunk);
		leadlag_default.Execute(*input_chunk);

		if (wexpr->filter_expr) {
			const auto filtered = filter_executor.SelectExpression(*input_chunk, filter_sel);
			for (idx_t f = 0; f < filtered; ++f) {
				filter_mask.SetValid(input_idx + filter_sel[f]);
			}
		}

		boundary_start.Execute(*input_chunk);
		boundary_end.Execute(*input_chunk);

		input_idx += count;
	}

	// build a segment tree for frame-adhering aggregates
	// see http://www.vldb.org/pvldb/vol8/p1058-leis.pdf
	unique_ptr<WindowSegmentTree> segment_tree = nullptr;

	if (wexpr->aggregate) {
		segment_tree = make_unique<WindowSegmentTree>(*(wexpr->aggregate), wexpr->bind_info.get(), wexpr->return_type,
		                                              &payload_collection, filter_mask, mode);
	}

	WindowBoundariesState bounds(wexpr);
	uint64_t dense_rank = 1, rank_equal = 0, rank = 1;

	// this is the main loop, go through all sorted rows and compute window function result
	const vector<LogicalType> output_types(1, wexpr->return_type);
	DataChunk output_chunk;
	output_chunk.Initialize(output_types);
	for (idx_t row_idx = 0; row_idx < input.Count(); row_idx++) {
		// Grow the chunk if necessary.
		const auto output_offset = row_idx % STANDARD_VECTOR_SIZE;
		if (output_offset == 0) {
			output.Append(output_chunk);
			output_chunk.Reset();
			output_chunk.SetCardinality(MinValue(idx_t(STANDARD_VECTOR_SIZE), input.Count() - row_idx));
		}
		auto &result = output_chunk.data[0];

		// special case, OVER (), aggregate over everything
		UpdateWindowBoundaries(bounds, input.Count(), row_idx, over, boundary_start.collection, boundary_end.collection,
		                       partition_mask, order_mask);
		if (WindowNeedsRank(wexpr)) {
			if (!bounds.is_same_partition || row_idx == 0) { // special case for first row, need to init
				dense_rank = 1;
				rank = 1;
				rank_equal = 0;
			} else if (!bounds.is_peer) {
				dense_rank++;
				rank += rank_equal;
				rank_equal = 0;
			}
			rank_equal++;
		}

		// if no values are read for window, result is NULL
		if (bounds.window_start >= bounds.window_end) {
			FlatVector::SetNull(result, output_offset, true);
			continue;
		}

		switch (wexpr->type) {
		case ExpressionType::WINDOW_AGGREGATE: {
			segment_tree->Compute(result, output_offset, bounds.window_start, bounds.window_end);
			break;
		}
		case ExpressionType::WINDOW_ROW_NUMBER: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = row_idx - bounds.partition_start + 1;
			break;
		}
		case ExpressionType::WINDOW_RANK_DENSE: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = dense_rank;
			break;
		}
		case ExpressionType::WINDOW_RANK: {
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = rank;
			break;
		}
		case ExpressionType::WINDOW_PERCENT_RANK: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start - 1;
			double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
			auto rdata = FlatVector::GetData<double>(result);
			rdata[output_offset] = percent_rank;
			break;
		}
		case ExpressionType::WINDOW_CUME_DIST: {
			int64_t denom = (int64_t)bounds.partition_end - bounds.partition_start;
			double cume_dist = denom > 0 ? ((double)(bounds.peer_end - bounds.partition_start)) / denom : 0;
			auto rdata = FlatVector::GetData<double>(result);
			rdata[output_offset] = cume_dist;
			break;
		}
		case ExpressionType::WINDOW_NTILE: {
			D_ASSERT(payload_collection.ColumnCount() == 1);
			auto n_param = GetCell<int64_t>(payload_collection, 0, row_idx);
			// With thanks from SQLite's ntileValueFunc()
			int64_t n_total = bounds.partition_end - bounds.partition_start;
			if (n_param > n_total) {
				// more groups allowed than we have values
				// map every entry to a unique group
				n_param = n_total;
			}
			int64_t n_size = (n_total / n_param);
			// find the row idx within the group
			D_ASSERT(row_idx >= bounds.partition_start);
			int64_t adjusted_row_idx = row_idx - bounds.partition_start;
			// now compute the ntile
			int64_t n_large = n_total - n_param * n_size;
			int64_t i_small = n_large * (n_size + 1);
			int64_t result_ntile;

			D_ASSERT((n_large * (n_size + 1) + (n_param - n_large) * n_size) == n_total);

			if (adjusted_row_idx < i_small) {
				result_ntile = 1 + adjusted_row_idx / (n_size + 1);
			} else {
				result_ntile = 1 + n_large + (adjusted_row_idx - i_small) / n_size;
			}
			// result has to be between [1, NTILE]
			D_ASSERT(result_ntile >= 1 && result_ntile <= n_param);
			auto rdata = FlatVector::GetData<int64_t>(result);
			rdata[output_offset] = result_ntile;
			break;
		}
		case ExpressionType::WINDOW_LEAD:
		case ExpressionType::WINDOW_LAG: {
			int64_t offset = 1;
			if (wexpr->offset_expr) {
				offset = GetCell<int64_t>(leadlag_offset.collection, 0, wexpr->offset_expr->IsScalar() ? 0 : row_idx);
			}
			int64_t val_idx = (int64_t)row_idx;
			if (wexpr->type == ExpressionType::WINDOW_LEAD) {
				val_idx += offset;
			} else {
				val_idx -= offset;
			}

			idx_t delta = 0;
			if (val_idx < (int64_t)row_idx) {
				// Count backwards
				delta = idx_t(row_idx - val_idx);
				val_idx = FindPrevStart(ignore_nulls, bounds.partition_start, row_idx, delta);
			} else if (val_idx > (int64_t)row_idx) {
				delta = idx_t(val_idx - row_idx);
				val_idx = FindNextStart(ignore_nulls, row_idx + 1, bounds.partition_end, delta);
			}
			// else offset is zero, so don't move.

			if (!delta) {
				payload_collection.CopyCell(0, val_idx, result, output_offset);
			} else if (wexpr->default_expr) {
				const auto source_row = wexpr->default_expr->IsScalar() ? 0 : row_idx;
				leadlag_default.collection.CopyCell(0, source_row, result, output_offset);
			} else {
				FlatVector::SetNull(result, output_offset, true);
			}
			break;
		}
		case ExpressionType::WINDOW_FIRST_VALUE: {
			idx_t n = 1;
			const auto first_idx = FindNextStart(ignore_nulls, bounds.window_start, bounds.window_end, n);
			payload_collection.CopyCell(0, first_idx, result, output_offset);
			break;
		}
		case ExpressionType::WINDOW_LAST_VALUE: {
			idx_t n = 1;
			payload_collection.CopyCell(0, FindPrevStart(ignore_nulls, bounds.window_start, bounds.window_end, n),
			                            result, output_offset);
			break;
		}
		case ExpressionType::WINDOW_NTH_VALUE: {
			D_ASSERT(payload_collection.ColumnCount() == 2);
			// Returns value evaluated at the row that is the n'th row of the window frame (counting from 1);
			// returns NULL if there is no such row.
			if (CellIsNull(payload_collection, 1, row_idx)) {
				FlatVector::SetNull(result, output_offset, true);
			} else {
				auto n_param = GetCell<int64_t>(payload_collection, 1, row_idx);
				if (n_param < 1) {
					FlatVector::SetNull(result, output_offset, true);
				} else {
					auto n = idx_t(n_param);
					const auto nth_index = FindNextStart(ignore_nulls, bounds.window_start, bounds.window_end, n);
					if (!n) {
						payload_collection.CopyCell(0, nth_index, result, output_offset);
					} else {
						FlatVector::SetNull(result, output_offset, true);
					}
				}
			}
			break;
		}
		default:
			throw InternalException("Window aggregate type %s", ExpressionTypeToString(wexpr->type));
		}
	}

	// Push the last chunk
	output.Append(output_chunk);
}

using WindowExpressions = vector<BoundWindowExpression *>;

static void ComputeWindowExpressions(WindowExpressions &window_exprs, ChunkCollection &input,
                                     ChunkCollection &window_results, ChunkCollection &over,
                                     WindowAggregationMode mode) {
	//	Idempotency
	if (input.Count() == 0) {
		return;
	}
	//	Pick out a function for the OVER clause
	auto over_expr = window_exprs[0];

	//	Set bits for the start of each partition
	vector<validity_t> partition_bits(ValidityMask::EntryCount(input.Count()), 0);
	ValidityMask partition_mask(partition_bits.data());
	partition_mask.SetValid(0);

	for (idx_t c = 0; c < over_expr->partitions.size(); ++c) {
		MaskColumn(partition_mask, over, c);
	}

	//	Set bits for the start of each peer group.
	//	Partitions also break peer groups, so start with the partition bits.
	const auto sort_col_count = over_expr->partitions.size() + over_expr->orders.size();
	ValidityMask order_mask(partition_mask, input.Count());
	for (idx_t c = over_expr->partitions.size(); c < sort_col_count; ++c) {
		MaskColumn(order_mask, over, c);
	}

	//	Compute the functions columnwise
	for (idx_t expr_idx = 0; expr_idx < window_exprs.size(); ++expr_idx) {
		ChunkCollection output;
		ComputeWindowExpression(window_exprs[expr_idx], input, output, over, partition_mask, order_mask, mode);
		window_results.Fuse(output);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
static void GeneratePartition(WindowLocalSourceState &state, WindowGlobalSinkState &gstate, const idx_t hash_bin) {
	auto &op = (PhysicalWindow &)gstate.op;
	WindowExpressions window_exprs;
	for (idx_t expr_idx = 0; expr_idx < op.select_list.size(); ++expr_idx) {
		D_ASSERT(op.select_list[expr_idx]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
		auto wexpr = reinterpret_cast<BoundWindowExpression *>(op.select_list[expr_idx].get());
		window_exprs.emplace_back(wexpr);
	}

	//	Get rid of any stale data
	state.chunks.Reset();
	state.window_results.Reset();
	state.hash_bin = hash_bin;
	state.position = 0;
	state.hash_group.reset();

	// There are three types of partitions:
	// 1. No partition (no sorting)
	// 2. One partition (sorting, but no hashing)
	// 3. Multiple partitions (sorting and hashing)
	const auto &input_types = op.children[0]->types;

	// Scan the sorted data into new Collections
	ChunkCollection input;
	ChunkCollection over;
	if (gstate.rows && !hash_bin) {
		//	No partition - convert row collection to chunk collection
		ScanRowCollection(*gstate.rows, input, input_types);
	} else if (hash_bin < gstate.hash_groups.size() && gstate.hash_groups[hash_bin]) {
		// Overwrite the collections with the sorted data
		SortCollectionForPartition(state, gstate, hash_bin);
		const auto over_types = state.hash_group->global_sort->sort_layout.logical_types;
		ScanSortedPartition(state, input, input_types, over, over_types);
	} else {
		return;
	}

	ChunkCollection output;
	ComputeWindowExpressions(window_exprs, input, output, over, gstate.mode);
	state.chunks.Merge(input);
	state.window_results.Merge(output);
}

static void Scan(WindowLocalSourceState &state, DataChunk &chunk) {
	ChunkCollection &big_data = state.chunks;
	ChunkCollection &window_results = state.window_results;

	if (state.position >= big_data.Count()) {
		return;
	}

	// just return what was computed before, appending the result cols of the window expressions at the end
	auto &proj_ch = big_data.GetChunkForRow(state.position);
	auto &wind_ch = window_results.GetChunkForRow(state.position);

	idx_t out_idx = 0;
	D_ASSERT(proj_ch.size() == wind_ch.size());
	chunk.SetCardinality(proj_ch);
	for (idx_t col_idx = 0; col_idx < proj_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(proj_ch.data[col_idx]);
	}
	for (idx_t col_idx = 0; col_idx < wind_ch.ColumnCount(); col_idx++) {
		chunk.data[out_idx++].Reference(wind_ch.data[col_idx]);
	}
	chunk.Verify();

	state.position += STANDARD_VECTOR_SIZE;
}

SinkResultType PhysicalWindow::Sink(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p,
                                    DataChunk &input) const {
	auto &gstate = (WindowGlobalSinkState &)gstate_p;
	auto &lstate = (WindowLocalSinkState &)lstate_p;

	lstate.Over(input);
	lstate.Hash();
	lstate.Sink(input, gstate);

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalWindow::Combine(ExecutionContext &context, GlobalSinkState &gstate_p, LocalSinkState &lstate_p) const {
	auto &gstate = (WindowGlobalSinkState &)gstate_p;
	auto &lstate = (WindowLocalSinkState &)lstate_p;
	lstate.Combine(gstate);
}

unique_ptr<LocalSinkState> PhysicalWindow::GetLocalSinkState(ExecutionContext &context) const {
	D_ASSERT(select_list[0]->GetExpressionClass() == ExpressionClass::BOUND_WINDOW);
	auto wexpr = reinterpret_cast<BoundWindowExpression *>(select_list[0].get());
	return make_unique<WindowLocalSinkState>(*this, wexpr);
}

unique_ptr<GlobalSinkState> PhysicalWindow::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<WindowGlobalSinkState>(*this, context);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class WindowGlobalSourceState : public GlobalSourceState {
public:
	explicit WindowGlobalSourceState(const PhysicalWindow &op) : op(op), next_bin(0) {
	}

	const PhysicalWindow &op;
	//! The output read position.
	atomic<idx_t> next_bin;

public:
	idx_t MaxThreads() override {
		auto &state = (WindowGlobalSinkState &)*op.sink_state;

		// If there is only one partition, we have to process it on one thread.
		if (state.hash_groups.empty()) {
			return 1;
		}

		idx_t max_threads = 0;
		for (const auto &hash_group : state.hash_groups) {
			if (hash_group) {
				max_threads++;
			}
		}

		return max_threads;
	}
};

unique_ptr<LocalSourceState> PhysicalWindow::GetLocalSourceState(ExecutionContext &context,
                                                                 GlobalSourceState &gstate) const {
	return make_unique<WindowLocalSourceState>(*this, context);
}

unique_ptr<GlobalSourceState> PhysicalWindow::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<WindowGlobalSourceState>(*this);
}

void PhysicalWindow::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                             LocalSourceState &lstate_p) const {
	auto &state = (WindowLocalSourceState &)lstate_p;
	auto &global_source = (WindowGlobalSourceState &)gstate_p;
	auto &gstate = (WindowGlobalSinkState &)*sink_state;

	const auto bin_count = gstate.hash_groups.empty() ? 1 : gstate.hash_groups.size();

	//	Move to the next bin if we are done.
	while (state.position >= state.chunks.Count()) {
		auto hash_bin = global_source.next_bin++;
		if (hash_bin >= bin_count) {
			return;
		}

		for (; hash_bin < gstate.hash_groups.size(); hash_bin = global_source.next_bin++) {
			if (gstate.hash_groups[hash_bin]) {
				break;
			}
		}
		GeneratePartition(state, gstate, hash_bin);
	}

	Scan(state, chunk);
}

string PhysicalWindow::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < select_list.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += select_list[i]->GetName();
	}
	return result;
}

} // namespace duckdb
