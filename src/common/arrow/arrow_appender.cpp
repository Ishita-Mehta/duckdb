#include "duckdb/common/arrow/arrow_appender.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/array.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/uuid.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Arrow buffers & append data
//===--------------------------------------------------------------------===//
struct ArrowBuffer {
	ArrowBuffer() : dataptr(nullptr), count(0), capacity(0) {
	}
	~ArrowBuffer() {
		if (!dataptr) {
			return;
		}
		free(dataptr);
		dataptr = nullptr;
		count = 0;
		capacity = 0;
	}
	// disable copy constructors
	ArrowBuffer(const ArrowBuffer &other) = delete;
	ArrowBuffer &operator=(const ArrowBuffer &) = delete;
	//! enable move constructors
	ArrowBuffer(ArrowBuffer &&other) noexcept {
		std::swap(dataptr, other.dataptr);
		std::swap(count, other.count);
		std::swap(capacity, other.capacity);
	}
	ArrowBuffer &operator=(ArrowBuffer &&other) noexcept {
		std::swap(dataptr, other.dataptr);
		std::swap(count, other.count);
		std::swap(capacity, other.capacity);
		return *this;
	}

	void reserve(idx_t bytes) {
		auto new_capacity = NextPowerOfTwo(bytes);
		if (new_capacity <= capacity) {
			return;
		}
		reserve_internal(new_capacity);
	}

	void resize(idx_t bytes) {
		reserve(bytes);
		count = bytes;
	}

	void resize(idx_t bytes, data_t value) {
		reserve(bytes);
		for (idx_t i = count; i < bytes; i++) {
			dataptr[i] = value;
		}
		count = bytes;
	}

	idx_t size() {
		return count;
	}

	data_ptr_t data() {
		return dataptr;
	}

	void shrink_to_fit() {
		reserve_internal(count);
	}

private:
	void reserve_internal(idx_t bytes) {
		if (dataptr) {
			dataptr = (data_ptr_t)realloc(dataptr, bytes);
		} else {
			dataptr = (data_ptr_t)malloc(bytes);
		}
		capacity = bytes;
	}

private:
	data_ptr_t dataptr = nullptr;
	idx_t count = 0;
	idx_t capacity = 0;
};

typedef void (*initialize_t)(ArrowAppendData &result, const LogicalType &type, idx_t capacity);
typedef void (*append_vector_t)(ArrowAppendData &append_data, Vector &input, idx_t size);
typedef void (*finalize_t)(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result);

struct ArrowAppendData {
	// the buffers of the arrow vector
	ArrowBuffer validity;
	ArrowBuffer main_buffer;
	ArrowBuffer aux_buffer;

	idx_t row_count = 0;
	idx_t null_count = 0;

	// function pointers for construction
	initialize_t initialize = nullptr;
	append_vector_t append_vector = nullptr;
	finalize_t finalize = nullptr;

	// child data (if any)
	vector<ArrowAppendData> child_data;

	//! the arrow array C API data, only set after Finalize
	unique_ptr<ArrowArray> array;
	duckdb::array<const void *, 3> buffers = {{nullptr, nullptr, nullptr}};
	vector<ArrowArray *> child_pointers;
};

//===--------------------------------------------------------------------===//
// ArrowAppender
//===--------------------------------------------------------------------===//
static ArrowAppendData InitializeArrowChild(const LogicalType &type, idx_t capacity);
static ArrowArray *FinalizeArrowChild(const LogicalType &type, ArrowAppendData &append_data);

ArrowAppender::ArrowAppender(vector<LogicalType> types_p, idx_t initial_capacity) : types(move(types_p)) {
	for (auto &type : types) {
		auto entry = InitializeArrowChild(type, initial_capacity);
		root_data.push_back(move(entry));
	}
}

ArrowAppender::~ArrowAppender() {
}

//===--------------------------------------------------------------------===//
// Append Helper Functions
//===--------------------------------------------------------------------===//
static void GetBitPosition(idx_t row_idx, uint8_t &current_byte, uint8_t &current_bit) {
	current_byte = row_idx / 8;
	current_bit = row_idx % 8;
}

static void UnsetBit(uint8_t *data, uint8_t current_byte, uint8_t current_bit) {
	data[current_byte] &= ~(1 << current_bit);
}

static void NextBit(uint8_t &current_byte, uint8_t &current_bit) {
	current_bit++;
	if (current_bit == 8) {
		current_byte++;
		current_bit = 0;
	}
}

static void ResizeValidity(ArrowBuffer &buffer, idx_t row_count) {
	auto byte_count = (row_count + 7) / 8;
	buffer.resize(byte_count, 0xFF);
}

static void SetNull(ArrowAppendData &append_data, uint8_t *validity_data, uint8_t current_byte, uint8_t current_bit) {
	UnsetBit(validity_data, current_byte, current_bit);
	append_data.null_count++;
}

static void AppendValidity(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t size) {
	// resize the buffer, filling the validity buffer with all valid values
	ResizeValidity(append_data.validity, append_data.row_count + size);
	if (format.validity.AllValid()) {
		// if all values are valid we don't need to do anything else
		return;
	}

	// otherwise we iterate through the validity mask
	auto validity_data = (uint8_t *)append_data.validity.data();
	uint8_t current_bit, current_byte;
	GetBitPosition(append_data.row_count, current_byte, current_bit);
	for (idx_t i = 0; i < size; i++) {
		auto source_idx = format.sel->get_index(i);
		// append the validity mask
		if (!format.validity.RowIsValid(source_idx)) {
			SetNull(append_data, validity_data, current_byte, current_bit);
		}
		NextBit(current_byte, current_bit);
	}
}

//===--------------------------------------------------------------------===//
// Scalar Types
//===--------------------------------------------------------------------===//
struct ArrowScalarConverter {
	template <class TGT, class SRC>
	static TGT Operation(SRC input) {
		return input;
	}

	static bool SkipNulls() {
		return false;
	}

	template <class TGT>
	static void SetNull(TGT &value) {
	}
};

struct ArrowIntervalConverter {
	template <class TGT, class SRC>
	static TGT Operation(SRC input) {
		return Interval::GetMilli(input);
	}

	static bool SkipNulls() {
		return true;
	}

	template <class TGT>
	static void SetNull(TGT &value) {
		value = 0;
	}
};

template <class TGT, class SRC = TGT, class OP = ArrowScalarConverter>
struct ArrowScalarBaseData {
	static void Append(ArrowAppendData &append_data, Vector &input, idx_t size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(size, format);

		// append the validity mask
		AppendValidity(append_data, format, size);

		// append the main data
		append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(TGT) * size);
		auto data = (SRC *)format.data;
		auto result_data = (TGT *)append_data.main_buffer.data();

		for (idx_t i = 0; i < size; i++) {
			auto source_idx = format.sel->get_index(i);
			auto result_idx = append_data.row_count + i;

			if (OP::SkipNulls() && !format.validity.RowIsValid(source_idx)) {
				OP::template SetNull<TGT>(result_data[result_idx]);
				continue;
			}
			result_data[result_idx] = OP::template Operation<TGT, SRC>(data[source_idx]);
		}
		append_data.row_count += size;
	}
};

template <class TGT, class SRC = TGT, class OP = ArrowScalarConverter>
struct ArrowScalarData : public ArrowScalarBaseData<TGT, SRC, OP> {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve(capacity * sizeof(TGT));
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
	}
};

//===--------------------------------------------------------------------===//
// Enums
//===--------------------------------------------------------------------===//
template <class TGT>
struct ArrowEnumData : public ArrowScalarBaseData<TGT> {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve(capacity * sizeof(TGT));
		// construct the enum child data
		auto enum_data = InitializeArrowChild(LogicalType::VARCHAR, EnumType::GetSize(type));
		enum_data.append_vector(enum_data, EnumType::GetValuesInsertOrder(type), EnumType::GetSize(type));
		result.child_data.push_back(move(enum_data));
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
		// finalize the enum child data, and assign it to the dictionary
		result->dictionary = FinalizeArrowChild(LogicalType::VARCHAR, append_data.child_data[0]);
	}
};

//===--------------------------------------------------------------------===//
// Boolean
//===--------------------------------------------------------------------===//
struct ArrowBoolData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		auto byte_count = (capacity + 7) / 8;
		result.main_buffer.reserve(byte_count);
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(size, format);

		// we initialize both the validity and the bit set to 1's
		ResizeValidity(append_data.validity, append_data.row_count + size);
		ResizeValidity(append_data.main_buffer, append_data.row_count + size);
		auto data = (bool *)format.data;

		auto result_data = (uint8_t *)append_data.main_buffer.data();
		auto validity_data = (uint8_t *)append_data.validity.data();
		uint8_t current_bit, current_byte;
		GetBitPosition(append_data.row_count, current_byte, current_bit);
		for (idx_t i = 0; i < size; i++) {
			auto source_idx = format.sel->get_index(i);
			// append the validity mask
			if (!format.validity.RowIsValid(source_idx)) {
				SetNull(append_data, validity_data, current_byte, current_bit);
			} else if (!data[source_idx]) {
				UnsetBit(result_data, current_byte, current_bit);
			}
			NextBit(current_byte, current_bit);
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();
	}
};

//===--------------------------------------------------------------------===//
// Varchar
//===--------------------------------------------------------------------===//
struct ArrowVarcharConverter {
	template <class SRC>
	static idx_t GetLength(SRC input) {
		return input.GetSize();
	}

	template <class SRC>
	static void WriteData(data_ptr_t target, SRC input) {
		memcpy(target, input.GetDataUnsafe(), input.GetSize());
	}
};

struct ArrowUUIDConverter {
	template <class SRC>
	static idx_t GetLength(SRC input) {
		return UUID::STRING_SIZE;
	}

	template <class SRC>
	static void WriteData(data_ptr_t target, SRC input) {
		UUID::ToString(input, (char *)target);
	}
};

template <class SRC = string_t, class OP = ArrowVarcharConverter>
struct ArrowVarcharData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));
		result.aux_buffer.reserve(capacity);
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(size, format);

		// resize the validity mask and set up the validity buffer for iteration
		ResizeValidity(append_data.validity, append_data.row_count + size);
		auto validity_data = (uint8_t *)append_data.validity.data();

		// resize the offset buffer - the offset buffer holds the offsets into the child array
		append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(uint32_t) * (size + 1));
		auto data = (SRC *)format.data;
		auto offset_data = (uint32_t *)append_data.main_buffer.data();
		if (append_data.row_count == 0) {
			// first entry
			offset_data[0] = 0;
		}
		// now append the string data to the auxiliary buffer
		// the auxiliary buffer's length depends on the string lengths, so we resize as required
		auto last_offset = offset_data[append_data.row_count];
		for (idx_t i = 0; i < size; i++) {
			auto source_idx = format.sel->get_index(i);
			auto offset_idx = append_data.row_count + i + 1;

			if (!format.validity.RowIsValid(source_idx)) {
				uint8_t current_bit, current_byte;
				GetBitPosition(append_data.row_count + i, current_byte, current_bit);
				SetNull(append_data, validity_data, current_byte, current_bit);
				offset_data[offset_idx] = last_offset;
				continue;
			}

			auto string_length = OP::GetLength(data[source_idx]);

			// append the offset data
			auto current_offset = last_offset + string_length;
			offset_data[offset_idx] = current_offset;

			// resize the string buffer if required, and write the string data
			append_data.aux_buffer.resize(current_offset);
			OP::WriteData(append_data.aux_buffer.data() + last_offset, data[source_idx]);

			last_offset = current_offset;
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 3;
		result->buffers[1] = append_data.main_buffer.data();
		result->buffers[2] = append_data.aux_buffer.data();
	}
};

//===--------------------------------------------------------------------===//
// Structs
//===--------------------------------------------------------------------===//
struct ArrowStructData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		auto &children = StructType::GetChildTypes(type);
		for (auto &child : children) {
			auto child_buffer = InitializeArrowChild(child.second, capacity);
			result.child_data.push_back(move(child_buffer));
		}
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(size, format);

		AppendValidity(append_data, format, size);
		// append the children of the struct
		auto &children = StructVector::GetEntries(input);
		for (idx_t child_idx = 0; child_idx < children.size(); child_idx++) {
			auto &child = children[child_idx];
			append_data.child_data[child_idx].append_vector(append_data.child_data[child_idx], *child, size);
		}
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 1;

		auto &child_types = StructType::GetChildTypes(type);
		append_data.child_pointers.resize(child_types.size());
		result->children = append_data.child_pointers.data();
		result->n_children = child_types.size();
		for (idx_t i = 0; i < child_types.size(); i++) {
			auto &child_type = child_types[i].second;
			append_data.child_pointers[i] = FinalizeArrowChild(child_type, append_data.child_data[i]);
		}
	}
};

//===--------------------------------------------------------------------===//
// Lists
//===--------------------------------------------------------------------===//
void AppendListOffsets(ArrowAppendData &append_data, UnifiedVectorFormat &format, idx_t size) {
	// resize the offset buffer - the offset buffer holds the offsets into the child array
	append_data.main_buffer.resize(append_data.main_buffer.size() + sizeof(uint32_t) * (size + 1));
	auto data = (list_entry_t *)format.data;
	auto offset_data = (uint32_t *)append_data.main_buffer.data();
	if (append_data.row_count == 0) {
		// first entry
		offset_data[0] = 0;
	}
	// set up the offsets using the list entries
	auto last_offset = offset_data[append_data.row_count];
	for (idx_t i = 0; i < size; i++) {
		auto source_idx = format.sel->get_index(i);
		auto offset_idx = append_data.row_count + i + 1;

		if (!format.validity.RowIsValid(source_idx)) {
			offset_data[offset_idx] = last_offset;
			continue;
		}

		// append the offset data
		last_offset += data[source_idx].length;
		offset_data[offset_idx] = last_offset;
	}
}

struct ArrowListData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		auto &child_type = ListType::GetChildType(type);
		result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));
		auto child_buffer = InitializeArrowChild(child_type, capacity);
		result.child_data.push_back(move(child_buffer));
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(size, format);

		AppendValidity(append_data, format, size);
		AppendListOffsets(append_data, format, size);

		// append the child vector of the list
		auto &child = ListVector::GetEntry(input);
		auto child_size = ListVector::GetListSize(input);
		child.Flatten(child_size);
		append_data.child_data[0].append_vector(append_data.child_data[0], child, child_size);
		append_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();

		auto &child_type = ListType::GetChildType(type);
		append_data.child_pointers.resize(1);
		result->children = append_data.child_pointers.data();
		result->n_children = 1;
		append_data.child_pointers[0] = FinalizeArrowChild(child_type, append_data.child_data[0]);
	}
};

//===--------------------------------------------------------------------===//
// Maps
//===--------------------------------------------------------------------===//
struct ArrowMapData {
	static void Initialize(ArrowAppendData &result, const LogicalType &type, idx_t capacity) {
		// map types are stored in a (too) clever way
		// the main buffer holds the null values and the offsets
		// then we have a single child, which is a struct of the map_type, and the key_type
		result.main_buffer.reserve((capacity + 1) * sizeof(uint32_t));

		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		ArrowAppendData internal_struct;
		internal_struct.child_data.push_back(InitializeArrowChild(key_type, capacity));
		internal_struct.child_data.push_back(InitializeArrowChild(value_type, capacity));

		result.child_data.push_back(move(internal_struct));
	}

	static void Append(ArrowAppendData &append_data, Vector &input, idx_t size) {
		UnifiedVectorFormat format;
		input.ToUnifiedFormat(size, format);

		AppendValidity(append_data, format, size);
		// maps exist as a struct of two lists, e.g. STRUCT(key VARCHAR[], value VARCHAR[])
		// since both lists are the same, arrow tries to be smart by storing the offsets only once
		// we can append the offsets from any of the two children
		auto &children = StructVector::GetEntries(input);
		UnifiedVectorFormat child_format;
		children[0]->ToUnifiedFormat(size, child_format);
		AppendListOffsets(append_data, child_format, size);

		// now we can append the children to the lists
		auto &struct_entries = StructVector::GetEntries(input);
		D_ASSERT(struct_entries.size() == 2);
		auto &key_vector = ListVector::GetEntry(*struct_entries[0]);
		auto &value_vector = ListVector::GetEntry(*struct_entries[1]);
		auto list_size = ListVector::GetListSize(*struct_entries[0]);
		D_ASSERT(list_size == ListVector::GetListSize(*struct_entries[1]));

		// perform the append
		auto &struct_data = append_data.child_data[0];
		auto &key_data = struct_data.child_data[0];
		auto &value_data = struct_data.child_data[1];
		key_data.append_vector(key_data, key_vector, list_size);
		value_data.append_vector(value_data, value_vector, list_size);

		append_data.row_count += size;
		struct_data.row_count += size;
	}

	static void Finalize(ArrowAppendData &append_data, const LogicalType &type, ArrowArray *result) {
		// set up the main map buffer
		result->n_buffers = 2;
		result->buffers[1] = append_data.main_buffer.data();

		// the main map buffer has a single child: a struct
		append_data.child_pointers.resize(1);
		result->children = append_data.child_pointers.data();
		result->n_children = 1;
		append_data.child_pointers[0] = FinalizeArrowChild(type, append_data.child_data[0]);

		// now that struct has two children: the key and the value type
		auto &struct_data = append_data.child_data[0];
		auto &struct_result = append_data.child_pointers[0];
		struct_data.child_pointers.resize(2);
		struct_result->n_children = 2;
		struct_result->children = struct_data.child_pointers.data();

		auto &key_type = MapType::KeyType(type);
		auto &value_type = MapType::ValueType(type);
		struct_data.child_pointers[0] = FinalizeArrowChild(key_type, struct_data.child_data[0]);
		struct_data.child_pointers[1] = FinalizeArrowChild(value_type, struct_data.child_data[1]);

		// keys cannot have null values
		if (struct_data.child_pointers[0]->null_count > 0) {
			throw std::runtime_error("Arrow doesn't accept NULL keys on Maps");
		}
	}
};

//! Append a data chunk to the underlying arrow array
void ArrowAppender::Append(DataChunk &input) {
	D_ASSERT(types == input.GetTypes());
	for (idx_t i = 0; i < input.ColumnCount(); i++) {
		root_data[i].append_vector(root_data[i], input.data[i], input.size());
	}
	row_count += input.size();
}
//===--------------------------------------------------------------------===//
// Initialize Arrow Child
//===--------------------------------------------------------------------===//
template <class OP>
static void InitializeFunctionPointers(ArrowAppendData &append_data) {
	append_data.initialize = OP::Initialize;
	append_data.append_vector = OP::Append;
	append_data.finalize = OP::Finalize;
}

static void InitializeFunctionPointers(ArrowAppendData &append_data, const LogicalType &type) {
	// handle special logical types
	switch (type.id()) {
	case LogicalTypeId::MAP:
		InitializeFunctionPointers<ArrowMapData>(append_data);
		return;
	case LogicalTypeId::DECIMAL:
		switch (type.InternalType()) {
		case PhysicalType::INT16:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t, int16_t>>(append_data);
			break;
		case PhysicalType::INT32:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t, int32_t>>(append_data);
			break;
		case PhysicalType::INT64:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t, int64_t>>(append_data);
			break;
		case PhysicalType::INT128:
			InitializeFunctionPointers<ArrowScalarData<hugeint_t>>(append_data);
			break;
		default:
			throw InternalException("Unsupported internal decimal type");
		}
		return;
	case LogicalTypeId::UUID:
		InitializeFunctionPointers<ArrowVarcharData<hugeint_t, ArrowUUIDConverter>>(append_data);
		return;
	case LogicalTypeId::ENUM:
		switch (type.InternalType()) {
		case PhysicalType::UINT8:
			InitializeFunctionPointers<ArrowEnumData<uint8_t>>(append_data);
			break;
		case PhysicalType::UINT16:
			InitializeFunctionPointers<ArrowEnumData<uint16_t>>(append_data);
			break;
		case PhysicalType::UINT32:
			InitializeFunctionPointers<ArrowEnumData<uint32_t>>(append_data);
			break;
		default:
			throw InternalException("Unsupported internal enum type");
		}
		return;
	default:
		break;
	}

	switch (type.InternalType()) {
	case PhysicalType::BOOL:
		InitializeFunctionPointers<ArrowBoolData>(append_data);
		break;
	case PhysicalType::VARCHAR:
		InitializeFunctionPointers<ArrowVarcharData<string_t>>(append_data);
		break;
	case PhysicalType::STRUCT:
		InitializeFunctionPointers<ArrowStructData>(append_data);
		break;
	case PhysicalType::LIST:
		InitializeFunctionPointers<ArrowListData>(append_data);
		break;
	case PhysicalType::INT8:
		InitializeFunctionPointers<ArrowScalarData<int8_t>>(append_data);
		break;
	case PhysicalType::INT16:
		InitializeFunctionPointers<ArrowScalarData<int16_t>>(append_data);
		break;
	case PhysicalType::INT32:
		InitializeFunctionPointers<ArrowScalarData<int32_t>>(append_data);
		break;
	case PhysicalType::INT64:
		InitializeFunctionPointers<ArrowScalarData<int64_t>>(append_data);
		break;
	case PhysicalType::UINT8:
		InitializeFunctionPointers<ArrowScalarData<uint8_t>>(append_data);
		break;
	case PhysicalType::UINT16:
		InitializeFunctionPointers<ArrowScalarData<uint16_t>>(append_data);
		break;
	case PhysicalType::UINT32:
		InitializeFunctionPointers<ArrowScalarData<uint32_t>>(append_data);
		break;
	case PhysicalType::UINT64:
		InitializeFunctionPointers<ArrowScalarData<uint64_t>>(append_data);
		break;
	case PhysicalType::INT128:
		InitializeFunctionPointers<ArrowScalarData<hugeint_t>>(append_data);
		break;
	case PhysicalType::FLOAT:
		InitializeFunctionPointers<ArrowScalarData<float>>(append_data);
		break;
	case PhysicalType::DOUBLE:
		InitializeFunctionPointers<ArrowScalarData<double>>(append_data);
		break;
	case PhysicalType::INTERVAL:
		InitializeFunctionPointers<ArrowScalarData<int64_t, interval_t, ArrowIntervalConverter>>(append_data);
		break;
	default:
		throw InternalException("FIXME: unsupported physical type");
	}
}

ArrowAppendData InitializeArrowChild(const LogicalType &type, idx_t capacity) {
	ArrowAppendData result;
	InitializeFunctionPointers(result, type);

	auto byte_count = (capacity + 7) / 8;
	result.validity.reserve(byte_count);
	result.initialize(result, type, capacity);
	return result;
}

static void ReleaseDuckDBArrowAppendArray(ArrowArray *array) {
	if (!array || !array->release) {
		return;
	}
	array->release = nullptr;
	auto holder = static_cast<ArrowAppendData *>(array->private_data);
	delete holder;
}

//===--------------------------------------------------------------------===//
// Finalize Arrow Child
//===--------------------------------------------------------------------===//
ArrowArray *FinalizeArrowChild(const LogicalType &type, ArrowAppendData &append_data) {
	auto result = make_unique<ArrowArray>();

	result->private_data = nullptr;
	result->release = ReleaseDuckDBArrowAppendArray;
	result->n_children = 0;
	result->null_count = 0;
	result->offset = 0;
	result->dictionary = nullptr;
	result->buffers = append_data.buffers.data();
	result->null_count = append_data.null_count;
	result->length = append_data.row_count;
	result->buffers[0] = append_data.validity.data();

	if (append_data.finalize) {
		append_data.finalize(append_data, type, result.get());
	}

	append_data.array = move(result);
	return append_data.array.get();
}

//! Returns the underlying arrow array
ArrowArray ArrowAppender::Finalize() {
	auto root_holder = make_unique<ArrowAppendData>();

	ArrowArray result;
	root_holder->child_pointers.resize(types.size());
	result.children = root_holder->child_pointers.data();
	result.n_children = types.size();

	// Configure root array
	result.length = row_count;
	result.n_children = types.size();
	result.n_buffers = 1;
	result.buffers = root_holder->buffers.data(); // there is no actual buffer there since we don't have NULLs
	result.offset = 0;
	result.null_count = 0; // needs to be 0
	result.dictionary = nullptr;
	root_holder->child_data = move(root_data);

	for (idx_t i = 0; i < root_holder->child_data.size(); i++) {
		root_holder->child_pointers[i] = FinalizeArrowChild(types[i], root_holder->child_data[i]);
	}

	// Release ownership to caller
	result.private_data = root_holder.release();
	result.release = ReleaseDuckDBArrowAppendArray;
	return result;
}

} // namespace duckdb
