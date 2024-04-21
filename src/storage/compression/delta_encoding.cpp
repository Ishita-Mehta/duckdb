#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/fsst.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "fsst.h"
#include "miniz_wrapper.hpp"

#include <string>

namespace duckdb {

void logMessage_DeltaEncoding(const std::string &message) {
	static std::ofstream logFile("C:/Tanmay/2023/USC/Spring 2024/ADS/Project/__forked_duckdb/duckdb/logging/log_delta_encode.txt", std::ios::app);
	logFile << message << std::endl;
}

void logUpdateState(const std::string &message) {
	static std::ofstream logFile(
	    "C:/Tanmay/2023/USC/Spring 2024/ADS/Project/__forked_duckdb/duckdb/logging/log_update_state.txt",
	    std::ios::app);
	logFile << message << std::endl;
}

void logStringScanPartial(const std::string &message) {
	static std::ofstream logFile(
	    "C:/Tanmay/2023/USC/Spring 2024/ADS/Project/__forked_duckdb/duckdb/logging/log_string_scan_partial.txt",
	    std::ios::app);
	logFile << message << std::endl;
}

void logStringFetchRow(const std::string &message) {
	static std::ofstream logFile(
	    "C:/Tanmay/2023/USC/Spring 2024/ADS/Project/__forked_duckdb/duckdb/logging/log_string_fetch_row.txt",
	    std::ios::app);
	logFile << message << std::endl;
}


typedef struct {
	uint32_t dict_size;
	uint32_t dict_end;
	uint32_t bitpacking_width;
	uint32_t delta_symbol_table_offset;
} delta_compression_header_t;

typedef struct Test_BPDeltaDecodeOffsets {
	idx_t delta_decode_start_row;
	idx_t bitunpack_alignment_offset;
	idx_t bitunpack_start_row;
	idx_t unused_delta_decoded_values;
	idx_t scan_offset;
	idx_t total_delta_decode_count;
	idx_t total_bitunpack_count;
} bp_delta_encoding_offsets_t;

struct DeltaEncodingStorage {
	static constexpr size_t COMPACTION_FLUSH_LIMIT = (size_t)Storage::BLOCK_SIZE / 5 * 4;
	static constexpr double MINIMUM_COMPRESSION_RATIO = 5;
	static constexpr double ANALYSIS_SAMPLE_SIZE = 0.25;

	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> analyze_state_p);
	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	template <bool ALLOW_DELTA_VECTORS = false>
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);

	static void SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container);
	static StringDictionaryContainer GetDictionary(ColumnSegment &segment, BufferHandle &handle);

	static char *FetchStringPointer(StringDictionaryContainer dict, data_ptr_t baseptr, int32_t dict_offset);
	static bp_delta_encoding_offsets_t CalculateBpDeltaOffsets(int64_t last_known_row, idx_t start, idx_t scan_count);
	static bool ParseDeltaSegmentHeader(data_ptr_t base_ptr, duckdb_fsst_decoder_t *decoder_out,
	                                   bitpacking_width_t *width_out);
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct DeltaAnalyzeState : public AnalyzeState {
	DeltaAnalyzeState() : count(0), delta_string_total_size(0), empty_strings(0) {
	}

	~DeltaAnalyzeState() override {
		if (delta_encoder) {
			duckdb_fsst_destroy(delta_encoder);
		}
	}

	duckdb_fsst_encoder_t *delta_encoder = nullptr;
	idx_t count;

	StringHeap delta_string_heap;
	vector<string_t> delta_strings;
	size_t delta_string_total_size;

	RandomEngine random_engine;
	bool have_valid_row = false;

	idx_t empty_strings;
};

unique_ptr<AnalyzeState> DeltaEncodingStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<DeltaAnalyzeState>();
}

bool DeltaEncodingStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<DeltaAnalyzeState>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	state.count += count;
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	// Note that we ignore the sampling in case we have not found any valid strings yet, this solves the issue of
	// not having seen any valid strings here leading to an empty fsst symbol table.
	bool sample_selected = !state.have_valid_row || state.random_engine.NextRandom() < ANALYSIS_SAMPLE_SIZE;

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);

		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}

		// We need to check all strings for this, otherwise we run in to trouble during compression if we miss ones
		auto string_size = data[idx].GetSize();
		if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
			return false;
		}

		if (!sample_selected) {
			continue;
		}

		if (string_size > 0) {
			state.have_valid_row = true;
			if (data[idx].IsInlined()) {
				state.delta_strings.push_back(data[idx]);
			} else {
				state.delta_strings.emplace_back(state.delta_string_heap.AddBlob(data[idx]));
			}
			state.delta_string_total_size += string_size;
		} else {
			state.empty_strings++;
		}
	}
	return true;
}

idx_t DeltaEncodingStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.Cast<DeltaAnalyzeState>();

	size_t compressed_dict_size = 0;
	size_t max_compressed_string_length = 0;

	auto string_count = state.delta_strings.size();

	if (!string_count) {
		return DConstants::INVALID_INDEX;
	}

	size_t output_buffer_size = 7 + 5 * state.delta_string_total_size; // size as specified in fsst.h
	// size_t output_buffer_size = state.delta_string_total_size;

	vector<size_t> delta_string_sizes;
	vector<unsigned char *> delta_string_ptrs;
	for (auto &str : state.delta_strings) {
		delta_string_sizes.push_back(str.GetSize());
		delta_string_ptrs.push_back((unsigned char *)str.GetData()); // NOLINT
	}

	state.delta_encoder = duckdb_fsst_create(string_count, &delta_string_sizes[0], &delta_string_ptrs[0], 0);

	// TODO: do we really need to encode to get a size estimate?
	auto compressed_ptrs = vector<unsigned char *>(string_count, nullptr);
	auto compressed_sizes = vector<size_t>(string_count, 0);
	unique_ptr<unsigned char[]> compressed_buffer(new unsigned char[output_buffer_size]);

	auto res =
	    duckdb_fsst_compress(state.delta_encoder, string_count, &delta_string_sizes[0], &delta_string_ptrs[0],
	                         output_buffer_size, compressed_buffer.get(), &compressed_sizes[0], &compressed_ptrs[0]);

	if (string_count != res) {
		throw std::runtime_error("Delta Encoding output buffer is too small unexpectedly");
	}

	// Sum and and Max compressed lengths
	for (auto &size : compressed_sizes) {
		compressed_dict_size += size;
		max_compressed_string_length = MaxValue(max_compressed_string_length, size);
	}
	D_ASSERT(compressed_dict_size == (compressed_ptrs[res - 1] - compressed_ptrs[0]) + compressed_sizes[res - 1]);

	auto minimum_width = BitpackingPrimitives::MinimumBitWidth(max_compressed_string_length);
	auto bitpacked_offsets_size =
	    BitpackingPrimitives::GetRequiredSize(string_count + state.empty_strings, minimum_width);

	auto estimated_base_size = (bitpacked_offsets_size + compressed_dict_size) * (1 / ANALYSIS_SAMPLE_SIZE);
	auto num_blocks = estimated_base_size / (Storage::BLOCK_SIZE - sizeof(duckdb_fsst_decoder_t));
	auto symtable_size = num_blocks * sizeof(duckdb_fsst_decoder_t);

	auto estimated_size = estimated_base_size + symtable_size;

	return estimated_size * MINIMUM_COMPRESSION_RATIO;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//

class DeltaCompressionState : public CompressionState {
public:
	explicit DeltaCompressionState(ColumnDataCheckpointer &checkpointer)
	    : checkpointer(checkpointer), function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_DELTA)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	~DeltaCompressionState() override {
		if (delta_encoder) {
			duckdb_fsst_destroy(delta_encoder);
		}
	}

	void Reset() {
		index_buffer.clear();
		current_width = 0;
		max_compressed_string_length = 0;
		last_fitting_size = 0;

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		current_handle = buffer_manager.Pin(current_segment->block);
		current_dictionary = DeltaEncodingStorage::GetDictionary(*current_segment, current_handle);
		current_end_ptr = current_handle.Ptr() + current_dictionary.end;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = std::move(compressed_segment);
		current_segment->function = function;
		Reset();
	}

	void UpdateState(string_t uncompressed_string, unsigned char *compressed_string, size_t compressed_string_len) {
		logUpdateState("Bhai yeh dekh input: " + uncompressed_string.GetString());


		std::string result;
		// Iterate through the compressed_string until the null terminator is encountered
		for (unsigned char *ptr = compressed_string; *ptr != '\0'; ++ptr) {
			result += *ptr; // Append each character to the result string
		}
		logUpdateState("Bhai compressed: " + result);

		if (!HasEnoughSpace(compressed_string_len)) {
			Flush();
			if (!HasEnoughSpace(compressed_string_len)) {
				throw InternalException("Delta Encoding string compression failed due to insufficient space in empty block");
			};
		}

		UncompressedStringStorage::UpdateStringStats(current_segment->stats, uncompressed_string);

		// Write string into dictionary
		current_dictionary.size += compressed_string_len;
		auto dict_pos = current_end_ptr - current_dictionary.size;
		memcpy(dict_pos, compressed_string, compressed_string_len);
		current_dictionary.Verify();

		// We just push the string length to effectively delta encode the strings
		index_buffer.push_back(NumericCast<uint32_t>(compressed_string_len));

		max_compressed_string_length = MaxValue(max_compressed_string_length, compressed_string_len);
		//max_compressed_string_length = compressed_string_len;

		current_width = BitpackingPrimitives::MinimumBitWidth(max_compressed_string_length);
		current_segment->count++;
	}

	void AddNull() {
		if (!HasEnoughSpace(0)) {
			Flush();
			if (!HasEnoughSpace(0)) {
				throw InternalException("Delta Encoding string compression failed due to insufficient space in empty block");
			};
		}
		index_buffer.push_back(0);
		current_segment->count++;
	}

	void AddEmptyString() {
		AddNull();
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, "");
	}

	size_t GetRequiredSize(size_t string_len) {
		bitpacking_width_t required_minimum_width;
		if (string_len > max_compressed_string_length) {
			required_minimum_width = BitpackingPrimitives::MinimumBitWidth(string_len);
		} else {
			required_minimum_width = current_width;
		}

		size_t current_dict_size = current_dictionary.size;
		idx_t current_string_count = index_buffer.size();

		size_t dict_offsets_size =
		    BitpackingPrimitives::GetRequiredSize(current_string_count + 1, required_minimum_width);

		// TODO switch to a symbol table per RowGroup, saves a bit of space
		return sizeof(delta_compression_header_t) + current_dict_size + dict_offsets_size + string_len +
		       fsst_serialized_symbol_table_size;
	}

	// Checks if there is enough space, if there is, sets last_fitting_size
	bool HasEnoughSpace(size_t string_len) {
		auto required_size = GetRequiredSize(string_len);

		if (required_size <= Storage::BLOCK_SIZE) {
			last_fitting_size = required_size;
			return true;
		}
		return false;
	}

	void Flush(bool final = false) {
		auto next_start = current_segment->start + current_segment->count;

		auto segment_size = Finalize();
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), segment_size);

		if (!final) {
			CreateEmptySegment(next_start);
		}
	}

	idx_t Finalize() {
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		auto handle = buffer_manager.Pin(current_segment->block);
		D_ASSERT(current_dictionary.end == Storage::BLOCK_SIZE);

		// calculate sizes
		auto compressed_index_buffer_size =
		    BitpackingPrimitives::GetRequiredSize(current_segment->count, current_width);
		auto total_size = sizeof(delta_compression_header_t) + compressed_index_buffer_size + current_dictionary.size +
		                  fsst_serialized_symbol_table_size;

		if (total_size != last_fitting_size) {
			throw InternalException("Delta Encoding string compression failed due to incorrect size calculation");
		}

		// calculate ptr and offsets
		auto base_ptr = handle.Ptr();
		auto header_ptr = reinterpret_cast<delta_compression_header_t *>(base_ptr);
		auto compressed_index_buffer_offset = sizeof(delta_compression_header_t);
		auto symbol_table_offset = compressed_index_buffer_offset + compressed_index_buffer_size;

		D_ASSERT(current_segment->count == index_buffer.size());
		BitpackingPrimitives::PackBuffer<sel_t, false>(base_ptr + compressed_index_buffer_offset,
		                                               reinterpret_cast<uint32_t *>(index_buffer.data()),
		                                               current_segment->count, current_width);

		// Write the fsst symbol table or nothing
		if (delta_encoder != nullptr) {
			memcpy(base_ptr + symbol_table_offset, &fsst_serialized_symbol_table[0], fsst_serialized_symbol_table_size);
		} else {
			memset(base_ptr + symbol_table_offset, 0, fsst_serialized_symbol_table_size);
		}

		Store<uint32_t>(NumericCast<uint32_t>(symbol_table_offset),
		                data_ptr_cast(&header_ptr->delta_symbol_table_offset));
		Store<uint32_t>((uint32_t)current_width, data_ptr_cast(&header_ptr->bitpacking_width));

		if (total_size >= DeltaEncodingStorage::COMPACTION_FLUSH_LIMIT) {
			// the block is full enough, don't bother moving around the dictionary
			return Storage::BLOCK_SIZE;
		}
		// the block has space left: figure out how much space we can save
		auto move_amount = Storage::BLOCK_SIZE - total_size;
		// move the dictionary so it lines up exactly with the offsets
		auto new_dictionary_offset = symbol_table_offset + fsst_serialized_symbol_table_size;
		memmove(base_ptr + new_dictionary_offset, base_ptr + current_dictionary.end - current_dictionary.size,
		        current_dictionary.size);
		current_dictionary.end -= move_amount;
		D_ASSERT(current_dictionary.end == total_size);
		// write the new dictionary (with the updated "end")
		DeltaEncodingStorage::SetDictionary(*current_segment, handle, current_dictionary);

		return total_size;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// State regarding current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	StringDictionaryContainer current_dictionary;
	data_ptr_t current_end_ptr;

	// Buffers and map for current segment
	vector<uint32_t> index_buffer;

	size_t max_compressed_string_length;
	bitpacking_width_t current_width;
	idx_t last_fitting_size;

	duckdb_fsst_encoder_t *delta_encoder = nullptr;
	unsigned char fsst_serialized_symbol_table[sizeof(duckdb_fsst_decoder_t)];
	size_t fsst_serialized_symbol_table_size = sizeof(duckdb_fsst_decoder_t);
};

unique_ptr<CompressionState> DeltaEncodingStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                          unique_ptr<AnalyzeState> analyze_state_p) {
	auto &analyze_state = analyze_state_p->Cast<DeltaAnalyzeState>();
	auto compression_state = make_uniq<DeltaCompressionState>(checkpointer);

	if (analyze_state.delta_encoder == nullptr) {
		throw InternalException("No encoder found during Delta Encoding compression");
	}

	compression_state->delta_encoder = analyze_state.delta_encoder;
	compression_state->fsst_serialized_symbol_table_size =
	    duckdb_fsst_export(compression_state->delta_encoder, &compression_state->fsst_serialized_symbol_table[0]);
	analyze_state.delta_encoder = nullptr;

	return std::move(compression_state);
}

void DeltaEncodingStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<DeltaCompressionState>();
	logMessage_DeltaEncoding("In Compress");
	// Get vector data
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	// Collect pointers to strings to compress
	vector<int32_t> values;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		logMessage_DeltaEncoding("data[idx]: " + data[idx].GetString());
		// Note: we treat nulls and empty strings the same
		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
			values.push_back(0); // Represent null or empty string as 0
		} else {
			logMessage_DeltaEncoding("data[idx] pushed to values[]: " + data[idx].GetString());
			// Convert string to integer
			values.push_back(std::stoll(data[idx].GetData()));
		}
	}

	// Compress values using delta encoding
	vector<int32_t> delta_encoded;
	delta_encoded.push_back(values[0]); // First value remains the same
	for (size_t i = 1; i < values.size(); i++) {
		int32_t delta = values[i] - values[i - 1];
		delta_encoded.push_back(delta);
	}

	// Push the delta-encoded values to the compression state one by one
	for (size_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
			state.AddNull();
		} else {
			// Convert delta-encoded integer back to string
			string encoded_str = to_string(delta_encoded[i]);
			logMessage_DeltaEncoding("encoded_str: " + encoded_str);
			logMessage_DeltaEncoding("encoded_str_size: " + std::to_string(encoded_str.length()));
			state.UpdateState(data[idx], (unsigned char *)encoded_str.c_str(), encoded_str.size());
		}
	}
}

void DeltaEncodingStorage::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<DeltaCompressionState>();
	state.Flush(true);
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct DeltaEncodingScanState : public StringScanState {
	DeltaEncodingScanState() {
		ResetStoredDelta();
	}

	buffer_ptr<void> duckdb_fsst_decoder;
	bitpacking_width_t current_width;

	// To speed up delta decoding we store the last index
	uint32_t last_known_index;
	int64_t last_known_row;

	void StoreLastDelta(uint32_t value, int64_t row) {
		last_known_index = value;
		last_known_row = row;
	}
	void ResetStoredDelta() {
		last_known_index = 0;
		last_known_row = -1;
	}
};

unique_ptr<SegmentScanState> DeltaEncodingStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_uniq<DeltaEncodingScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	state->handle = buffer_manager.Pin(segment.block);
	auto base_ptr = state->handle.Ptr() + segment.GetBlockOffset();

	state->duckdb_fsst_decoder = make_buffer<duckdb_fsst_decoder_t>();
	auto retval = ParseDeltaSegmentHeader(
	    base_ptr, reinterpret_cast<duckdb_fsst_decoder_t *>(state->duckdb_fsst_decoder.get()), &state->current_width);
	if (!retval) {
		state->duckdb_fsst_decoder = nullptr;
	}

	return std::move(state);
}

void DeltaEncoding_DecodeIndices(uint32_t *buffer_in, uint32_t *buffer_out, idx_t decode_count, uint32_t last_known_value) {
	buffer_out[0] = buffer_in[0];
	buffer_out[0] += last_known_value;
	for (idx_t i = 1; i < decode_count; i++) {
		buffer_out[i] = buffer_in[i] + buffer_out[i - 1];
	}
}

void Delta_BitUnpackRange(data_ptr_t src_ptr, data_ptr_t dst_ptr, idx_t count, idx_t row, bitpacking_width_t width) {
	auto bitunpack_src_ptr = &src_ptr[(row * width) / 8];
	BitpackingPrimitives::UnPackBuffer<uint32_t>(dst_ptr, bitunpack_src_ptr, count, width);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <bool ALLOW_DELTA_VECTORS>
void DeltaEncodingStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                    idx_t result_offset) {
	logStringScanPartial("In StringScanPartial");
	auto &scan_state = state.scan_state->Cast<DeltaEncodingScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	bool enable_fsst_vectors;
	if (ALLOW_DELTA_VECTORS) {
		auto &config = DBConfig::GetConfig(segment.db);
		enable_fsst_vectors = config.options.enable_fsst_vectors;
	} else {
		enable_fsst_vectors = false;
	}

	auto baseptr = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto dict = GetDictionary(segment, scan_state.handle);
	auto base_data = data_ptr_cast(baseptr + sizeof(delta_compression_header_t));
	string_t *result_data;

	if (scan_count == 0) {
		return;
	}

	if (enable_fsst_vectors) {
		D_ASSERT(result_offset == 0);
		if (scan_state.duckdb_fsst_decoder) {
			D_ASSERT(result_offset == 0 || result.GetVectorType() == VectorType::FSST_VECTOR);
			result.SetVectorType(VectorType::FSST_VECTOR);
			DeltaEncodingVector::RegisterDecoder(result, scan_state.duckdb_fsst_decoder);
			result_data = DeltaEncodingVector::GetCompressedData<string_t>(result);
		} else {
			D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
			result_data = FlatVector::GetData<string_t>(result);
		}
	} else {
		D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
		result_data = FlatVector::GetData<string_t>(result);
	}

	// My addition
	/*for (idx_t i = 0; i < result_data->GetSize(); i++) {
		logStringScanPartial("SSP: " + result_data[i].GetString());
	}*/
	// End
	//logStringScanPartial("SSP: " + result_data[0].GetString());


	if (start == 0 || scan_state.last_known_row >= (int64_t)start) {
		scan_state.ResetStoredDelta();
	}

	auto offsets = CalculateBpDeltaOffsets(scan_state.last_known_row, start, scan_count);

	auto bitunpack_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_bitunpack_count]);
	Delta_BitUnpackRange(base_data, data_ptr_cast(bitunpack_buffer.get()), offsets.total_bitunpack_count,
	               offsets.bitunpack_start_row, scan_state.current_width);
	auto delta_decode_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_delta_decode_count]);
	DeltaEncoding_DecodeIndices(bitunpack_buffer.get() + offsets.bitunpack_alignment_offset, delta_decode_buffer.get(),
	                   offsets.total_delta_decode_count, scan_state.last_known_index);

	for (idx_t i = 0; i < scan_count; i++) {
		uint32_t string_length = bitunpack_buffer[i + offsets.scan_offset];
		result_data[i] = UncompressedStringStorage::FetchStringFromDict(
		    segment, dict, result, baseptr, delta_decode_buffer[i + offsets.unused_delta_decoded_values],
		    string_length);
		// Decompressing
		if (i != 0) {
			result_data[i] = std::to_string(std::stoi(result_data[i - 1].GetString()) + std::stoi(result_data[i].GetString()));
		}
		DeltaEncodingVector::SetCount(result, scan_count);
	}

	//if (enable_fsst_vectors) {
	//	// Lookup decompressed offsets in dict
	//	for (idx_t i = 0; i < scan_count; i++) {
	//		uint32_t string_length = bitunpack_buffer[i + offsets.scan_offset];
	//		result_data[i] = UncompressedStringStorage::FetchStringFromDict(
	//		    segment, dict, result, baseptr, delta_decode_buffer[i + offsets.unused_delta_decoded_values],
	//		    string_length);
	//		DeltaEncodingVector::SetCount(result, scan_count);
	//	}
	//}
	// else {
	//	// Just decompress
	//	for (idx_t i = 0; i < scan_count; i++) {
	//		uint32_t str_len = bitunpack_buffer[i + offsets.scan_offset];
	//		auto str_ptr = DeltaEncodingStorage::FetchStringPointer(
	//		    dict, baseptr, delta_decode_buffer[i + offsets.unused_delta_decoded_values]);

	//		if (str_len > 0) {
	//			result_data[i + result_offset] =
	//			    FSSTPrimitives::DecompressValue(scan_state.duckdb_fsst_decoder.get(), result, str_ptr, str_len);
	//		} else {
	//			result_data[i + result_offset] = string_t(nullptr, 0);
	//		}
	//	}
	//}

	scan_state.StoreLastDelta(delta_decode_buffer[scan_count + offsets.unused_delta_decoded_values - 1],
	                          start + scan_count - 1);
}

void DeltaEncodingStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	StringScanPartial<true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
void DeltaEncodingStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                 idx_t result_idx) {
	logStringFetchRow("In StringFetchRow");
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	auto base_ptr = handle.Ptr() + segment.GetBlockOffset();
	auto base_data = data_ptr_cast(base_ptr + sizeof(delta_compression_header_t));
	auto dict = GetDictionary(segment, handle);

	duckdb_fsst_decoder_t decoder;
	bitpacking_width_t width;
	auto have_symbol_table = ParseDeltaSegmentHeader(base_ptr, &decoder, &width);

	auto result_data = FlatVector::GetData<string_t>(result);

	if (have_symbol_table) {
		// We basically just do a scan of 1 which is kinda expensive as we need to repeatedly delta decode until we
		// reach the row we want, we could consider a more clever caching trick if this is slow
		auto offsets = CalculateBpDeltaOffsets(-1, row_id, 1);

		auto bitunpack_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_bitunpack_count]);
		Delta_BitUnpackRange(base_data, data_ptr_cast(bitunpack_buffer.get()), offsets.total_bitunpack_count,
		               offsets.bitunpack_start_row, width);
		auto delta_decode_buffer = unique_ptr<uint32_t[]>(new uint32_t[offsets.total_delta_decode_count]);
		DeltaEncoding_DecodeIndices(bitunpack_buffer.get() + offsets.bitunpack_alignment_offset, delta_decode_buffer.get(),
		                   offsets.total_delta_decode_count, 0);

		uint32_t string_length = bitunpack_buffer[offsets.scan_offset];

		string_t compressed_string = UncompressedStringStorage::FetchStringFromDict(
		    segment, dict, result, base_ptr, delta_decode_buffer[offsets.unused_delta_decoded_values], string_length);

		logStringFetchRow("StringFetchRow: " + compressed_string.GetString());
		// result_data[result_idx] = compressed_string;

		result_data[result_idx] = FSSTPrimitives::DecompressValue((void *)&decoder, result, compressed_string.GetData(),
		                                                          compressed_string.GetSize());
	} else {
		// There's no fsst symtable, this only happens for empty strings or nulls, we can just emit an empty string
		result_data[result_idx] = string_t(nullptr, 0);
	}
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction DeltaEncodingCompressionFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(
	    CompressionType::COMPRESSION_DELTA, data_type, DeltaEncodingStorage::StringInitAnalyze, DeltaEncodingStorage::StringAnalyze,
	    DeltaEncodingStorage::StringFinalAnalyze, DeltaEncodingStorage::InitCompression, DeltaEncodingStorage::Compress,
	    DeltaEncodingStorage::FinalizeCompress, DeltaEncodingStorage::StringInitScan, DeltaEncodingStorage::StringScan,
	    DeltaEncodingStorage::StringScanPartial<false>, DeltaEncodingStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool DeltaEncodingCompressionFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}

//===--------------------------------------------------------------------===//
// Helper Functions
//===--------------------------------------------------------------------===//
void DeltaEncodingStorage::SetDictionary(ColumnSegment &segment, BufferHandle &handle, StringDictionaryContainer container) {
	auto header_ptr = reinterpret_cast<delta_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	Store<uint32_t>(container.size, data_ptr_cast(&header_ptr->dict_size));
	Store<uint32_t>(container.end, data_ptr_cast(&header_ptr->dict_end));
}

StringDictionaryContainer DeltaEncodingStorage::GetDictionary(ColumnSegment &segment, BufferHandle &handle) {
	auto header_ptr = reinterpret_cast<delta_compression_header_t *>(handle.Ptr() + segment.GetBlockOffset());
	StringDictionaryContainer container;
	container.size = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_size));
	container.end = Load<uint32_t>(data_ptr_cast(&header_ptr->dict_end));
	return container;
}

char *DeltaEncodingStorage::FetchStringPointer(StringDictionaryContainer dict, data_ptr_t baseptr, int32_t dict_offset) {
	if (dict_offset == 0) {
		return nullptr;
	}

	auto dict_end = baseptr + dict.end;
	auto dict_pos = dict_end - dict_offset;
	return char_ptr_cast(dict_pos);
}

// Returns false if no symbol table was found. This means all strings are either empty or null
bool DeltaEncodingStorage::ParseDeltaSegmentHeader(data_ptr_t base_ptr, duckdb_fsst_decoder_t *decoder_out,
                                         bitpacking_width_t *width_out) {
	auto header_ptr = reinterpret_cast<delta_compression_header_t *>(base_ptr);
	auto delta_symbol_table_offset = Load<uint32_t>(data_ptr_cast(&header_ptr->delta_symbol_table_offset));
	*width_out = (bitpacking_width_t)(Load<uint32_t>(data_ptr_cast(&header_ptr->bitpacking_width)));
	return duckdb_fsst_import(decoder_out, base_ptr + delta_symbol_table_offset);
}

// The calculation of offsets and counts while scanning or fetching is a bit tricky, for two reasons:
// - bitunpacking needs to be aligned to BITPACKING_ALGORITHM_GROUP_SIZE
// - delta decoding needs to decode from the last known value.
bp_delta_encoding_offsets_t DeltaEncodingStorage::CalculateBpDeltaOffsets(int64_t last_known_row, idx_t start, idx_t scan_count) {
	D_ASSERT((idx_t)(last_known_row + 1) <= start);
	bp_delta_encoding_offsets_t result;

	result.delta_decode_start_row = (idx_t)(last_known_row + 1);
	result.bitunpack_alignment_offset =
	    result.delta_decode_start_row % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
	result.bitunpack_start_row = result.delta_decode_start_row - result.bitunpack_alignment_offset;
	result.unused_delta_decoded_values = start - result.delta_decode_start_row;
	result.scan_offset = result.bitunpack_alignment_offset + result.unused_delta_decoded_values;
	result.total_delta_decode_count = scan_count + result.unused_delta_decoded_values;
	result.total_bitunpack_count =
	    BitpackingPrimitives::RoundUpToAlgorithmGroupSize<idx_t>(scan_count + result.scan_offset);

	D_ASSERT(result.total_delta_decode_count + result.bitunpack_alignment_offset <= result.total_bitunpack_count);
	return result;
}

} // namespace duckdb
