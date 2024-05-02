#include "snappy.h" // Snappy compression library

#include "duckdb/common/string_util.hpp"

#include <iostream>
#include <string>

namespace duckdb {

void logMessage(const std::string &message) {
	static std::ofstream logFile("C:/Users/ishit/ADS project/duckdb/logs/loggers.txt", std::ios::app);
	logFile << message << std::endl;
	logFile.flush();
}

struct SnappyCompressionHeader {
	uint32_t compressed_size;   // Size of the compressed data
	uint32_t uncompressed_size; // Size of the uncompressed data before compression
};

struct SnappyStorage {
	static constexpr double MINIMUM_COMPRESSION_RATIO = 1.2;
	// Analyze Phase
	static unique_ptr<AnalyzeState> StringInitAnalyze(ColumnData &col_data, PhysicalType type);
	static bool StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count);
	static idx_t StringFinalAnalyze(AnalyzeState &state_p);

	// Compress Phase
	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> analyze_state_p);

	static void Compress(CompressionState &state_p, Vector &scan_vector, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);

	// Scan Phase
	static unique_ptr<SegmentScanState> StringInitScan(ColumnSegment &segment);
	static void StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
	                              idx_t result_offset);
	static void StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result);
	static void StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
	                           idx_t result_idx);
};

//--------------------------------------------------------------------
//============
// ANALYZE
//============
//--------------------------------------------------------------------

struct SnappyAnalyzeState : public AnalyzeState {
	SnappyAnalyzeState() : count(0), snappy_string_total_size(0), empty_strings(0) {
	}

	//~SnappyAnalyzeState() override {

	//}

	idx_t count;

	StringHeap snappy_string_heap;
	vector<string_t> snappy_strings;
	size_t snappy_string_total_size;

	RandomEngine random_engine;
	bool have_valid_row = false;

	idx_t empty_strings;
};

unique_ptr<AnalyzeState> SnappyStorage::StringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	// logMessage("Entered StringInitAnalyze function.");
	return make_uniq<SnappyAnalyzeState>();
}

bool SnappyStorage::StringAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	// AnalyzeState &state_p: a reference to an AnalyzeState object which holds the state of the analysis process
	// Vector &input: the vector that contains the strings to be analyzed
	// idx_t count: the number of elements in the vector to analyze

	// logMessage("Entered StringAnalyze function.");
	//  casts the AnalyzeState object to SnappyAnalyzeState object
	//  SnappyAnalyzeState is a more specific state class suitable for Snappy compression
	//  It contains Snappy specific data such as a list of strings, total size of strings and counters for empty strings
	auto &state = state_p.Cast<SnappyAnalyzeState>();

	// Input vector in converted to UnifiedVectorFormat - It standardizes the access to data within the vector, handling
	// complexities like data indirection (selection vectors) and nullity checks efficiently
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	// The total count of processed strings is updated. Data is fetched in a standardized format for easier processing.
	state.count += count;
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
	// logMessage("vector string from idx");
	// logMessage(input.ToString(count));
	// logMessage("data->GetString():");
	// logMessage(data->GetString());
	// logMessage("data->GetData():");
	// logMessage(data->GetData());

	// logMessage(std::to_string(typeid(data).name()));

	// logMessage("Count:");
	// logMessage(std::to_string(count));
	string all_data;

	// std::vector<char> compressed_data(compressed_size);

	// for (idx_t i = 0; i < count; i++) {
	//	auto idx = vdata.sel->get_index(i);
	//	all_data += data->GetData();
	//	; // Concatenate the current string
	//	all_data += " ";
	//	//auto original_string_size = data[idx].GetSize();
	//	//logMessage("String:");
	//	//logMessage(data->GetString());
	//	//logMessage(std::to_string(original_string_size));
	//	//string compressedoutput;

	//	//auto compressed_size = snappy::Compress(data->GetData(), original_string_size, &compressedoutput);
	//	//// size_t Compress(const char* input, size_t input_length, string* output);
	//	//logMessage(compressedoutput);
	//	//logMessage(std::to_string(compressed_size));
	//	//logMessage("why are we not exiting this loop");
	//}
	// logMessage("Continuing");
	////logMessage(all_data);
	// string compressed;

	// logMessage("all data size:");
	// logMessage(std::to_string(all_data.size()));
	// auto compressed_size =  snappy::Compress(all_data.data(), all_data.size(), &compressed);
	// logMessage("compressed_size");
	// logMessage(std::to_string(compressed_size));

	// Loops through each element in the vector to analyze its properties and suitability for compression:
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);

		// Null Check: Skips processing for null values.
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}

		// We need to check all strings for this, otherwise we run in to trouble during compression if we miss ones
		// Size Check: Strings exceeding a predefined size limit (StringUncompressed::STRING_BLOCK_LIMIT) are deemed
		// unsuitable for compression, and the method returns false.
		auto string_size = data[idx].GetSize();
		if (string_size >= StringUncompressed::STRING_BLOCK_LIMIT) {
			// logMessage("Returning False and Exiting StringAnalyze");
			return false;
		}

		// Checks if the string has a non-zero size.
		if (string_size > 0) {
			state.have_valid_row = true;
			if (data[idx].IsInlined()) {
				// If data[idx].IsInlined(), the string is directly pushed to state.snappy_strings.
				state.snappy_strings.push_back(data[idx]);
			} else {
				// If not inlined, the string is added to a heap (for larger strings) before being added to
				// state.snappy_strings.
				state.snappy_strings.emplace_back(state.snappy_string_heap.AddBlob(data[idx]));
			}
			// The total size of processed strings is updated.
			state.snappy_string_total_size += string_size;
		} else {
			// Increments the counter for empty strings.
			state.empty_strings++;
		}
	}
	// logMessage("Returning True and Exiting StringAnalyze");

	return true;
}

idx_t SnappyStorage::StringFinalAnalyze(AnalyzeState &state_p) {
	// casting the general AnalyzeState to a more specific SnappyAnalyzeState,
	//  which contains Snappy-specific data such as a collection of strings and their total size.
	// logMessage("Entered StringFinalAnalyze function.");
	auto &state = state_p.Cast<SnappyAnalyzeState>();
	// You can use Snappy's compression to get an actual measure of the
	// compressed sizes for an accurate estimation, or use average compression ratios known for your data types as a
	// heuristic.
	// logMessage("total size of strings:");
	// logMessage(std::to_string(state.snappy_string_total_size));
	size_t compressed_string_size_sum = snappy::MaxCompressedLength(state.snappy_string_total_size);
	// logMessage("compressed_string_size_sum");
	// logMessage(compressed_string_size_sum);
	// logMessage(compressed_string_size_sum->GetString());
	// logMessage(std::to_string(compressed_string_size_sum));

	// maximum length of any compressed string
	size_t max_compressed_string_length = 0;

	auto string_count = state.snappy_strings.size();
	// If there are no strings to compress (string_count is zero), the function returns an invalid index,
	// indicating that no compression can be performed.
	if (!string_count) {
		return DConstants::INVALID_INDEX;
	}
	// Calculates an initial buffer size (output_buffer_size) which is based on the total size of all strings to be
	// compressed.
	size_t output_buffer_size = 7 + 2 * state.snappy_string_total_size; // size as specified in fsst.h

	// Populates two vectors (snappy_string_sizes and snappy_string_ptrs) with the sizes and pointers of the strings,
	// preparing them for compression.
	vector<size_t> snappy_string_sizes;
	vector<unsigned char *> snappy_string_ptrs;
	for (auto &str : state.snappy_strings) {
		snappy_string_sizes.push_back(str.GetSize());
		snappy_string_ptrs.push_back((unsigned char *)str.GetData()); // NOLINT
	}

	return compressed_string_size_sum * MINIMUM_COMPRESSION_RATIO;
}

//--------------------------------------------------------------------
//============
// COMPRESS
//============
//--------------------------------------------------------------------

class SnappyCompressionState : public CompressionState {
public: // maintain state during the compression and analysis phases.
	explicit SnappyCompressionState(ColumnDataCheckpointer &checkpointer)
	    : checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_SNAPPY)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	void CreateEmptySegment(idx_t row_start) {
		// logMessage("Entered CreateEmptySegment.");
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		current_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment->function = function;
		Reset();
	}

	void Reset() {
		// logMessage("Entered Reset.");
		compressed_data.clear(); // Clear any previous data
		current_size = 0;        // Reset the current size of compressed data

		// Reset the pointers into the current segment
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		current_handle = buffer_manager.Pin(current_segment->block);
		current_end_ptr = current_handle.Ptr(); // Point to the start as no dictionary is used
	}

	void UpdateState(string_t uncompressed_string, unsigned char *compressed_string, size_t total_size,
	                 int valid_rows) {
		// logMessage("Entered UpdateState.");

		if (!HasEnoughSpace(total_size)) {
			// logMessage("Doesn't have enough space, will be flushed");
			Flush();
			if (!HasEnoughSpace(total_size)) {
				// logMessage("Snappy compression failed due to insufficient space in block");
				throw InternalException("Snappy compression failed due to insufficient space in block");
			};
		}
		// logMessage("There is enough space");

		// Create and store the header
		SnappyCompressionHeader header;
		header.compressed_size = static_cast<uint32_t>(total_size - sizeof(header)); // size of just the compressed data
		header.uncompressed_size =
		    static_cast<uint32_t>(uncompressed_string.GetSize()); // you need to set this from the input

		// Write the compressed string with its header to the segment's buffer
		memcpy(current_end_ptr + current_size, compressed_string, total_size);
		current_size += total_size;
		// current_segment->count += 1;
		current_segment->count += valid_rows;
		// logMessage("current_size:");
		// logMessage(std::to_string(current_size));
		// logMessage("current_segment->count:");
		// logMessage(std::to_string(current_segment->count));

		// logMessage("Exiting UpdateState");
	}

	void AddNull() {
		// logMessage("Entered AddNull.");
		if (!HasEnoughSpace(0)) {
			// logMessage("Doesnt have enough space will be flushed");
			Flush();
			if (!HasEnoughSpace(0)) {
				// logMessage("Snappy compression failed due to insufficient space in block");
				throw InternalException("Snappy compression failed due to insufficient space in block");
			};
		}
		current_segment->count++;
	}

	void AddEmptyString() {
		// logMessage("Entered addEmptyString.");
		AddNull();
		UncompressedStringStorage::UpdateStringStats(current_segment->stats, "");
	}

	size_t GetRequiredSize(size_t string_len) {
		// logMessage("Entered GetRequiredSize.");
		return current_size + string_len; // Directly add the size of the string to current buffer size
	}

	bool HasEnoughSpace(size_t string_len) {
		// logMessage("Entered HasEnoughSpace.");
		return (current_size + string_len <= Storage::BLOCK_SIZE);
	}

	void Flush(bool final = false) {
		// logMessage("Entered Flush.");
		// logMessage("Flushing segment - Current Size: " + std::to_string(current_size) +
		//          ", Count: " + std::to_string(current_segment->count));

		if (current_size > 0) {
			auto segment_size = Finalize(); // Ensure Finalize calculates and returns the size correctly
			auto &state = checkpointer.GetCheckpointState();
			state.FlushSegment(std::move(current_segment), segment_size);
		}

		if (!final) {
			CreateEmptySegment(current_segment->start + current_segment->count);
		}
	}

	idx_t Finalize() {
		// logMessage("Entered Finalize.");
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		auto handle = buffer_manager.Pin(current_segment->block);
		memcpy(handle.Ptr(), current_end_ptr, current_size); // Ensure all data is copied from the temporary buffer
		// logMessage("finalize current_size:");
		// logMessage(std::to_string(current_size));
		return current_size; // Return the total size of data written to the segment
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;

	// State regarding the current segment
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle current_handle;
	data_ptr_t current_end_ptr;

	// A vector to store compressed data temporarily if needed
	std::vector<uint8_t> compressed_data;
	size_t current_size;
};

// ColumnDataCheckpointer &checkpointer: A reference to an object responsible for managing the data checkpointing
// process, which includes tasks such as writing compressed data to storage. This object likely handles aspects of data
// management such as tracking which data has been processed, ensuring data integrity during compression, and possibly
// managing I/O operations.

// unique_ptr<AnalyzeState> analyze_state_p: A unique pointer to an AnalyzeState object that holds the results and state
// from the analysis phase. This object is specifically tailored to the needs of the Snappy compression, encapsulated in
// an SnappyAnalyzeState.
unique_ptr<CompressionState> SnappyStorage::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                            unique_ptr<AnalyzeState> analyze_state_p) {

	// casts the generic AnalyzeState to SnappyAnalyzeState, which is a more specialized class that contains
	// Snappy-specific
	// logMessage("Entered InitCompression.");
	auto &analyze_state = analyze_state_p->Cast<SnappyAnalyzeState>();
	// Here, a new SnappyCompressionState object is created using a factory function make_uniq (likely a variant of
	// std::make_unique). This object is initialized with the checkpointer to handle aspects of compression state
	// management, like buffering and writing compressed data.

	auto compression_state = make_uniq<SnappyCompressionState>(checkpointer);

	return compression_state;
}

// void SnappyStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
//	// Cast the general CompressionState to a SnappyCompressionState
//	logMessage("Entered Compress");
//	auto &state = state_p.Cast<SnappyCompressionState>();
//
//	// Get vector data
//	UnifiedVectorFormat vdata;
//	scan_vector.ToUnifiedFormat(count, vdata);
//	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
//	logMessage("data get string in compress");
//	logMessage(data->GetString());
//
//	std::stringstream buffer; // Use a stringstream to accumulate data
//
//	//logMessage("Entered Compress");
//
//     for (idx_t i = 0; i < count; i++) {
//		auto idx = vdata.sel->get_index(i);
//		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
//			state.AddNull();
//			continue;
//		}
//
//		string_t current_string = data[idx];
//		buffer.write(current_string.GetData(), current_string.GetSize());
//	}
//
//	std::string uncompressed_data = buffer.str();
//	//std::string compressed_data;
//	std::vector<unsigned char> compressed_data(snappy::MaxCompressedLength(uncompressed_data.size()));
//	size_t compressed_size = 0;
//	snappy::RawCompress(uncompressed_data.data(), uncompressed_data.size(),
//	                    reinterpret_cast<char *>(compressed_data.data()), &compressed_size);
//	compressed_data.resize(compressed_size); // Adjust the size of the vector to the actual compressed size
//
//	logMessage("Uncompressed blob:");
//	logMessage(uncompressed_data.data());
//	logMessage("Size of uncompressed blob:");
//	logMessage(std::to_string(uncompressed_data.size()));
//
//	logMessage("Size of compressed data:");
//	logMessage(std::to_string(compressed_size));
//
//
//
//		//// Compress each valid string using Snappy
//		//string_t current_string = data[idx];
//		//logMessage("Uncompressed Size:");
//		//logMessage(std::to_string(current_string.GetSize()));
//		//size_t compressed_size = snappy::MaxCompressedLength(current_string.GetSize());
//		//logMessage("compressed_size:");
//		//logMessage(std::to_string(compressed_size));
//		//std::vector<char> compressed_data(compressed_size);
//		//logMessage("compressed_data.data():");
//		//logMessage(compressed_data.data());
//
//		//// Perform the compression
//		//snappy::RawCompress(current_string.GetData(),
//		//	current_string.GetSize(),
//		//	compressed_data.data(),
//		//    &compressed_size);
//		///*  void RawCompress(const char* input,
//   //                 size_t input_length,
//   //                 char* compressed,
//   //                 size_t* compressed_length);*/
//
//		//logMessage("Post Compression size:");
//		//logMessage(std::to_string(compressed_size));
//
//		// Update the state with the compressed data
//	state.UpdateState(compressed_data.data(), compressed_data.size());
//
//
// }
void SnappyStorage::Compress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	// logMessage("Entered Compress");
	auto &state = state_p.Cast<SnappyCompressionState>();
	int valid_rows = 0;

	// Get vector data
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<string_t>(vdata);

	std::stringstream buffer; // Use a stringstream to accumulate data

	// Concatenate data into the buffer, only valid rows
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx) || data[idx].GetSize() == 0) {
			state.AddNull();
			continue;
		}
		valid_rows++;
		string_t current_string = data[idx];
		buffer.write(current_string.GetData(), current_string.GetSize());
	}
	// logMessage("Number of valid rows: " + std::to_string(valid_rows));
	std::string uncompressed_data = buffer.str();
	if (uncompressed_data.empty()) {
		return; // No valid data to compress
	}

	// Prepare the compressed data container
	std::vector<unsigned char> compressed_data(snappy::MaxCompressedLength(uncompressed_data.size()));
	size_t compressed_size = 0;
	snappy::RawCompress(uncompressed_data.data(), uncompressed_data.size(),
	                    reinterpret_cast<char *>(compressed_data.data()), &compressed_size);

	// Adjust the size of the vector to the actual compressed size
	compressed_data.resize(compressed_size);

	// Prepare the header
	SnappyCompressionHeader header;
	header.compressed_size = compressed_size;
	header.uncompressed_size = uncompressed_data.size();

	// Allocate space for header + compressed data
	std::vector<unsigned char> data_with_header(sizeof(header) + compressed_size);
	memcpy(data_with_header.data(), &header, sizeof(header));
	memcpy(data_with_header.data() + sizeof(header), compressed_data.data(), compressed_size);

	// Update state with the compressed data, including the header
	state.UpdateState(uncompressed_data.data(), data_with_header.data(), data_with_header.size(), valid_rows);

	// logMessage("Compressed data size: " + std::to_string(compressed_size) +
	// ", with header: " + std::to_string(sizeof(header) + compressed_size));
}

void SnappyStorage::FinalizeCompress(CompressionState &state_p) {
	// Cast the general CompressionState to a more specific SnappyCompressionState
	auto &state = state_p.Cast<SnappyCompressionState>();

	// Flush any remaining compressed data to ensure all data is written before finalization.
	state.Flush(true); // Pass `true` to indicate final flush
}

//--------------------------------------------------------------------
//============
// SCAN
//============
//--------------------------------------------------------------------

struct SnappyScanState : public SegmentScanState {
	BufferHandle handle;                    // Handle to manage pinned memory
	std::vector<char> decompression_buffer; // Buffer to hold decompressed data
	uint32_t compressed_size;               // Size of the compressed data in bytes
	uint32_t uncompressed_size;             // Size of the uncompressed data in bytes

	SnappyScanState() : compressed_size(0), uncompressed_size(0) {
	}
};

unique_ptr<SegmentScanState> SnappyStorage::StringInitScan(ColumnSegment &segment) {
	auto state = make_uniq<SnappyScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);

	state->handle = buffer_manager.Pin(segment.block);
	auto *header = reinterpret_cast<SnappyCompressionHeader *>(state->handle.Ptr() + segment.GetBlockOffset());

	state->compressed_size = header->compressed_size;
	state->uncompressed_size = header->uncompressed_size;

	return state;
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//

void SnappyStorage::StringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                                      idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<SnappyScanState>();

	// Check if decompression is needed (only decompress once per segment scan)
	if (scan_state.decompression_buffer.empty()) {
		char *compressed_data_start = reinterpret_cast<char *>(scan_state.handle.Ptr() + segment.GetBlockOffset() +
		                                                       sizeof(SnappyCompressionHeader));

		// Ensure the buffer is large enough to hold the uncompressed data
		scan_state.decompression_buffer.resize(scan_state.uncompressed_size);

		// Perform the decompression directly into the buffer
		size_t actual_uncompressed_size = 0;
		if (!snappy::GetUncompressedLength(compressed_data_start, scan_state.compressed_size,
		                                   &actual_uncompressed_size)) {
			throw std::runtime_error("Failed to get uncompressed length");
		}
		assert(actual_uncompressed_size == scan_state.uncompressed_size); // Optional: verify size consistency

		if (!snappy::RawUncompress(compressed_data_start, scan_state.compressed_size,
		                           scan_state.decompression_buffer.data())) {
			throw std::runtime_error("Decompression failed");
		}
	}

	// Directly use the data from the decompression buffer
	auto result_data = FlatVector::GetData<string_t>(result);
	string_t decompressed_string(scan_state.decompression_buffer.data(), scan_state.uncompressed_size);

	// Fill the result vector with the decompressed string (assuming one large string for simplicity)
	for (idx_t i = 0; i < scan_count; i++) {
		result_data[result_offset + i] = decompressed_string;
	}
}

void SnappyStorage::StringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	// Simply call StringScanPartial with an offset of 0
	StringScanPartial(segment, state, scan_count, result, 0);
}

void SnappyStorage::StringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                   idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// Get the header to know sizes
	auto *header = reinterpret_cast<SnappyCompressionHeader *>(handle.Ptr() + segment.GetBlockOffset());

	// Get the start pointer to the compressed data
	char *compressed_data_start = reinterpret_cast<char *>(header + 1);

	std::vector<char> decompression_buffer(header->uncompressed_size);
	snappy::RawUncompress(compressed_data_start, header->compressed_size, decompression_buffer.data());

	auto result_data = FlatVector::GetData<string_t>(result);
	result_data[result_idx] = string_t(decompression_buffer.data(), header->uncompressed_size);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
CompressionFunction SnappyFun::GetFunction(PhysicalType data_type) {
	D_ASSERT(data_type == PhysicalType::VARCHAR);
	return CompressionFunction(
	    CompressionType::COMPRESSION_SNAPPY, data_type, SnappyStorage::StringInitAnalyze, SnappyStorage::StringAnalyze,
	    SnappyStorage::StringFinalAnalyze, SnappyStorage::InitCompression, SnappyStorage::Compress,
	    SnappyStorage::FinalizeCompress, SnappyStorage::StringInitScan, SnappyStorage::StringScan,
	    SnappyStorage::StringScanPartial, SnappyStorage::StringFetchRow, UncompressedFunctions::EmptySkip);
}

bool SnappyFun::TypeIsSupported(PhysicalType type) {
	return type == PhysicalType::VARCHAR;
}

} // namespace duckdb
