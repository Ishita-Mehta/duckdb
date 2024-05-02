add_subdirectory(chimp)
add_subdirectory(alp)

include_directories(
  include
  ../../third_party/snappy
)

add_library_unity(
  duckdb_storage_compression
  OBJECT
  numeric_constant.cpp
  fixed_size_uncompressed.cpp
  rle.cpp
  dictionary_compression.cpp
  delta_encoding.cpp
  string_uncompressed.cpp
  uncompressed.cpp
  validity_uncompressed.cpp
  bitpacking.cpp
  bitpacking_hugeint.cpp
  patas.cpp
  alprd.cpp
  snappy.cpp
  fsst.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_compression>
    PARENT_SCOPE)
