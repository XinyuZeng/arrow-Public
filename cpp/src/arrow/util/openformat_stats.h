#pragma once

#include <atomic>
#include <chrono>

namespace arrow {
namespace openformat {
extern uint64_t num_read;
extern int64_t time_read;
// Parquet detailed profiling
extern int64_t time_levels;
extern int64_t time_def_levels;
extern int64_t time_read_values;
extern int64_t time_reserve_values;
extern int64_t time_read_records;
extern int64_t time_read_record_data;
extern int64_t time_delimit_records;
extern int64_t time_transfer_column_data;
extern int64_t time_load_batch;  // load batch covers above two
extern int64_t time_build_array;
// ORC detailed profiling
extern int64_t time_orc_decode;
extern int64_t time_orc_append;
extern int64_t time_orc_append_numeric;

#define OF_INIT_TIMER(begin) auto begin = std::chrono::steady_clock::now();
#define OF_UPDATE_TIMER(begin) begin = std::chrono::steady_clock::now();
#define OF_ADD_TIME(begin, dst)                                \
  dst += std::chrono::duration_cast<std::chrono::nanoseconds>( \
             std::chrono::steady_clock::now() - begin)         \
             .count();
// Projection breakdown
extern int64_t time_parse_metadata;
extern uint32_t n_rle;
extern uint32_t n_bitpack;
extern uint32_t n_rle_seq;
extern uint32_t n_bitpack_seq;
}  // namespace openformat
}  // namespace arrow
