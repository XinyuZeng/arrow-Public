#include "arrow/util/openformat_stats.h"
namespace arrow {
namespace openformat {
uint64_t num_read = 0;
int64_t time_read = 0;
int64_t time_levels = 0;
int64_t time_def_levels = 0;
int64_t time_reserve_values = 0;
int64_t time_read_values = 0;
int64_t time_delimit_records = 0;
int64_t time_read_records = 0;
int64_t time_read_record_data = 0;
int64_t time_transfer_column_data = 0;
int64_t time_load_batch = 0;
int64_t time_build_array = 0;
// ORC
int64_t time_orc_decode = 0;
int64_t time_orc_append = 0;
int64_t time_orc_append_numeric = 0;
// Projection breakdown
int64_t time_parse_metadata = 0;

uint32_t n_rle = 0;
uint32_t n_bitpack = 0;
uint32_t n_rle_seq = 0;
uint32_t n_bitpack_seq = 0;
}  // namespace openformat
}  // namespace arrow