#include <parquet/arrow/reader.h>

#include <fstream>
#include <iostream>
#include <sstream>

// #include <gperftools/profiler.h>
#include "arrow/adapters/orc/adapter.h"
#include "arrow/adapters/orc/options.h"
#include "arrow/array.h"
#include "arrow/array/builder_binary.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/csv/api.h"
#include "arrow/dataset/api.h"
#include "arrow/dataset/discovery.h"
#include "arrow/dataset/file_base.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/file.h"
#include "arrow/util/range.h"
#include "json.hpp"
#include "parquet/arrow/reader.h"
#include "parquet/arrow/writer.h"
#include "parquet/file_reader.h"
#include "parquet/properties.h"
#include "stats.h"

namespace ds = arrow::dataset;
namespace fs = arrow::fs;
namespace cp = arrow::compute;

#define ABORT_ON_FAILURE(expr)                     \
  do {                                             \
    arrow::Status status_ = (expr);                \
    if (!status_.ok()) {                           \
      std::cerr << status_.message() << std::endl; \
      abort();                                     \
    }                                              \
  } while (0);

arrow::Status RunMain(int argc, char** argv) {
  std::string file_path = argv[1];
  // optional argument: batch_size, if input is -b
  int batch_size = 1024 * 1024;
  if (argc > 2) {
    if (std::string(argv[2]) == "-b") {
      batch_size = std::stoi(argv[3]);
    }
  }
  auto file_type = file_path.substr(file_path.find_last_of(".") + 1);
  ARROW_ASSIGN_OR_RAISE(
      auto input, arrow::io::ReadableFile::Open(file_path, arrow::default_memory_pool()));
  std::shared_ptr<arrow::Table> table;
  if (file_type == "orc") {
    std::cout << "not implemented" << std::endl;
  } else if (file_type == "parquet") {
    arrow::MemoryPool* pool = arrow::default_memory_pool();

    // Configure general Parquet reader settings
    auto reader_properties = parquet::ReaderProperties(pool);
    // reader_properties.set_buffer_size(4096 * 4);
    // reader_properties.enable_buffered_stream();

    // Configure Arrow-specific Parquet reader settings
    auto arrow_reader_props = parquet::ArrowReaderProperties();
    arrow_reader_props.set_batch_size(batch_size);  // default 64 * 1024

    parquet::arrow::FileReaderBuilder reader_builder;
    ARROW_RETURN_NOT_OK(reader_builder.Open(input, reader_properties));
    reader_builder.memory_pool(pool);
    reader_builder.properties(arrow_reader_props);

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;
    ARROW_RETURN_NOT_OK(reader_builder.Build(&arrow_reader));

    std::shared_ptr<::arrow::RecordBatchReader> rb_reader;
    ARROW_RETURN_NOT_OK(arrow_reader->GetRecordBatchReader(
        arrow::internal::Iota(arrow_reader->num_row_groups()), &rb_reader));

    auto begin = stats::Time::now();
    for (arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch : *rb_reader) {
      // Operate on each batch...
    }

    // ARROW_RETURN_NOT_OK(pq_reader->ReadTable(&table));
    stats::cout_sec(begin, "read pq");
    std::cout << "num row groups:" << arrow_reader->num_row_groups() << std::endl;
  } else {
    std::cout << "Unsupported file type: " << file_type << std::endl;
    return arrow::Status::Invalid("Unsupported file type: ", file_type);
  }
  // std::cout << "table schema: " << table->schema()->ToString() << std::endl;
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  arrow::Status status = RunMain(argc, argv);
  if (!status.ok()) {
    std::cerr << status << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}