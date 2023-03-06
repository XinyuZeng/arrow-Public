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
#include "json.hpp"
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
  bool use_threads = false;
  ARROW_ASSIGN_OR_RAISE(
      auto input, arrow::io::ReadableFile::Open(file_path, arrow::default_memory_pool()));
  std::shared_ptr<arrow::Table> table;
  if (file_type == "orc") {
    ARROW_ASSIGN_OR_RAISE(auto orc_reader, arrow::adapters::orc::ORCFileReader::Open(
                                               input, arrow::default_memory_pool()))
    // Read table from ORC file
    auto begin = stats::Time::now();
    ARROW_ASSIGN_OR_RAISE(table, orc_reader->Read());
    stats::cout_sec(begin, "read orc");
    std::cout << "num stripes: " << orc_reader->NumberOfStripes() << std::endl;
  } else if (file_type == "parquet") {
    // Instantiate TableReader from input stream and options
    std::unique_ptr<parquet::arrow::FileReader> pq_reader;
    // ARROW_RETURN_NOT_OK(
    //     parquet::arrow::OpenFile(input, arrow::default_memory_pool(), &pq_reader));
    auto pq_file_reader = parquet::ParquetFileReader::Open(input);
    parquet::ArrowReaderProperties arrow_properties;
    arrow_properties.set_batch_size(batch_size);
    arrow_properties.set_use_threads(use_threads);
    // arrow_properties.set_read_dictionary(0, true);
    parquet::arrow::FileReader::Make(arrow::default_memory_pool(),
                                     std::move(pq_file_reader), arrow_properties,
                                     &pq_reader);
    if (batch_size != 0) {
      pq_reader->set_batch_size(batch_size);
    }
    pq_reader->set_use_threads(use_threads);
    auto begin = stats::Time::now();
    ARROW_RETURN_NOT_OK(pq_reader->ReadTable(&table));
    stats::cout_sec(begin, "read pq");
    std::cout << "num row groups:" << pq_reader->num_row_groups() << std::endl;
  } else {
    std::cout << "Unsupported file type: " << file_type << std::endl;
    return arrow::Status::Invalid("Unsupported file type: ", file_type);
  }
  std::cout << "table schema: " << table->schema()->ToString() << std::endl;
  return arrow::Status::OK();
}

int main(int argc, char** argv) {
  // int filter_on = atoi(argv[1]);
  // int proj_on = atoi(argv[2]);
  // std::string base_path = argv[3];
  // std::string root_path;
  // auto fs = std::make_shared<fs::LocalFileSystem>(fs::LocalFileSystem());
  // auto format = std::make_shared<ds::ParquetFileFormat>();
  // auto begin = stats::Time::now();
  // // ProfilerStart("profile.out");
  // auto table = FilterAndSelectDataset(fs, format, base_path, filter_on, proj_on);
  // table->num_rows();
  // // ProfilerStop();
  // stats::cout_sec(begin, "read pq");

  // std::shared_ptr<arrow::io::FileOutputStream> outfile;
  // std::string file_name = "lineitem_1K";
  // PARQUET_ASSIGN_OR_THROW(
  //     outfile, arrow::io::FileOutputStream::Open("/root/arrow-private/cpp/" + file_name
  //     +
  //                                                ".parquet"));
  // // FIXME: hard code for now
  // uint32_t row_group_size = 1 * 1024 * 1024;
  // uint32_t dictionary_pages_size = 1 * 1024 * 1024;  // 1MB
  // arrow::Compression::type codec = arrow::Compression::SNAPPY;
  // std::shared_ptr<parquet::WriterProperties> properties =
  //     parquet::WriterProperties::Builder()
  //         .dictionary_pagesize_limit(dictionary_pages_size)
  //         ->compression(codec)
  //         ->enable_dictionary()
  //         ->build();

  // begin = stats::Time::now();
  // PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(),
  //                                                 outfile, row_group_size,
  //                                                 properties));
  arrow::Status status = RunMain(argc, argv);
  if (!status.ok()) {
    std::cerr << status << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_SUCCESS;
}