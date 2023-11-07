// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <chrono>
#include <ctime>
#include <iostream>
#include <list>
#include <memory>

#include "arrow/util/openformat_config.h"
#include "arrow/util/openformat_stats.h"
#include "parquet/api/reader.h"

int main(int argc, char** argv) {
  if (argc > 4 || argc < 1) {
    std::cerr << "Usage: parquet-scan [--batch-size=] [--columns=...] <file>"
              << std::endl;
    return -1;
  }

  std::string filename;

  // Read command-line options
  int batch_size = 1024;
  const std::string COLUMNS_PREFIX = "--columns=";
  const std::string BATCH_SIZE_PREFIX = "--batch-size=";
  std::vector<int> columns;
  int num_columns = 0;

  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ((param = std::strstr(argv[i], COLUMNS_PREFIX.c_str()))) {
      value = std::strtok(param + COLUMNS_PREFIX.length(), ",");
      while (value) {
        columns.push_back(std::atoi(value));
        value = std::strtok(nullptr, ",");
        num_columns++;
      }
    } else if ((param = std::strstr(argv[i], BATCH_SIZE_PREFIX.c_str()))) {
      value = std::strtok(param + BATCH_SIZE_PREFIX.length(), " ");
      if (value) {
        batch_size = std::atoi(value);
      }
    } else {
      filename = argv[i];
    }
  }

  try {
    double total_time;
    // auto reader_properties = parquet::default_reader_properties();
    // reader_properties.enable_buffered_stream();
    // std::clock_t start_time = std::clock();
    auto begin = std::chrono::steady_clock::now();
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename);
    // parquet::ParquetFileReader::OpenFile(filename, false, reader_properties);

    int64_t total_rows =
        parquet::ScanFileContentsColumnVectorBatch(columns, batch_size, reader.get());

    total_time = (static_cast<std::chrono::duration<double>>(
                      std::chrono::steady_clock::now() - begin))
                     .count();
    // total_time = static_cast<double>(std::clock() - start_time) /
    //              static_cast<double>(CLOCKS_PER_SEC);
    std::cout << total_rows << " rows scanned in " << total_time << " seconds."
              << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }
  std::cout << "parse meta: " << arrow::openformat::time_parse_metadata << " ns\n";
  std::cout << "total levels time: " << arrow::openformat::time_levels << "ns"
            << std::endl;
  std::cout << "total def levels time: " << arrow::openformat::time_def_levels << "ns"
            << std::endl;
  std::cout << "n_rle: " << arrow::openformat::n_rle << std::endl;
  std::cout << "n_bitpack: " << arrow::openformat::n_bitpack << std::endl;
  std::cout << "n_rle_seq: " << arrow::openformat::n_rle_seq << std::endl;
  std::cout << "n_bitpack_seq: " << arrow::openformat::n_bitpack_seq << std::endl;
#if OF_STATS_ENABLE
  std::cout << "total read time: " << arrow::openformat::time_read << "ns" << std::endl;
  std::cout << "total read cnt: " << arrow::openformat::num_read << std::endl;
#endif
  return 0;
}
