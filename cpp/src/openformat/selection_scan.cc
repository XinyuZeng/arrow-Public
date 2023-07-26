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

#include "arrow/scalar.h"
#include "arrow/util/openformat_config.h"
#include "arrow/util/openformat_stats.h"
#include "parquet/api/reader.h"

int main(int argc, char** argv) {
  if (argc > 5 || argc < 2) {
    std::cerr << "Usage: selection-scan [--batch-size=] [--columns=...] "
                 "[--rows=...] <file> "
              << std::endl;
    return -1;
  }

  std::string filename;

  // Read command-line options
  int batch_size = 1024;
  int filter_idx = 0;
  MyFilterType filter_type = MyFilterType::POINT;  // 0 for point, 1 for range
  int type = 0;  // 0 for int64, 1 for double, 2 for string
  std::shared_ptr<arrow::Scalar> v1;
  std::shared_ptr<arrow::Scalar> v2;
  const std::string COLUMNS_PREFIX = "--columns=";
  const std::string ROWS_PREFIX = "--rows=";
  const std::string BATCH_SIZE_PREFIX = "--batch-size=";
  const std::string IDX_PREFIX = "--filter_idx=";
  const std::string F_TYPE_PREFIX = "--filter_type=";
  const std::string TYPE_PREFIX = "--type=";
  const std::string V1_PREFIX = "--v1=";
  const std::string V2_PREFIX = "--v2=";
  std::vector<int> columns;
  std::vector<uint32_t> rows;
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
    } else if ((param = std::strstr(argv[i], ROWS_PREFIX.c_str()))) {
      value = std::strtok(param + ROWS_PREFIX.length(), ",");
      while (value) {
        rows.push_back(std::atoi(value));
        value = std::strtok(nullptr, ",");
      }
    } else if ((param = std::strstr(argv[i], BATCH_SIZE_PREFIX.c_str()))) {
      value = std::strtok(param + BATCH_SIZE_PREFIX.length(), " ");
      if (value) {
        batch_size = std::atoi(value);
      }
    } else if ((param = std::strstr(argv[i], IDX_PREFIX.c_str()))) {
      value = std::strtok(param + IDX_PREFIX.length(), " ");
      if (value) {
        filter_idx = std::atoi(value);
      }
    } else if ((param = std::strstr(argv[i], F_TYPE_PREFIX.c_str()))) {
      value = std::strtok(param + F_TYPE_PREFIX.length(), " ");
      if (value) {
        filter_type = std::atoi(value) == 0 ? MyFilterType::POINT : MyFilterType::RANGE;
      }
    } else if ((param = std::strstr(argv[i], TYPE_PREFIX.c_str()))) {
      value = std::strtok(param + TYPE_PREFIX.length(), " ");
      if (value) {
        type = std::atoi(value);
      }
    } else if ((param = std::strstr(argv[i], V1_PREFIX.c_str()))) {
      value = std::strtok(param + V1_PREFIX.length(), " ");
      if (value) {
        if (type == 0) {
          v1 = std::make_shared<arrow::Int64Scalar>(std::atoi(value));
        } else if (type == 1) {
          v1 = std::make_shared<arrow::DoubleScalar>(std::atof(value));
        } else {
          v1 = std::make_shared<arrow::StringScalar>(value);
        }
      }
    } else if ((param = std::strstr(argv[i], V2_PREFIX.c_str()))) {
      value = std::strtok(param + V2_PREFIX.length(), " ");
      if (value) {
        if (type == 0) {
          v2 = std::make_shared<arrow::Int64Scalar>(std::atoi(value));
        } else if (type == 1) {
          v2 = std::make_shared<arrow::DoubleScalar>(std::atof(value));
        } else {
          v2 = std::make_shared<arrow::StringScalar>(value);
        }
      }
    } else {
      filename = argv[i];
    }
  }

  MyFilter filter{filter_idx, filter_type, v1, v2};

  try {
    double total_time;
    double time_inside = 0;
    // auto reader_properties = parquet::default_reader_properties();
    // reader_properties.enable_buffered_stream();
    // std::clock_t start_time = std::clock();
    auto begin = std::chrono::steady_clock::now();
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename);
    // parquet::ParquetFileReader::OpenFile(filename, false, reader_properties);

    int64_t total_rows = parquet::ScanFileContentsBitpos(
        columns, batch_size, reader.get(), rows, nullptr, &time_inside);

    total_time = (static_cast<std::chrono::duration<double>>(
                      std::chrono::steady_clock::now() - begin))
                     .count();
    // total_time = static_cast<double>(std::clock() - start_time) /
    //              static_cast<double>(CLOCKS_PER_SEC);
    std::cout << total_rows << " rows scanned in " << total_time << " seconds."
              << std::endl;
    std::cout << "Inside time: " << time_inside << " seconds." << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }

#if OF_STATS_ENABLE
  std::cout << "total read time: " << arrow::openformat::time_read << "ns" << std::endl;
  std::cout << "total read cnt: " << arrow::openformat::num_read << std::endl;
#endif
  system("cat /proc/$PPID/io");
  return 0;
}
