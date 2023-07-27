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
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <fstream>
#include <future>
#include <iostream>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <vector>

#include <string_view>
#include "arrow/scalar.h"
#include "arrow/util/openformat_config.h"
#include "arrow/util/openformat_stats.h"
#include "json.hpp"
#include "parquet/api/reader.h"

int single_file(const std::string& filename, const std::vector<uint32_t>& rows) {
  // std::string filename;
  // std::cout << "read: " << filename << std::endl;

  // Read command-line options
  int batch_size = 1024;
  std::vector<int> columns = {1, 2};
  // std::vector<uint32_t> rows;
  // int num_columns = 0;

  try {
    // double total_time;
    double time_inside = 0;
    // auto reader_properties = parquet::default_reader_properties();
    // reader_properties.enable_buffered_stream();
    // std::clock_t start_time = std::clock();
    // auto begin = std::chrono::steady_clock::now();
    std::unique_ptr<parquet::ParquetFileReader> reader =
        parquet::ParquetFileReader::OpenFile(filename);
    // parquet::ParquetFileReader::OpenFile(filename, false, reader_properties);

    parquet::ScanFileContentsBitpos(columns, batch_size, reader.get(), rows, nullptr,
                                    &time_inside, false);

    // total_time = (static_cast<std::chrono::duration<double>>(
    //                   std::chrono::steady_clock::now() - begin))
    //                  .count();
    // total_time = static_cast<double>(std::clock() - start_time) /
    //              static_cast<double>(CLOCKS_PER_SEC);
    // std::cout << total_rows << " rows scanned in " << total_time << " seconds."
    //           << std::endl;
    // std::cout << "Inside time: " << time_inside << " seconds." << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }

#if OF_STATS_ENABLE
  std::cout << "total read time: " << arrow::openformat::time_read << "ns" << std::endl;
  std::cout << "total read cnt: " << arrow::openformat::num_read << std::endl;
#endif
  return 0;
}

class ThreadPool {
 public:
  explicit ThreadPool(size_t numThreads) {
    workers.reserve(numThreads);
    for (size_t i = 0; i < numThreads; ++i) {
      workers.emplace_back([this] {
        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(this->queueMutex);
            this->condition.wait(lock,
                                 [this] { return this->stop || !this->tasks.empty(); });
            if (this->stop && this->tasks.empty()) return;
            task = std::move(this->tasks.front());
            this->tasks.pop();
          }
          task();
        }
      });
    }
  }

  template <class F>
  void enqueue(F&& f) {
    {
      std::unique_lock<std::mutex> lock(queueMutex);
      tasks.emplace(std::forward<F>(f));
    }
    condition.notify_one();
  }

  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queueMutex);
      stop = true;
    }
    condition.notify_all();
    for (std::thread& worker : workers) worker.join();
  }

 private:
  std::vector<std::thread> workers;
  std::queue<std::function<void()>> tasks;
  std::mutex queueMutex;
  std::condition_variable condition;
  bool stop = false;
};

int main(int argc, char** argv) {
  std::ifstream ifs = std::ifstream(argv[1]);
  nlohmann::json ex_jf = nlohmann::json::parse(ifs);
  std::map<std::string, std::vector<uint32_t>> file_to_ids;
  for (auto& item : ex_jf.items()) {
    // convert comma separated ids into rows
    std::stringstream ss(std::string(item.value()));
    std::string id;
    while (std::getline(ss, id, ',')) {
      file_to_ids[item.key()].push_back(std::stoi(id));
    }
  }

  auto* pool = new ThreadPool(8);  // Use 8 threads
  // std::cout << "total num of files: " << file_to_ids.size() << std::endl;
  auto begin = std::chrono::steady_clock::now();
  for (const auto& item : file_to_ids) {
    pool->enqueue([item] {
      // std::ostringstream cmd;
      // cmd << "scan_exec_pq --columns=1,2 --rows=" << item.second << " " << item.first;
      // auto c_str = const_cast<char*>(cmd.str().c_str());
      single_file(item.first, item.second);
      // std::cout << "finished " << item.first << std::endl;
      // std::system(cmd.str().c_str());
    });
  }
  delete pool;
  auto total_time = (static_cast<std::chrono::duration<double>>(
                         std::chrono::steady_clock::now() - begin))
                        .count();
  std::cout << "total time: " << total_time << " seconds." << std::endl;
  // system("cat /proc/$PPID/io");
  return 0;
}