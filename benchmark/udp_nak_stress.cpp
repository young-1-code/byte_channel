#include "core/udp/udp_pub.h"
#include "core/udp/udp_sub.h"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

using Clock = std::chrono::steady_clock;

uint64_t now_ns() {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}

struct Options {
  int seconds = 10;
  int payload = 1024;
  int rate = 0;
  int port_base = 26100;
  bool crc = true;
};

bool parse_options(int argc, char** argv, Options& options) {
  for (int index = 1; index < argc; ++index) {
    const std::string argument = argv[index];
    auto value = [&](const char* name, int& target) {
      if (argument != name || index + 1 >= argc) return false;
      target = std::atoi(argv[++index]);
      return target > 0;
    };
    if (value("--seconds", options.seconds) || value("--payload", options.payload) ||
        value("--rate", options.rate) ||
        value("--port-base", options.port_base)) {
      continue;
    }
    if (argument == "--no-crc") {
      options.crc = false;
      continue;
    }
    if (argument == "--help") {
      std::cout << "usage: udp_nak_stress [--seconds N] [--payload N] [--rate records/s] [--port-base N] [--no-crc]\n";
      return false;
    }
    std::cerr << "unknown or invalid option: " << argument << '\n';
    return false;
  }
  return options.seconds > 0 && options.payload >= 32 && options.payload <= 8 * 1024 * 1024 &&
         options.rate >= 0 &&
         options.port_base <= 65532;
}

}  // namespace

int main(int argc, char** argv) {
  Options options;
  if (!parse_options(argc, argv, options)) return 2;

  UdpPubConfig pub_config;
  pub_config.topic = "udp-stress";
  pub_config.data_address = "127.0.0.1";
  pub_config.data_port = static_cast<uint16_t>(options.port_base);
  pub_config.control_port = static_cast<uint16_t>(options.port_base + 1);
  pub_config.data_mode = UdpDataMode::kUnicast;
  pub_config.enable_crc32 = options.crc;

  UdpSubConfig sub_config;
  sub_config.topic = pub_config.topic;
  sub_config.data_address = "127.0.0.1";
  sub_config.pub_address = "127.0.0.1";
  sub_config.data_port = static_cast<uint16_t>(options.port_base + 2);
  sub_config.control_port = pub_config.control_port;
  sub_config.sub_id = 1;
  sub_config.data_mode = UdpDataMode::kUnicast;
  sub_config.enable_crc32 = options.crc;

  UdpPublisher publisher;
  UdpSubscriber subscriber;
  if (publisher.create(pub_config) != channel_status::kOk ||
      subscriber.subscribe(sub_config) != channel_status::kOk) {
    std::cerr << "failed to start UDP stress endpoints\n";
    return 1;
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(250));
  const uint64_t record_size = static_cast<uint64_t>(options.payload);
  std::vector<char> record(options.payload, 0);
  std::vector<char> received(options.payload, 0);
  std::atomic<bool> stop{false};
  std::atomic<uint64_t> produced{0};
  std::atomic<uint64_t> pushed_bytes{0};
  std::atomic<uint64_t> blocked{0};
  uint64_t consumed = 0;
  uint64_t bad_records = 0;
  uint64_t latency_sum_ns = 0;
  uint64_t latency_max_ns = 0;

  const auto start = Clock::now();
  const auto deadline = start + std::chrono::seconds(options.seconds);
  auto consume_one = [&] {
    if (subscriber.try_pop(received.data(), options.payload) != channel_status::kOk) return false;
    uint64_t sequence = 0;
    uint64_t timestamp = 0;
    std::memcpy(&sequence, received.data(), sizeof(sequence));
    std::memcpy(&timestamp, received.data() + sizeof(sequence), sizeof(timestamp));
    if (sequence != consumed) ++bad_records;
    ++consumed;
    const uint64_t latency = now_ns() - timestamp;
    latency_sum_ns += latency;
    latency_max_ns = std::max(latency_max_ns, latency);
    return true;
  };
  std::thread producer([&] {
    uint64_t sequence = 0;
    auto next_send = Clock::now();
    const auto interval = options.rate > 0
                              ? std::chrono::duration<double>(1.0 / options.rate)
                              : std::chrono::duration<double>(0);
    while (!stop.load(std::memory_order_acquire)) {
      if (options.rate > 0) {
        std::this_thread::sleep_until(next_send);
        next_send += std::chrono::duration_cast<Clock::duration>(interval);
      }
      std::memset(record.data(), 0, record.size());
      std::memcpy(record.data(), &sequence, sizeof(sequence));
      const uint64_t timestamp = now_ns();
      std::memcpy(record.data() + sizeof(sequence), &timestamp, sizeof(timestamp));
      const int result = publisher.try_push(record.data(), options.payload);
      if (result == channel_status::kOk) {
        ++sequence;
        produced.fetch_add(1, std::memory_order_relaxed);
        pushed_bytes.fetch_add(record_size, std::memory_order_relaxed);
      } else if (result == channel_status::kWouldBlock) {
        blocked.fetch_add(1, std::memory_order_relaxed);
        std::this_thread::yield();
      } else {
        break;
      }
    }
  });

  while (Clock::now() < deadline) {
    if (!consume_one()) std::this_thread::yield();
  }

  const uint64_t consumed_during_run = consumed;
  stop.store(true, std::memory_order_release);
  producer.join();
  const uint64_t produced_after_stop = produced.load(std::memory_order_relaxed);
  const auto drain_deadline = Clock::now() + std::chrono::seconds(5);
  while (consumed < produced_after_stop && Clock::now() < drain_deadline) {
    if (!consume_one()) std::this_thread::yield();
  }
  const bool drained = consumed == produced_after_stop;
  subscriber.unsubscribe();
  publisher.close();

  const double run_seconds = static_cast<double>(options.seconds);
  const double total_seconds = std::chrono::duration<double>(Clock::now() - start).count();
  const double drain_seconds = total_seconds - run_seconds;
  const uint64_t produced_count = produced.load(std::memory_order_relaxed);
  const uint64_t bytes = pushed_bytes.load(std::memory_order_relaxed);
  const double mib_per_second = static_cast<double>(bytes) / (1024.0 * 1024.0) / run_seconds;
  const double records_per_second = static_cast<double>(consumed_during_run) / run_seconds;
  const double average_latency_us = consumed == 0 ? 0.0 : static_cast<double>(latency_sum_ns) / consumed / 1000.0;

  std::cout << std::fixed << std::setprecision(2)
            << "udp_nak_stress run_seconds=" << run_seconds << " drain_seconds=" << drain_seconds
            << " payload=" << options.payload << " rate=" << (options.rate == 0 ? "unlimited" : std::to_string(options.rate))
            << " crc=" << (options.crc ? "on" : "off") << '\n'
            << "produced_records=" << produced_count << " consumed_records=" << consumed
            << " consumed_during_run=" << consumed_during_run
            << " bad_records=" << bad_records << " blocked_pushes=" << blocked.load()
            << " drained=" << (drained ? "yes" : "no") << '\n'
            << "throughput_mib_s=" << mib_per_second << " records_s=" << records_per_second
            << " avg_latency_us=" << average_latency_us
            << " max_latency_us=" << static_cast<double>(latency_max_ns) / 1000.0 << '\n';
  return bad_records == 0 && consumed > 0 ? 0 : 1;
}
