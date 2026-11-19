#include "spsc_channel.hpp"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <time.h>
#include <thread>
#include <vector>

namespace {
constexpr int kBufferSize = 8 * 1024 * 1024;
constexpr int kSampleMask = 1023;

struct Message {
  uint64_t sequence;
  uint64_t send_ns;
  char padding[48];
};

uint64_t now_ns() {
  timespec ts{};
  clock_gettime(CLOCK_MONOTONIC_RAW, &ts);
  return static_cast<uint64_t>(ts.tv_sec) * 1000000000ULL + ts.tv_nsec;
}

double percentile(std::vector<uint64_t> values, double p) {
  if (values.empty()) return 0.0;
  const size_t index = static_cast<size_t>(p * (values.size() - 1));
  std::nth_element(values.begin(), values.begin() + index, values.end());
  return static_cast<double>(values[index]);
}
}

int main(int argc, char **argv) {
  const int seconds = argc > 1 ? std::max(1, std::atoi(argv[1])) : 10;
  SpscPublisher publisher;
  if (publisher.create(kBufferSize, "latency_benchmark", MemoryType::Thread) != 0)
    return 1;
  SpscSubscriber subscriber;
  if (subscriber.subscribe(publisher.shared_memory()) != 0) return 1;

  std::atomic<bool> stop{false};
  std::atomic<bool> producer_done{false};
  std::atomic<bool> failed{false};
  std::atomic<uint64_t> produced{0};
  std::atomic<uint64_t> consumed{0};
  std::atomic<uint64_t> retries{0};
  std::vector<uint64_t> samples;
  samples.reserve(200000);

  std::thread producer([&] {
    Message message{};
    uint64_t sequence = 0;
    while (!stop.load(std::memory_order_relaxed)) {
      message.sequence = sequence;
      message.send_ns = now_ns();
      const int result = publisher.try_push(reinterpret_cast<const char *>(&message),
                                            sizeof(message));
      if (result == 0) {
        ++produced;
        ++sequence;
      } else if (result == channel_status::kWouldBlock) {
        ++retries;
        std::this_thread::yield();
      } else {
        failed.store(true, std::memory_order_release);
        break;
      }
    }
    producer_done.store(true, std::memory_order_release);
  });

  std::thread consumer([&] {
    Message message{};
    uint64_t expected = 0;
    while (!failed.load(std::memory_order_relaxed) &&
           (!stop.load(std::memory_order_relaxed) ||
            !producer_done.load(std::memory_order_acquire) ||
            consumed.load(std::memory_order_relaxed) <
                produced.load(std::memory_order_relaxed))) {
      const int result = subscriber.try_pop(reinterpret_cast<char *>(&message),
                                            sizeof(message));
      if (result == 0) {
        if (message.sequence != expected) {
          failed.store(true, std::memory_order_release);
          break;
        }
        if ((expected & kSampleMask) == 0) samples.push_back(now_ns() - message.send_ns);
        ++expected;
        ++consumed;
      } else if (result == channel_status::kWouldBlock) {
        std::this_thread::yield();
      } else {
        failed.store(true, std::memory_order_release);
        break;
      }
    }
  });

  std::this_thread::sleep_for(std::chrono::seconds(seconds));
  stop.store(true, std::memory_order_release);
  producer.join();
  consumer.join();
  std::cout << "duration=" << seconds << "s buffer=" << kBufferSize
            << " produced=" << produced << " consumed=" << consumed
            << " retries=" << retries << " samples=" << samples.size()
            << " status=" << (failed ? "FAIL" : "OK") << '\n';
  std::cout << "latency_ns p50=" << percentile(samples, .50)
            << " p99=" << percentile(samples, .99)
            << " p999=" << percentile(samples, .999) << '\n';
  return failed || produced != consumed ? 1 : 0;
}
