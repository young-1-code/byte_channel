#include "channel.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <thread>
#include <vector>

namespace {

struct Message {
    uint64_t sequence;
    char payload[56];
};

constexpr int kBufferSize = 1 << 20;
constexpr int kDefaultMessageCount = 1'000'000;
constexpr auto kTimeout = std::chrono::seconds(60);

int parse_positive_int(const char *value, int fallback)
{
    char *end = nullptr;
    long parsed = std::strtol(value, &end, 10);
    if (end == value || *end != '\0' || parsed <= 0 ||
        parsed > std::numeric_limits<int>::max()) {
        return fallback;
    }
    return static_cast<int>(parsed);
}

bool run_atomic_benchmark()
{
    uint64_t counter = 0;
    constexpr int thread_count = 8;
    constexpr int increments_per_thread = 1'000'000;
    std::vector<std::thread> workers;
    workers.reserve(thread_count);

    for (int i = 0; i < thread_count; ++i) {
        workers.emplace_back([&counter] {
            for (int j = 0; j < increments_per_thread; ++j) {
                channel_atomic::fetch_add(&counter, uint64_t{1});
            }
        });
    }
    for (auto &worker : workers) worker.join();

    uint64_t expected = static_cast<uint64_t>(thread_count) * increments_per_thread;
    std::cout << "atomic counter: " << counter << " / " << expected << '\n';
    return counter == expected;
}

bool run_channel_benchmark(int message_count, int subscriber_count)
{
    Pulisher publisher;
    const std::string topic = "channel_benchmark";
    if (publisher.create(kBufferSize, topic, MemoryType::Thread) != 0) {
        std::cerr << "failed to create publisher\n";
        return false;
    }

    std::vector<Subscriber> subscribers(subscriber_count);
    for (auto &subscriber : subscribers) {
        if (subscriber.subscribe(publisher.shared_memory()) != 0) {
            std::cerr << "failed to subscribe\n";
            return false;
        }
    }

    std::atomic<bool> failed{false};
    std::atomic<int> finished{0};
    std::vector<std::thread> consumers;
    consumers.reserve(subscriber_count);

    for (int index = 0; index < subscriber_count; ++index) {
        consumers.emplace_back([&, index] {
            Message message{};
            uint64_t expected_sequence = 0;
            while (expected_sequence < static_cast<uint64_t>(message_count) &&
                   !failed.load(std::memory_order_acquire)) {
                int result = subscribers[index].subcribe(
                    reinterpret_cast<char *>(&message), sizeof(message));
                if (result == -2) {
                    std::this_thread::yield();
                    continue;
                }
                if (result != 0 || message.sequence != expected_sequence) {
                    failed.store(true, std::memory_order_release);
                    return;
                }
                ++expected_sequence;
            }
            finished.fetch_add(1, std::memory_order_acq_rel);
        });
    }

    auto start = std::chrono::steady_clock::now();
    for (uint64_t sequence = 0;
         sequence < static_cast<uint64_t>(message_count) &&
         !failed.load(std::memory_order_acquire);
         ++sequence) {
        Message message{};
        message.sequence = sequence;

        for (;;) {
            int result = publisher.publish(
                reinterpret_cast<const char *>(&message), sizeof(message));
            if (result == 0) break;
            if (result != -2) {
                failed.store(true, std::memory_order_release);
                break;
            }
            if (std::chrono::steady_clock::now() - start > kTimeout) {
                std::cerr << "publisher blocked for more than 60 seconds\n";
                failed.store(true, std::memory_order_release);
                break;
            }
            std::this_thread::yield();
        }
    }

    for (auto &consumer : consumers) consumer.join();
    auto elapsed = std::chrono::duration<double>(
        std::chrono::steady_clock::now() - start).count();

    bool success = !failed.load(std::memory_order_acquire) &&
                   finished.load(std::memory_order_acquire) == subscriber_count;
    double messages_per_second = elapsed > 0.0 ? message_count / elapsed : 0.0;
    double mib_per_second = messages_per_second * sizeof(Message) / (1024.0 * 1024.0);

    std::cout << std::fixed << std::setprecision(2)
              << "channel: " << message_count << " messages, "
              << subscriber_count << " subscribers, "
              << elapsed << " s, "
              << messages_per_second << " msg/s, "
              << mib_per_second << " MiB/s\n";
    return success;
}

}

int main(int argc, char **argv)
{
    int message_count = argc > 1
        ? parse_positive_int(argv[1], kDefaultMessageCount)
        : kDefaultMessageCount;
    int subscriber_count = argc > 2
        ? parse_positive_int(argv[2], 4)
        : 4;

    bool atomic_ok = run_atomic_benchmark();
    bool channel_ok = run_channel_benchmark(message_count, subscriber_count);
    return atomic_ok && channel_ok ? 0 : 1;
}
