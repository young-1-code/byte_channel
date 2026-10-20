#include "channel.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <csignal>
#include <thread>
#include <vector>

namespace {

struct Message {
    uint64_t sequence;
    char payload[56];
};

constexpr int kBufferSize = 1 << 20;
constexpr int kDefaultSubscriberCount = 4;
constexpr int kDefaultReportInterval = 3;

std::atomic<bool> g_stop{false};

void handle_signal(int)
{
    g_stop.store(true, std::memory_order_release);
}

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

bool run_channel_benchmark(int subscriber_count, int report_interval)
{
    Pulisher publisher;
    const std::string topic = "channel_continuous_benchmark";
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
    std::atomic<uint64_t> produced{0};
    std::atomic<uint64_t> consumed{0};
    std::vector<std::thread> consumers;
    consumers.reserve(subscriber_count);

    for (int index = 0; index < subscriber_count; ++index) {
        consumers.emplace_back([&, index] {
            Message message{};
            uint64_t expected_sequence = 0;
            while (!g_stop.load(std::memory_order_acquire) &&
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
                consumed.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    std::thread reporter([&, report_interval] {
        auto previous_time = std::chrono::steady_clock::now();
        uint64_t previous_produced = 0;
        uint64_t previous_consumed = 0;

        while (!g_stop.load(std::memory_order_acquire) &&
               !failed.load(std::memory_order_acquire)) {
            std::this_thread::sleep_for(
                std::chrono::seconds(report_interval));

            auto now = std::chrono::steady_clock::now();
            double elapsed = std::chrono::duration<double>(
                now - previous_time).count();
            uint64_t current_produced = produced.load(std::memory_order_relaxed);
            uint64_t current_consumed = consumed.load(std::memory_order_relaxed);
            double produced_rate = elapsed > 0.0
                ? (current_produced - previous_produced) / elapsed
                : 0.0;
            double consumed_rate = elapsed > 0.0
                ? (current_consumed - previous_consumed) /
                    elapsed / subscriber_count
                : 0.0;
            double produced_bandwidth =
                produced_rate * sizeof(Message) / (1024.0 * 1024.0);
            double consumed_bandwidth =
                consumed_rate * sizeof(Message) / (1024.0 * 1024.0);

            std::cout << std::fixed << std::setprecision(2)
                      << "interval=" << elapsed << " s, "
                      << "produce=" << produced_rate << " msg/s, "
                      << "produce_bw=" << produced_bandwidth << " MiB/s, "
                      << "consume=" << consumed_rate << " msg/s/subscriber, "
                      << "consume_bw=" << consumed_bandwidth
                      << " MiB/s/subscriber, "
                      << "consume_bw_total="
                      << consumed_bandwidth * subscriber_count << " MiB/s, "
                      << "produced=" << current_produced << ", "
                      << "consumed=" << current_consumed << '\n'
                      << std::flush;

            previous_time = now;
            previous_produced = current_produced;
            previous_consumed = current_consumed;
        }
    });

    uint64_t sequence = 0;
    while (!g_stop.load(std::memory_order_acquire) &&
           !failed.load(std::memory_order_acquire)) {
        Message message{};
        message.sequence = sequence;

        bool published = false;
        while (!g_stop.load(std::memory_order_acquire) &&
               !failed.load(std::memory_order_acquire)) {
            int result = publisher.publish(
                reinterpret_cast<const char *>(&message), sizeof(message));
            if (result == 0) {
                published = true;
                break;
            }
            if (result != -2) {
                failed.store(true, std::memory_order_release);
                break;
            }
            std::this_thread::yield();
        }
        if (!published) break;
        produced.fetch_add(1, std::memory_order_relaxed);
        ++sequence;
    }

    g_stop.store(true, std::memory_order_release);
    reporter.join();
    for (auto &consumer : consumers) consumer.join();

    std::cout << "stopped: produced=" << produced.load()
              << ", consumed=" << consumed.load() << '\n';
    return !failed.load(std::memory_order_acquire);
}

}

int main(int argc, char **argv)
{
    int subscriber_count = argc > 1
        ? parse_positive_int(argv[1], kDefaultSubscriberCount)
        : kDefaultSubscriberCount;
    int report_interval = argc > 2
        ? parse_positive_int(argv[2], kDefaultReportInterval)
        : kDefaultReportInterval;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    bool atomic_ok = run_atomic_benchmark();
    bool channel_ok = run_channel_benchmark(subscriber_count, report_interval);
    return atomic_ok && channel_ok ? 0 : 1;
}
