#include "spsc_channel.hpp"

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

bool run_channel_benchmark(int report_interval)
{
    SpscPublisher publisher;
    const std::string topic = "spsc_channel_benchmark";
    if (publisher.create(kBufferSize, topic, MemoryType::Thread) != 0) {
        std::cerr << "failed to create publisher\n";
        return false;
    }

    SpscSubscriber subscriber;
    if (subscriber.subscribe(publisher.shared_memory()) != 0) {
        std::cerr << "failed to subscribe\n";
        return false;
    }

    auto report_time = std::chrono::steady_clock::now() +
        std::chrono::seconds(report_interval);
    auto interval_start = std::chrono::steady_clock::now();
    uint64_t produced = 0;
    uint64_t consumed = 0;
    uint64_t sequence = 0;
    while (!g_stop.load(std::memory_order_acquire)) {
        Message message{};
        message.sequence = sequence;

        if (publisher.publish(
                reinterpret_cast<const char *>(&message), sizeof(message)) != 0) {
            std::cerr << "failed to publish\n";
            return false;
        }
        ++produced;

        Message received{};
        if (subscriber.subcribe(
                reinterpret_cast<char *>(&received), sizeof(received)) != 0 ||
            received.sequence != sequence) {
            std::cerr << "failed to consume\n";
            return false;
        }
        ++consumed;
        ++sequence;

        auto now = std::chrono::steady_clock::now();
        if (now >= report_time) {
            double elapsed = std::chrono::duration<double>(
                now - interval_start).count();
            double messages_per_second = elapsed > 0.0
                ? consumed / elapsed
                : 0.0;
            double mib_per_second =
                messages_per_second * sizeof(Message) / (1024.0 * 1024.0);
            std::cout << std::fixed << std::setprecision(2)
                      << "interval=" << elapsed << " s, "
                      << "spsc=" << messages_per_second << " msg/s, "
                      << "bandwidth=" << mib_per_second << " MiB/s, "
                      << "produced=" << produced << ", "
                      << "consumed=" << consumed << '\n'
                      << std::flush;
            interval_start = now;
            produced = 0;
            consumed = 0;
            report_time = now + std::chrono::seconds(report_interval);
        }
    }

    std::cout << "stopped: produced=" << produced
              << ", consumed=" << consumed << '\n';
    return true;
}

}

int main(int argc, char **argv)
{
    int report_interval = argc > 1
        ? parse_positive_int(argv[1], kDefaultReportInterval)
        : kDefaultReportInterval;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    bool atomic_ok = run_atomic_benchmark();
    bool channel_ok = run_channel_benchmark(report_interval);
    return atomic_ok && channel_ok ? 0 : 1;
}
