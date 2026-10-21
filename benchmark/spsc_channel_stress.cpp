#include "spsc_channel.hpp"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <iomanip>
#include <iostream>
#include <limits>
#include <thread>

namespace {

struct Message {
    uint64_t sequence;
    char payload[56];
};

constexpr int kBufferSize = 1 << 20;
constexpr int kDefaultDuration = 10;
constexpr int kDefaultReportInterval = 1;

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

void fill_message(Message &message, uint64_t sequence)
{
    message.sequence = sequence;
    for (size_t index = 0; index < sizeof(message.payload); ++index) {
        message.payload[index] = static_cast<char>((sequence + index) & 0xff);
    }
}

bool valid_message(const Message &message, uint64_t expected_sequence)
{
    if (message.sequence != expected_sequence) return false;
    for (size_t index = 0; index < sizeof(message.payload); ++index) {
        if (message.payload[index] !=
            static_cast<char>((expected_sequence + index) & 0xff)) {
            return false;
        }
    }
    return true;
}

}

int main(int argc, char **argv)
{
    int duration = argc > 1
        ? parse_positive_int(argv[1], kDefaultDuration)
        : kDefaultDuration;
    int report_interval = argc > 2
        ? parse_positive_int(argv[2], kDefaultReportInterval)
        : kDefaultReportInterval;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    SpscPublisher publisher;
    if (publisher.create(kBufferSize, "spsc_channel_stress", MemoryType::Thread) != 0) {
        std::cerr << "failed to create spsc publisher\n";
        return 1;
    }

    SpscSubscriber subscriber;
    if (subscriber.subscribe(publisher.shared_memory()) != 0) {
        std::cerr << "failed to create spsc subscriber\n";
        return 1;
    }

    std::atomic<bool> failed{false};
    std::atomic<uint64_t> produced{0};
    std::atomic<uint64_t> consumed{0};
    std::atomic<uint64_t> publish_retries{0};
    std::atomic<uint64_t> consume_retries{0};
    std::atomic<bool> producer_done{false};

    std::thread producer([&] {
        Message message{};
        uint64_t sequence = 0;
        while (!g_stop.load(std::memory_order_acquire) &&
               !failed.load(std::memory_order_acquire)) {
            fill_message(message, sequence);
            int result = publisher.try_push(
                reinterpret_cast<const char *>(&message), sizeof(message));
            if (result == 0) {
                ++produced;
                ++sequence;
            } else if (result == -2) {
                ++publish_retries;
                std::this_thread::yield();
            } else {
                failed.store(true, std::memory_order_release);
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        Message message{};
        uint64_t expected_sequence = 0;
        while (!failed.load(std::memory_order_acquire) &&
               (!g_stop.load(std::memory_order_acquire) ||
                !producer_done.load(std::memory_order_acquire) ||
                consumed.load(std::memory_order_relaxed) <
                    produced.load(std::memory_order_relaxed))) {
            int result = subscriber.try_pop(
                reinterpret_cast<char *>(&message), sizeof(message));
            if (result == 0) {
                if (!valid_message(message, expected_sequence)) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
                ++consumed;
                ++expected_sequence;
            } else if (result == -2) {
                ++consume_retries;
                std::this_thread::yield();
            } else {
                failed.store(true, std::memory_order_release);
            }
        }
    });

    auto start = std::chrono::steady_clock::now();
    auto report_time = start + std::chrono::seconds(report_interval);
    auto deadline = start + std::chrono::seconds(duration);
    uint64_t previous_consumed = 0;
    auto previous_time = start;

    while (!g_stop.load(std::memory_order_acquire) &&
           !failed.load(std::memory_order_acquire) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        auto now = std::chrono::steady_clock::now();
        if (now < report_time) continue;

        double elapsed = std::chrono::duration<double>(now - previous_time).count();
        uint64_t current_produced = produced.load(std::memory_order_relaxed);
        uint64_t current_consumed = consumed.load(std::memory_order_relaxed);
        double messages_per_second = elapsed > 0.0
            ? (current_consumed - previous_consumed) / elapsed
            : 0.0;
        double mib_per_second =
            messages_per_second * sizeof(Message) / (1024.0 * 1024.0);

        std::cout << std::fixed << std::setprecision(2)
                  << "interval=" << elapsed << " s, "
                  << "spsc=" << messages_per_second << " msg/s, "
                  << "bandwidth=" << mib_per_second << " MiB/s, "
                  << "produced=" << current_produced << ", "
                  << "consumed=" << current_consumed << '\n'
                  << std::flush;

        previous_time = now;
        previous_consumed = current_consumed;
        report_time = now + std::chrono::seconds(report_interval);
    }

    g_stop.store(true, std::memory_order_release);
    producer.join();
    consumer.join();

    uint64_t final_produced = produced.load(std::memory_order_relaxed);
    uint64_t final_consumed = consumed.load(std::memory_order_relaxed);
    std::cout << "summary: produced=" << final_produced
              << ", consumed=" << final_consumed
              << ", publish_retries="
              << publish_retries.load(std::memory_order_relaxed)
              << ", consume_retries="
              << consume_retries.load(std::memory_order_relaxed) << '\n';

    if (failed.load(std::memory_order_acquire) ||
        final_produced != final_consumed) {
        std::cerr << "spsc stress failed\n";
        return 1;
    }
    return 0;
}
