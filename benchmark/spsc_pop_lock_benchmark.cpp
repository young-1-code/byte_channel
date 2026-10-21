#include "spsc_channel.hpp"

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

constexpr int kBufferSize = 1 << 20;
constexpr int kDefaultDuration = 3;
constexpr int kDefaultMaxBlockSize = kBufferSize;

struct Result {
    uint64_t produced = 0;
    uint64_t consumed = 0;
    uint64_t publish_retries = 0;
    uint64_t consume_retries = 0;
    bool failed = false;
};

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

void fill_message(std::vector<char> &message, uint64_t sequence)
{
    for (size_t index = 0; index < message.size(); ++index) {
        message[index] = static_cast<char>((sequence + index) & 0xff);
    }
}

bool valid_message(const char *message, int size, uint64_t sequence)
{
    for (int index = 0; index < size; ++index) {
        if (message[index] != static_cast<char>((sequence + index) & 0xff)) {
            return false;
        }
    }
    return true;
}

template <typename Publisher, typename Subscriber, bool UsePopLock>
Result run_case(int message_size, int duration_seconds, int max_block_size)
{
    Publisher publisher;
    if (publisher.create(
            kBufferSize, "pop_lock_benchmark", MemoryType::Thread) != 0) {
        return {.failed = true};
    }

    Subscriber subscriber;
    if (subscriber.subscribe(publisher.shared_memory()) != 0) {
        return {.failed = true};
    }
    if constexpr (UsePopLock) {
        if (subscriber.enable_pop_lock_mode(max_block_size) != 0) {
            return {.failed = true};
        }
    }

    std::vector<char> send(static_cast<size_t>(message_size));
    std::vector<char> receive(static_cast<size_t>(message_size));
    std::atomic<bool> stop{false};
    std::atomic<bool> failed{false};
    std::atomic<bool> producer_done{false};
    std::atomic<uint64_t> produced{0};
    std::atomic<uint64_t> consumed{0};
    std::atomic<uint64_t> publish_retries{0};
    std::atomic<uint64_t> consume_retries{0};
    Result result;

    std::thread producer([&] {
        uint64_t sequence = 0;
        while (!stop.load(std::memory_order_relaxed) &&
               !failed.load(std::memory_order_relaxed)) {
            fill_message(send, sequence);
            int status = publisher.try_push(send.data(), message_size);
            if (status == 0) {
                produced.fetch_add(1, std::memory_order_relaxed);
                ++sequence;
            } else if (status == -2) {
                publish_retries.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::yield();
            } else {
                failed.store(true, std::memory_order_release);
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        uint64_t expected_sequence = 0;
        while (!failed.load(std::memory_order_relaxed) &&
               (!stop.load(std::memory_order_relaxed) ||
                !producer_done.load(std::memory_order_acquire) ||
                consumed.load(std::memory_order_relaxed) <
                    produced.load(std::memory_order_relaxed))) {
            const char *data = nullptr;
            int status = 0;
            if constexpr (UsePopLock) {
                data = subscriber.try_pop_lock(message_size);
                status = data == nullptr ? -2 : 0;
            } else {
                status = subscriber.try_pop(receive.data(), message_size);
                data = receive.data();
            }

            if (status == 0) {
                if (!valid_message(data, message_size, expected_sequence)) {
                    failed.store(true, std::memory_order_release);
                    break;
                }
                if constexpr (UsePopLock) {
                    if (subscriber.pop_unlock() != 0) {
                        failed.store(true, std::memory_order_release);
                        break;
                    }
                }
                consumed.fetch_add(1, std::memory_order_relaxed);
                ++expected_sequence;
            } else if (status == -2) {
                consume_retries.fetch_add(1, std::memory_order_relaxed);
                std::this_thread::yield();
            } else {
                failed.store(true, std::memory_order_release);
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::seconds(duration_seconds));
    stop.store(true, std::memory_order_release);
    producer.join();
    consumer.join();

    result.produced = produced.load(std::memory_order_relaxed);
    result.consumed = consumed.load(std::memory_order_relaxed);
    result.publish_retries = publish_retries.load(std::memory_order_relaxed);
    result.consume_retries = consume_retries.load(std::memory_order_relaxed);
    result.failed = failed.load(std::memory_order_acquire) ||
                    result.produced != result.consumed;
    return result;
}

void print_result(
    const char *channel,
    const char *mode,
    int message_size,
    int duration_seconds,
    const Result &result)
{
    double messages_per_second =
        static_cast<double>(result.consumed) / duration_seconds;
    double mib_per_second =
        messages_per_second * message_size / (1024.0 * 1024.0);
    std::cout << std::fixed << std::setprecision(2)
              << "channel=" << channel << ", "
              << "mode=" << mode << ", "
              << "message_size=" << message_size << ", "
              << "interval=" << duration_seconds << " s, "
              << "spsc=" << std::setprecision(0) << messages_per_second
              << " msg/s, "
              << "bandwidth=" << std::setprecision(2) << mib_per_second
              << " MiB/s, "
              << "produced=" << result.produced << ", "
              << "consumed=" << result.consumed << ", "
              << "publish_retries=" << result.publish_retries << ", "
              << "consume_retries=" << result.consume_retries << ", "
              << "status=" << (result.failed ? "FAIL" : "OK") << '\n';
}

}

int main(int argc, char **argv)
{
    int duration_seconds = argc > 1
        ? parse_positive_int(argv[1], kDefaultDuration)
        : kDefaultDuration;
    int max_block_size = argc > 2
        ? parse_positive_int(argv[2], kDefaultMaxBlockSize)
        : kDefaultMaxBlockSize;

    std::cout << "buffer=" << kBufferSize
              << ", duration=" << duration_seconds
              << ", pop_lock_max_block=" << max_block_size << '\n';

    for (int message_size : {64, 256, 1024, 4096}) {
        Result generic_copy_result = run_case<Pulisher, Subscriber, false>(
            message_size, duration_seconds, max_block_size);
        Result generic_lock_result = run_case<Pulisher, Subscriber, true>(
            message_size, duration_seconds, max_block_size);
        Result spsc_copy_result = run_case<SpscPublisher, SpscSubscriber, false>(
            message_size, duration_seconds, max_block_size);
        Result spsc_lock_result = run_case<SpscPublisher, SpscSubscriber, true>(
            message_size, duration_seconds, max_block_size);
        print_result(
            "generic", "copy", message_size, duration_seconds,
            generic_copy_result);
        print_result(
            "generic", "pop_lock", message_size, duration_seconds,
            generic_lock_result);
        print_result(
            "spsc", "copy", message_size, duration_seconds,
            spsc_copy_result);
        print_result(
            "spsc", "pop_lock", message_size, duration_seconds,
            spsc_lock_result);
    }
    return 0;
}
