#include "channel.hpp"
#include "spsc_channel.hpp"

#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <limits>
#include <fcntl.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {

constexpr int kBufferSize = 1 << 20;
constexpr int kDefaultDuration = 3;

struct ControlMessage {
    uint64_t target_messages;
    int32_t status;
};

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

bool write_all(int fd, const void *data, size_t size)
{
    const auto *bytes = static_cast<const uint8_t *>(data);
    while (size > 0) {
        ssize_t written = ::write(fd, bytes, size);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) return false;
        bytes += written;
        size -= static_cast<size_t>(written);
    }
    return true;
}

bool read_all(int fd, void *data, size_t size)
{
    auto *bytes = static_cast<uint8_t *>(data);
    while (size > 0) {
        ssize_t read_size = ::read(fd, bytes, size);
        if (read_size < 0 && errno == EINTR) continue;
        if (read_size <= 0) return false;
        bytes += read_size;
        size -= static_cast<size_t>(read_size);
    }
    return true;
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

template <typename Publisher, typename Subscriber>
Result run_case(int message_size, int duration_seconds, const char *channel_name)
{
    int control_pipe[2] = {-1, -1};
    int result_pipe[2] = {-1, -1};
    if (::pipe(control_pipe) != 0 || ::pipe(result_pipe) != 0) {
        return {.failed = true};
    }

    const std::string topic =
        "ipc_channel_benchmark_" + std::to_string(::getpid()) + "_" +
        std::to_string(message_size) + "_" + channel_name;
    pid_t child_pid = ::fork();
    if (child_pid < 0) {
        return {.failed = true};
    }

    if (child_pid == 0) {
        ::close(control_pipe[1]);
        ::close(result_pipe[0]);

        ControlMessage start{};
        Result result;
        if (!read_all(control_pipe[0], &start, sizeof(start)) ||
            start.status != channel_status::kOk) {
            result.failed = true;
            write_all(result_pipe[1], &result, sizeof(result));
            _exit(1);
        }

        Subscriber subscriber;
        if (subscriber.subscribe(topic) != channel_status::kOk) {
            result.failed = true;
            write_all(result_pipe[1], &result, sizeof(result));
            _exit(1);
        }

        ControlMessage ready{0, channel_status::kOk};
        if (!write_all(result_pipe[1], &ready, sizeof(ready))) _exit(1);
        int flags = ::fcntl(control_pipe[0], F_GETFL, 0);
        if (flags >= 0) ::fcntl(control_pipe[0], F_SETFL, flags | O_NONBLOCK);

        std::vector<char> message(static_cast<size_t>(message_size));
        uint64_t expected_sequence = 0;
        uint64_t target_messages = std::numeric_limits<uint64_t>::max();
        uint8_t control_buffer[sizeof(ControlMessage)] = {};
        size_t control_bytes = 0;
        bool stopping = false;
        while (!result.failed &&
               (!stopping || result.consumed < target_messages)) {
            if (!stopping) {
                ssize_t received = ::read(
                    control_pipe[0], control_buffer + control_bytes,
                    sizeof(ControlMessage) - control_bytes);
                if (received > 0) {
                    control_bytes += static_cast<size_t>(received);
                } else if (received == 0) {
                    result.failed = true;
                } else if (errno != EAGAIN && errno != EINTR) {
                    result.failed = true;
                }

                if (control_bytes == sizeof(ControlMessage)) {
                    ControlMessage stop{};
                    std::memcpy(&stop, control_buffer, sizeof(stop));
                    target_messages = stop.target_messages;
                    stopping = true;
                }
            }

            int status = subscriber.try_pop(message.data(), message_size);
            if (status == channel_status::kOk) {
                if (!valid_message(message.data(), message_size, expected_sequence)) {
                    result.failed = true;
                    break;
                }
                ++result.consumed;
                ++expected_sequence;
            } else if (status == channel_status::kWouldBlock) {
                ++result.consume_retries;
                std::this_thread::yield();
            } else {
                result.failed = true;
            }
        }

        write_all(result_pipe[1], &result, sizeof(result));
        _exit(result.failed ? 1 : 0);
    }

    ::close(control_pipe[0]);
    ::close(result_pipe[1]);

    Publisher publisher;
    int create_status = publisher.create(
        kBufferSize, topic, MemoryType::Shared);
    ControlMessage start{0, create_status};
    if (!write_all(control_pipe[1], &start, sizeof(start)) ||
        create_status != channel_status::kOk) {
        ::close(control_pipe[1]);
        int child_status = 0;
        ::waitpid(child_pid, &child_status, 0);
        return {.failed = true};
    }

    ControlMessage ready{};
    if (!read_all(result_pipe[0], &ready, sizeof(ready)) ||
        ready.status != channel_status::kOk) {
        ::close(control_pipe[1]);
        int child_status = 0;
        ::waitpid(child_pid, &child_status, 0);
        return {.failed = true};
    }

    std::vector<char> message(static_cast<size_t>(message_size));
    uint64_t sequence = 0;
    Result result;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(duration_seconds);
    while (std::chrono::steady_clock::now() < deadline) {
        fill_message(message, sequence);
        int status = publisher.try_push(message.data(), message_size);
        if (status == channel_status::kOk) {
            ++result.produced;
            ++sequence;
        } else if (status == channel_status::kWouldBlock) {
            ++result.publish_retries;
            std::this_thread::yield();
        } else {
            result.failed = true;
            break;
        }
    }

    ControlMessage stop{result.produced, channel_status::kOk};
    if (!write_all(control_pipe[1], &stop, sizeof(stop))) result.failed = true;
    ::close(control_pipe[1]);

    Result child_result;
    bool received_result = read_all(result_pipe[0], &child_result, sizeof(child_result));
    ::close(result_pipe[0]);
    int child_status = 0;
    ::waitpid(child_pid, &child_status, 0);

    result.consumed = child_result.consumed;
    result.consume_retries = child_result.consume_retries;
    result.failed = result.failed || !received_result || child_result.failed ||
                    result.produced != result.consumed ||
                    !WIFEXITED(child_status) || WEXITSTATUS(child_status) != 0;
    return result;
}

void print_result(
    const char *channel,
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
              << "mode=ipc, "
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

    std::cout << "buffer=" << kBufferSize
              << ", duration=" << duration_seconds
              << ", transport=shared_memory_process" << '\n';

    bool failed = false;
    for (int message_size : {64, 256, 1024, 4096}) {
        Result generic = run_case<Pulisher, Subscriber>(
            message_size, duration_seconds, "generic");
        Result spsc = run_case<SpscPublisher, SpscSubscriber>(
            message_size, duration_seconds, "spsc");
        print_result("generic", message_size, duration_seconds, generic);
        print_result("spsc", message_size, duration_seconds, spsc);
        failed = failed || generic.failed || spsc.failed;
    }
    return failed ? 1 : 0;
}
