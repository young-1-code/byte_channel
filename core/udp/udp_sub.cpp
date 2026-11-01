#include "udp_sub.h"

#include <arpa/inet.h>
#include <atomic>
#include <array>
#include <chrono>
#include <cstring>
#include <iterator>
#include <map>
#include <random>
#include <sys/epoll.h>
#include <thread>
#include <vector>

#include "engine/system/udp_endpoint.h"
#include "engine/system/udp_event_loop.h"
#include "engine/system/udp_socket.h"
#include "proto.h"
#include "udp_spsc_adapter.h"

struct UdpSubscriber::Impl {
    UdpSubConfig config;
    UdpSpscPublisher output;
    UdpSpscSubscriber application;
    btch::udp::system::UdpSocket data_socket;
    btch::udp::system::UdpSocket control_socket;
    btch::udp::system::UdpEventLoop events;
    sockaddr_in publisher{};
    std::thread thread;
    std::atomic<bool> stop{false};
    uint64_t commit_offset = 0;
    uint64_t epoch = 0;
    uint32_t stream_id = 0;
    uint64_t last_update = 0;
    std::map<uint64_t, uint64_t> ranges;
    uint64_t requested_resume = 0;
    bool established = false;
    std::chrono::steady_clock::time_point next_setup{};
    std::mt19937 random{std::random_device{}()};
    bool nak_scheduled = false;
    std::vector<std::vector<uint8_t>> pending_data;
    size_t pending_bytes = 0;
    uint8_t nak_retries = 0;
    std::chrono::steady_clock::time_point next_nak{};
    std::chrono::steady_clock::time_point next_heartbeat{};

    int Start(const UdpSubConfig& value)
    {
        if (thread.joinable() || value.topic.empty() || value.data_address.empty() || value.pub_address.empty() || value.control_port == 0 || value.sub_id == 0 || value.max_payload == 0 || value.max_payload > btch::udp::kMaxPayload || (value.data_mode == UdpDataMode::kMulticast && value.data_port == 0)) return channel_status::kInvalidArgument;
        config = value;
        if (output.create(8 * 1024 * 1024, "udp_sub_output", MemoryType::Thread) != channel_status::kOk || application.subscribe(output.shared_memory()) != channel_status::kOk) return channel_status::kSystemError;
        if (!btch::udp::system::parse_ipv4(config.pub_address, publisher, config.control_port)) return channel_status::kInvalidArgument;
        btch::udp::system::UdpSocketConfig data_cfg;
        data_cfg.port = config.data_port;
        btch::udp::system::UdpSocketConfig control_cfg;
        control_cfg.port = 0;
        if (data_socket.open(data_cfg) != 0 || control_socket.open(control_cfg) != 0) { data_socket.close();
            control_socket.close();
            return channel_status::kSystemError;
        }
        if (events.open() != 0 || events.add(data_socket.fd(), EPOLLIN) != 0 || events.add(control_socket.fd(), EPOLLIN) != 0) { data_socket.close();
            control_socket.close();
            return channel_status::kSystemError;
        }
        if (config.data_mode == UdpDataMode::kMulticast && btch::udp::system::join_multicast(data_socket.fd(), config.data_address, config.interface_address) != 0) return channel_status::kSystemError;
        stop.store(false);
        next_setup = std::chrono::steady_clock::now();
        next_heartbeat = next_setup + std::chrono::seconds(1);
        thread = std::thread([this] { Run(); });
        return channel_status::kOk;
    }

    void Run()
    {
        std::vector<uint8_t> packet(65535);
        auto last_heartbeat = std::chrono::steady_clock::now();
        while (!stop.load(std::memory_order_acquire)) {
            btch::udp::system::UdpEvent event;
            events.wait(&event, 1, 1);
            // 一次事件循环批量清空内核队列，避免每轮只处理一个 DATA 包导致
            // UDP 接收队列积压。控制包在每个批次前后各处理一次，保证 NAK/RST
            // 不会被持续的数据流长期饿死。
            for (int index = 0; index < 64; ++index) {
                sockaddr_in control_source{};
                const int control_length = control_socket.receive(packet.data(), packet.size(), &control_source);
                if (control_length < 0) break;
                if (control_length >= static_cast<int>(btch::udp::kHeaderLength)) {
                    HandleControl(packet.data(), control_length, control_source);
                }
            }
            for (int index = 0; index < 256; ++index) {
                sockaddr_in source{};
                const int length = data_socket.receive(packet.data(), packet.size(), &source);
                if (length < 0) break;
                if (length >= static_cast<int>(btch::udp::kHeaderLength)) {
                    HandleData(packet.data(), length);
                }
            }
            for (int index = 0; index < 64; ++index) {
                sockaddr_in control_source{};
                const int control_length = control_socket.receive(packet.data(), packet.size(), &control_source);
                if (control_length < 0) break;
                if (control_length >= static_cast<int>(btch::udp::kHeaderLength)) {
                    HandleControl(packet.data(), control_length, control_source);
                }
            }
            const auto now = std::chrono::steady_clock::now();
            if (!established && now >= next_setup) { SendSetup();
                next_setup = now + std::chrono::milliseconds(200);
            }
            if (established && now >= next_heartbeat) { SendHeartbeat();
                next_heartbeat = now + std::chrono::seconds(1);
            }
            if (commit_offset != last_update && (last_update == 0 || now - last_heartbeat > std::chrono::milliseconds(5))) { SendUpdate();
                last_heartbeat = now;
            }
            if (!ranges.empty() && std::chrono::steady_clock::now() >= next_nak) SendNak();
            std::this_thread::sleep_for(std::chrono::microseconds(20));
        }
    }

    void HandleData(const uint8_t* bytes, size_t length)
    {
        btch::udp::Header h;
        if (!btch::udp::decode_header(bytes, length, h) ||
        h.command != btch::udp::Command::kData ||
        h.topic_id != btch::udp::topic_id(config.topic) || h.stream_id == 0 ||
        h.payload_length == 0 ||
        (stream_id != 0 && h.stream_id != stream_id) ||
        h.payload_length != length - btch::udp::kHeaderLength ||
        h.data_offset + h.payload_length < h.data_offset) {
            return;
        }
        if ((h.flags & btch::udp::kFlagCrc32) != 0) {
            std::array<uint8_t, btch::udp::kHeaderLength + btch::udp::kMaxPayload>
            copy{};
            if (length > copy.size()) return;
            std::memcpy(copy.data(), bytes, length);
            const uint32_t expected = h.checksum;
            std::memset(copy.data() + 48, 0, 4);
            if (btch::udp::crc32(copy.data(), length) != expected) return;
        }
        if (!established) { if (pending_bytes + length <= 8u * 1024u * 1024u) { pending_data.emplace_back(bytes, bytes + length);
                pending_bytes += length;
            } return;
        }
        if (epoch != 0 && h.stream_epoch != epoch) return;
        if (epoch == 0) epoch = h.stream_epoch;
        const uint64_t packet_end = h.data_offset + h.payload_length;
        if (packet_end <= commit_offset) return;
        const uint64_t write_offset = std::max(h.data_offset, commit_offset);
        const size_t skip = static_cast<size_t>(write_offset - h.data_offset);
        const int write_length = static_cast<int>(packet_end - write_offset);
        const int result = output.try_write_at(
        write_offset,
        reinterpret_cast<const char *>(bytes + btch::udp::kHeaderLength + skip),
        write_length);
        if (result != channel_status::kOk) return;
        RecordRange(write_offset, packet_end);
        Advance();
        if (!ranges.empty() && !nak_scheduled) { std::uniform_int_distribution<int> delay(50, 200);
            next_nak = std::chrono::steady_clock::now() + std::chrono::microseconds(delay(random));
            nak_scheduled = true;
        }
    }

    void HandleControl(const uint8_t* bytes, size_t length, const sockaddr_in& source)
    {
        if (source.sin_addr.s_addr != publisher.sin_addr.s_addr || source.sin_port != publisher.sin_port) return;
        btch::udp::Header h;
        if (!btch::udp::decode_header(bytes, length, h) || h.topic_id != btch::udp::topic_id(config.topic) || h.stream_id == 0) return;
        if (h.command == btch::udp::Command::kHeartbeat) return;
        if (h.command == btch::udp::Command::kRst) { if (!established || h.stream_id != stream_id || h.stream_epoch != epoch) return;
            established = false;
            requested_resume = commit_offset;
            ranges.clear();
            pending_data.clear();
            pending_bytes = 0;
            nak_retries = 0;
            nak_scheduled = false;
            next_setup = std::chrono::steady_clock::now();
            return;
        }
        if (h.command != btch::udp::Command::kEstablish || h.payload_length + btch::udp::kHeaderLength > length) return;
        btch::udp::EstablishPayload establish;
        if (!btch::udp::decode_establish(bytes + btch::udp::kHeaderLength, h.payload_length, establish) || establish.sub_id != config.sub_id) return;
        if (output.initialize_position(establish.start_offset) != channel_status::kOk || application.reset_consumer_position(establish.start_offset) != channel_status::kOk) return;
        commit_offset = establish.start_offset;
        requested_resume = establish.start_offset;
        epoch = h.stream_epoch;
        stream_id = h.stream_id;
        ranges.clear();
        nak_retries = 0;
        nak_scheduled = false;
        last_update = 0;
        established = true;
        auto pending = std::move(pending_data);
        pending_data.clear();
        pending_bytes = 0;
        for (const auto& data : pending) HandleData(data.data(), data.size());
    }

    void RecordRange(uint64_t start, uint64_t end)
    {
        if (start >= end) return;
        auto it = ranges.lower_bound(start);
        if (it != ranges.begin()) {
            auto previous = std::prev(it);
            if (previous->second >= start) {
                start = std::min(start, previous->first);
                end = std::max(end, previous->second);
                it = ranges.erase(previous);
            }
        }
        while (it != ranges.end() && it->first <= end) {
            end = std::max(end, it->second);
            it = ranges.erase(it);
        }
        ranges.emplace(start, end);
    }

    void Advance() { for (;;) { auto it = ranges.begin();
            if (it == ranges.end() || it->first > commit_offset) break;
            commit_offset = std::max(commit_offset, it->second);
            ranges.erase(it);
            if (output.commit_until(commit_offset) != channel_status::kOk) break;
        } if (ranges.empty()) { nak_retries = 0;
            nak_scheduled = false;
        } }

    void SendSetup() { btch::udp::SetupPayload setup;
        setup.sub_id = config.sub_id;
        setup.data_ipv4 = 0;
        setup.data_port = data_socket.local_port();
        const bool resume = config.has_resume_offset || requested_resume != 0;
        setup.resume_offset = requested_resume != 0 ? requested_resume : config.resume_offset;
        std::vector<uint8_t> body(resume ? 20 : 12), packet(btch::udp::kHeaderLength + body.size());
        btch::udp::encode_setup(setup, resume, body.data(), body.size());
        btch::udp::Header h;
        h.command = btch::udp::Command::kSetup;
        h.flags = resume ? btch::udp::kFlagHasResume : 0;
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = 0;
        h.sub_id = config.sub_id;
        h.payload_length = body.size();
        btch::udp::encode_header(h, packet.data(), packet.size());
        std::memcpy(packet.data() + btch::udp::kHeaderLength, body.data(), body.size());
        control_socket.send_to(publisher, packet.data(), packet.size());
    }

    void SendUpdate() { btch::udp::UpdatePayload update{config.sub_id, application.consumed_position()};
        std::vector<uint8_t> body(12), packet(btch::udp::kHeaderLength + 12);
        btch::udp::encode_update(update, body.data(), body.size());
        btch::udp::Header h;
        h.command = btch::udp::Command::kUpdate;
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = stream_id;
        h.stream_epoch = epoch;
        h.sub_id = config.sub_id;
        h.payload_length = body.size();
        btch::udp::encode_header(h, packet.data(), packet.size());
        std::memcpy(packet.data() + btch::udp::kHeaderLength, body.data(), body.size());
        control_socket.send_to(publisher, packet.data(), packet.size());
        last_update = application.consumed_position();
    }

    void SendHeartbeat() { btch::udp::Header h;
        h.command = btch::udp::Command::kHeartbeat;
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = stream_id;
        h.stream_epoch = epoch;
        h.sub_id = config.sub_id;
        std::array<uint8_t, btch::udp::kHeaderLength> packet{};
        btch::udp::encode_header(h, packet.data(), packet.size());
        control_socket.send_to(publisher, packet.data(), packet.size());
    }

    void SendNak()
    {
        if (nak_retries >= 2) { SendRst();
            return;
        }
        btch::udp::NakPayload nak;
        nak.sub_id = config.sub_id;
        nak.range_count = 0;
        uint64_t cursor = commit_offset;
        for (const auto& received : ranges) {
            if (received.second <= cursor) continue;
            if (received.first > cursor && nak.range_count < btch::udp::kMaxNakRanges) {
                const uint64_t length = received.first - cursor;
                if (length <= UINT32_MAX) nak.ranges[nak.range_count++] = {cursor, static_cast<uint32_t>(length)};
            }
            cursor = std::max(cursor, received.second);
        }
        if (nak.range_count == 0) { next_nak = std::chrono::steady_clock::now() + std::chrono::milliseconds(2);
            return;
        }
        std::vector<uint8_t> body(6 + nak.range_count * 12), packet(btch::udp::kHeaderLength + body.size());
        btch::udp::encode_nak(nak, body.data(), body.size());
        btch::udp::Header h;
        h.command = btch::udp::Command::kNak;
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = stream_id;
        h.stream_epoch = epoch;
        h.sub_id = config.sub_id;
        h.payload_length = body.size();
        btch::udp::encode_header(h, packet.data(), packet.size());
        std::memcpy(packet.data() + btch::udp::kHeaderLength, body.data(), body.size());
        control_socket.send_to(publisher, packet.data(), packet.size());
        ++nak_retries;
        next_nak = std::chrono::steady_clock::now() + std::chrono::milliseconds(2);
        nak_scheduled = true;
    }

    void SendRst() { btch::udp::Header h;
        h.command = btch::udp::Command::kRst;
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = stream_id;
        h.stream_epoch = epoch;
        h.sub_id = config.sub_id;
        std::array<uint8_t, btch::udp::kHeaderLength> packet{};
        btch::udp::encode_header(h, packet.data(), packet.size());
        control_socket.send_to(publisher, packet.data(), packet.size());
        established = false;
        requested_resume = commit_offset;
        nak_retries = 0;
        nak_scheduled = false;
        next_setup = std::chrono::steady_clock::now();
    }

    void SendExit() { if (control_socket.fd() < 0 || stream_id == 0) return;
        btch::udp::Header h;
        h.command = btch::udp::Command::kExit;
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = stream_id;
        h.stream_epoch = epoch;
        h.sub_id = config.sub_id;
        std::array<uint8_t, btch::udp::kHeaderLength> packet{};
        btch::udp::encode_header(h, packet.data(), packet.size());
        control_socket.send_to(publisher, packet.data(), packet.size());
    }
    void Close() { if (established) SendExit();
        stop.store(true);
        if (thread.joinable()) thread.join();
        data_socket.close();
        control_socket.close();
        established = false;
    }
};

UdpSubscriber::UdpSubscriber() : impl_(new Impl) {}
UdpSubscriber::~UdpSubscriber() { unsubscribe();
}
int UdpSubscriber::subscribe(const UdpSubConfig& config) { return impl_->Start(config);
}
int UdpSubscriber::try_pop(char* data, int length) { return impl_->application.try_pop(data, length);
}
int UdpSubscriber::enable_pop_lock_mode(int max_length) { return impl_->application.enable_pop_lock_mode(max_length);
}
const char* UdpSubscriber::try_pop_lock(int length) { return impl_->application.try_pop_lock(length);
}
int UdpSubscriber::pop_unlock() { return impl_->application.pop_unlock();
}
int UdpSubscriber::unsubscribe() { impl_->Close();
    return channel_status::kOk;
}
