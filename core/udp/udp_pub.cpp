#include "udp_pub.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <map>
#include <mutex>
#include <random>
#include <sys/epoll.h>
#include <thread>
#include <vector>

#include "engine/system/udp_endpoint.h"
#include "engine/system/udp_event_loop.h"
#include "engine/system/udp_socket.h"
#include "proto.h"
#include "udp_spsc_adapter.h"

namespace {

constexpr size_t kRetentionBytes = 8u * 1024u * 1024u;
}

struct UdpPublisher::Impl {
    struct Subscriber {
        sockaddr_in control_peer{};
        sockaddr_in data_peer{};
        uint64_t consumed = 0;
        std::chrono::steady_clock::time_point last_update{};
        bool active = false;
    };
    struct Packet {
        uint64_t offset = 0;
        uint64_t seq = 0;
        std::vector<uint8_t> payload;
    };
    UdpPubConfig config;
    UdpSpscPublisher input;
    UdpSpscSubscriber reader;
    btch::udp::system::UdpSocket data_socket;
    btch::udp::system::UdpSocket control_socket;
    btch::udp::system::UdpEventLoop events;
    sockaddr_in data_destination{};
    std::map<uint32_t, Subscriber> subscribers;
    std::deque<Packet> retention;
    std::thread thread;
    std::atomic<bool> stop{false};
    uint64_t offset = 0;
    uint64_t sequence = 0;
    std::atomic<size_t> retained_bytes{0};
    uint32_t stream_id = 1;
    uint32_t epoch = 1;
    std::atomic<bool> created{false};

    int Start(const UdpPubConfig& value)
    {
        if (thread.joinable() || value.topic.empty() || value.data_address.empty() || value.data_port == 0 ||
        value.control_port == 0 || value.max_payload == 0 || value.max_payload > btch::udp::kMaxPayload) return channel_status::kInvalidArgument;
        config = value;
        stream_id = std::random_device{}();
        if (stream_id == 0) stream_id = 1;
        if (input.create(static_cast<int>(kRetentionBytes), "udp_pub_input", MemoryType::Thread) != channel_status::kOk || reader.subscribe(input.shared_memory()) != channel_status::kOk) return channel_status::kSystemError;
        if (!btch::udp::system::parse_ipv4(config.data_address, data_destination, config.data_port)) return channel_status::kInvalidArgument;
        btch::udp::system::UdpSocketConfig data_cfg;
        data_cfg.port = 0;
        btch::udp::system::UdpSocketConfig ctl_cfg;
        ctl_cfg.port = config.control_port;
        ctl_cfg.bind_address = config.control_address;
        if (data_socket.open(data_cfg) != 0 || control_socket.open(ctl_cfg) != 0) { data_socket.close();
            control_socket.close();
            return channel_status::kSystemError;
        }
        if (events.open() != 0 || events.add(control_socket.fd(), EPOLLIN) != 0) { data_socket.close();
            control_socket.close();
            return channel_status::kSystemError;
        }
        btch::udp::system::set_broadcast(data_socket.fd(), config.data_mode == UdpDataMode::kUnicast);
        if (config.data_mode == UdpDataMode::kMulticast && !config.interface_address.empty() && btch::udp::system::set_multicast_interface(data_socket.fd(), config.interface_address) != 0) return channel_status::kInvalidArgument;
        stop.store(false);
        created = true;
        thread = std::thread([this] { Run(); });
        return channel_status::kOk;
    }

    void Run()
    {
        std::vector<uint8_t> buffer(config.max_payload);
        while (!stop.load(std::memory_order_acquire)) {
            btch::udp::system::UdpEvent event;
            events.wait(&event, 1, 1);
            PollControl();
            ExpireSubscribers();
            bool published = false;
            for (int batch = 0; batch < 256; ++batch) {
                const int available = reader.available_read_len();
                if (available <= 0) break;
                const int length =
                std::min<int>(available, static_cast<int>(buffer.size()));
                if (!CanRetain(static_cast<size_t>(length))) break;
                if (reader.try_pop(reinterpret_cast<char *>(buffer.data()), length) !=
                channel_status::kOk) {
                    break;
                }
                Publish(buffer.data(), length);
                published = true;
            }
            if (!published) {
                std::this_thread::sleep_for(std::chrono::microseconds(20));
            }
        }
    }

    void PollControl()
    {
        std::vector<uint8_t> packet(btch::udp::kHeaderLength + 6 + btch::udp::kMaxNakRanges * 12);
        sockaddr_in peer{};
        for (;;) {
            const int length = control_socket.receive(packet.data(), packet.size(), &peer);
            if (length < 0) return;
            btch::udp::Header h;
            if (!btch::udp::decode_header(packet.data(), length, h) || h.topic_id != btch::udp::topic_id(config.topic) || (h.command != btch::udp::Command::kSetup && h.stream_id != stream_id) || static_cast<size_t>(length) < btch::udp::kHeaderLength + h.payload_length) continue;
            const uint8_t* body = packet.data() + btch::udp::kHeaderLength;
            if (h.command == btch::udp::Command::kSetup) Setup(h, body, peer);
            else if (h.command == btch::udp::Command::kUpdate) Update(h, body, h.payload_length, peer);
            else if (h.command == btch::udp::Command::kNak) Nak(h, body, peer);
            else if (h.command == btch::udp::Command::kHeartbeat) Heartbeat(h, peer);
            else if (h.command == btch::udp::Command::kExit || h.command == btch::udp::Command::kRst) { auto it = subscribers.find(h.sub_id);
                if (h.stream_epoch == epoch && it != subscribers.end() && it->second.control_peer.sin_addr.s_addr == peer.sin_addr.s_addr && it->second.control_peer.sin_port == peer.sin_port) it->second.active = false;
            }
        }
    }

    void Setup(const btch::udp::Header& h, const uint8_t* body, const sockaddr_in& peer)
    {
        btch::udp::SetupPayload setup;
        const bool resume = (h.flags & btch::udp::kFlagHasResume) != 0;
        if (!btch::udp::decode_setup(body, h.payload_length, resume, setup) ||
        h.sub_id != setup.sub_id) return;
        const bool resume_invalid = setup.resume_offset > offset || (retention.empty() ? setup.resume_offset != offset : setup.resume_offset < retention.front().offset);
        if (resume && resume_invalid) { SendRst(peer, setup.sub_id);
            return;
        }
        // stream_epoch 属于整个 Pub 流，而不是单个 Sub。单个订阅者重连时
        // 只恢复自己的消费位置，不能让其他活跃订阅者看到新的 epoch。
        auto& sub = subscribers[setup.sub_id];
        sub.control_peer = peer;
        sub.active = true;
        sub.last_update = std::chrono::steady_clock::now();
        sub.consumed = resume ? setup.resume_offset : offset;
        if (setup.data_port) { sub.data_peer.sin_family = AF_INET;
            sub.data_peer.sin_port = htons(setup.data_port);
            sub.data_peer.sin_addr.s_addr = setup.data_ipv4 ? setup.data_ipv4 : peer.sin_addr.s_addr;
        }
        btch::udp::EstablishPayload establish{setup.sub_id, sub.consumed};
        std::vector<uint8_t> body_out(12), packet(btch::udp::kHeaderLength + 12);
        btch::udp::encode_establish(establish, body_out.data(), body_out.size());
        btch::udp::Header response;
        response.command = btch::udp::Command::kEstablish;
        response.topic_id = btch::udp::topic_id(config.topic);
        response.stream_id = stream_id;
        response.stream_epoch = epoch;
        response.sub_id = setup.sub_id;
        response.payload_length = body_out.size();
        btch::udp::encode_header(response, packet.data(), packet.size());
        std::memcpy(packet.data() + btch::udp::kHeaderLength, body_out.data(), body_out.size());
        control_socket.send_to(peer, packet.data(), packet.size());
        const sockaddr_in& replay_destination = config.data_mode == UdpDataMode::kMulticast ? data_destination : sub.data_peer;
        for (const auto& saved : retention) if (saved.offset + saved.payload.size() > sub.consumed) Send(saved, replay_destination, true);
    }

    void Update(const btch::udp::Header& header, const uint8_t* body, size_t length, const sockaddr_in& peer) { btch::udp::UpdatePayload update;
        if (header.stream_epoch != epoch || !btch::udp::decode_update(body, length, update) || header.sub_id != update.sub_id) return;
        auto it = subscribers.find(update.sub_id);
        if (it == subscribers.end() || !it->second.active || it->second.control_peer.sin_addr.s_addr != peer.sin_addr.s_addr || it->second.control_peer.sin_port != peer.sin_port || update.consumed_offset < it->second.consumed || update.consumed_offset > offset) return;
        it->second.last_update = std::chrono::steady_clock::now();
        it->second.consumed = update.consumed_offset;
        Reclaim();
    }

    void Heartbeat(const btch::udp::Header& header, const sockaddr_in& peer) { auto it = subscribers.find(header.sub_id);
        if (header.stream_epoch != epoch || it == subscribers.end() || !it->second.active || it->second.control_peer.sin_addr.s_addr != peer.sin_addr.s_addr || it->second.control_peer.sin_port != peer.sin_port) return;
        it->second.last_update = std::chrono::steady_clock::now();
        btch::udp::Header response;
        response.command = btch::udp::Command::kHeartbeat;
        response.topic_id = btch::udp::topic_id(config.topic);
        response.stream_id = stream_id;
        response.stream_epoch = epoch;
        response.sub_id = header.sub_id;
        std::array<uint8_t, btch::udp::kHeaderLength> wire{};
        btch::udp::encode_header(response, wire.data(), wire.size());
        control_socket.send_to(peer, wire.data(), wire.size());
    }

    void Nak(const btch::udp::Header& h, const uint8_t* body, const sockaddr_in& peer) { btch::udp::NakPayload nak;
        if (h.stream_epoch != epoch || !btch::udp::decode_nak(body, h.payload_length, nak) || h.sub_id != nak.sub_id) return;
        auto it = subscribers.find(nak.sub_id);
        if (it == subscribers.end() || !it->second.active || it->second.control_peer.sin_addr.s_addr != peer.sin_addr.s_addr || it->second.control_peer.sin_port != peer.sin_port) return;
        const sockaddr_in& destination = config.data_mode == UdpDataMode::kMulticast ? data_destination : it->second.data_peer;
        for (size_t i = 0; i < nak.range_count; ++i) { const auto& range = nak.ranges[i];
            if (range.length == 0 || range.start_offset > UINT64_MAX - range.length) continue;
            const uint64_t requested_end = range.start_offset + range.length;
            for (const auto& packet : retention) if (packet.offset < requested_end && packet.offset + packet.payload.size() > range.start_offset) Send(packet, destination, true);
        } }

    void Publish(const uint8_t* data, size_t length)
    {
        Packet packet{offset, sequence++, std::vector<uint8_t>(data, data + length)};
        retention.push_back(packet);
        offset += length;
        retained_bytes.fetch_add(length, std::memory_order_release);
        if (config.data_mode == UdpDataMode::kMulticast) Send(packet, data_destination, false);
        else for (const auto& entry : subscribers) if (entry.second.active) Send(packet, entry.second.data_peer, false);
        Reclaim();
    }

    bool CanRetain(size_t length) const {
        uint64_t minimum = offset;
        for (const auto& entry : subscribers) if (entry.second.active) minimum = std::min(minimum, entry.second.consumed);
        return offset + length - minimum <= kRetentionBytes;
    }

    void Send(const Packet& packet_data, const sockaddr_in& destination, bool retransmit)
    {
        if (destination.sin_port == 0) return;
        std::array<uint8_t, btch::udp::kHeaderLength + btch::udp::kMaxPayload> wire{};
        btch::udp::Header h;
        h.command = btch::udp::Command::kData;
        h.flags = (config.enable_crc32 ? btch::udp::kFlagCrc32 : 0) |
        (retransmit ? btch::udp::kFlagRetransmit : 0);
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = stream_id;
        h.stream_epoch = epoch;
        h.packet_seq = packet_data.seq;
        h.data_offset = packet_data.offset;
        h.payload_length = packet_data.payload.size();
        const size_t wire_length = btch::udp::kHeaderLength + packet_data.payload.size();
        btch::udp::encode_header(h, wire.data(), wire.size());
        std::memcpy(wire.data() + btch::udp::kHeaderLength,
        packet_data.payload.data(), packet_data.payload.size());
        if (config.enable_crc32) {
            h.checksum = btch::udp::crc32(wire.data(), wire_length);
            btch::udp::encode_header(h, wire.data(), wire.size());
        }
        data_socket.send_to(destination, wire.data(), wire_length);
    }

    void Reclaim() { uint64_t minimum = offset;
        for (const auto& entry : subscribers) if (entry.second.active) minimum = std::min(minimum, entry.second.consumed);
        while (!retention.empty() && retention.front().offset + retention.front().payload.size() <= minimum) { retained_bytes.fetch_sub(retention.front().payload.size(), std::memory_order_release);
            retention.pop_front();
        } }
    void ExpireSubscribers() { const auto now = std::chrono::steady_clock::now();
        for (auto& entry : subscribers) if (entry.second.active && entry.second.last_update.time_since_epoch().count() != 0 && now - entry.second.last_update > std::chrono::seconds(10)) { SendRst(entry.second.control_peer, entry.first);
            entry.second.active = false;
        } Reclaim();
    }
    void SendRst(const sockaddr_in& peer, uint32_t sub_id) { btch::udp::Header h;
        h.command = btch::udp::Command::kRst;
        h.topic_id = btch::udp::topic_id(config.topic);
        h.stream_id = stream_id;
        h.stream_epoch = epoch;
        h.sub_id = sub_id;
        std::array<uint8_t, btch::udp::kHeaderLength> wire{};
        btch::udp::encode_header(h, wire.data(), wire.size());
        control_socket.send_to(peer, wire.data(), wire.size());
    }
    void Close() { stop.store(true);
        if (thread.joinable()) thread.join();
        data_socket.close();
        control_socket.close();
        subscribers.clear();
        retention.clear();
        created = false;
    }
};

UdpPublisher::UdpPublisher() : impl_(new Impl) {}
UdpPublisher::~UdpPublisher() { close();
}
int UdpPublisher::create(const UdpPubConfig& config) { return impl_->Start(config);
}
int UdpPublisher::try_push(const char* data, int length) { if (!impl_->created.load(std::memory_order_acquire)) return channel_status::kNotInitialized;
    if (data == nullptr || length <= 0) return channel_status::kInvalidArgument;
    const size_t retained = impl_->retained_bytes.load(std::memory_order_acquire);
    const size_t input_free = static_cast<size_t>(impl_->input.available_write_len());
    const size_t input_used = kRetentionBytes - input_free;
    if (input_free < static_cast<size_t>(length) || retained + input_used + static_cast<size_t>(length) > kRetentionBytes) return channel_status::kWouldBlock;
    return impl_->input.try_push(data, length);
}
int UdpPublisher::get_available_publish_len() const { if (!impl_->created.load(std::memory_order_acquire)) return 0;
    const size_t retained = impl_->retained_bytes.load(std::memory_order_acquire);
    const size_t input_free = static_cast<size_t>(impl_->input.available_write_len());
    const size_t input_used = kRetentionBytes - input_free;
    if (retained + input_used >= kRetentionBytes) return 0;
    return static_cast<int>(std::min(kRetentionBytes - retained - input_used, input_free));
}
int UdpPublisher::close() { impl_->Close();
    return channel_status::kOk;
}
