#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace btch::udp {

inline constexpr uint32_t kMagic = 0x42544348u;
inline constexpr uint8_t kVersion = 1;
inline constexpr uint16_t kHeaderLength = 52;
inline constexpr size_t kDefaultPayload = 1200;
inline constexpr size_t kMaxPayload = 1400;
inline constexpr size_t kMaxNakRanges = 32;

enum class Command : uint8_t {
  kSetup = 1,
  kEstablish = 2,
  kNak = 3,
  kUpdate = 4,
  kData = 5,
  kRst = 6,
  kHeartbeat = 7,
  kExit = 8,
};

enum Flags : uint16_t {
  kFlagCrc32 = 1u << 0,
  kFlagHasResume = 1u << 1,
  kFlagRetransmit = 1u << 2,
  kFlagUnicast = 1u << 3,
};

struct Header {
  uint32_t magic = kMagic;
  uint8_t version = kVersion;
  Command command = Command::kData;
  uint16_t header_length = kHeaderLength;
  uint16_t flags = 0;
  uint16_t reserved = 0;
  uint32_t topic_id = 0;
  uint32_t stream_id = 0;
  uint32_t stream_epoch = 0;
  uint32_t sub_id = 0;
  uint64_t packet_seq = 0;
  uint64_t data_offset = 0;
  uint32_t payload_length = 0;
  uint32_t checksum = 0;
};

struct SetupPayload {
  uint32_t sub_id = 0;
  uint32_t data_ipv4 = 0;
  uint16_t data_port = 0;
  uint16_t reserved = 0;
  uint64_t resume_offset = 0;
};

struct EstablishPayload {
  uint32_t sub_id = 0;
  uint64_t start_offset = 0;
};

struct UpdatePayload {
  uint32_t sub_id = 0;
  uint64_t consumed_offset = 0;
};

struct NakRange {
  uint64_t start_offset = 0;
  uint32_t length = 0;
};

struct NakPayload {
  uint32_t sub_id = 0;
  uint16_t range_count = 0;
  std::array<NakRange, kMaxNakRanges> ranges{};
};

uint32_t topic_id(std::string_view topic);
uint32_t crc32(const uint8_t *data, size_t length);
size_t encode_header(const Header &header, uint8_t *output, size_t capacity);
bool decode_header(const uint8_t *data, size_t length, Header &header);
size_t encode_setup(const SetupPayload &payload, bool has_resume,
                    uint8_t *output, size_t capacity);
bool decode_setup(const uint8_t *data, size_t length, bool has_resume,
                  SetupPayload &payload);
size_t encode_establish(const EstablishPayload &payload, uint8_t *output,
                        size_t capacity);
bool decode_establish(const uint8_t *data, size_t length,
                      EstablishPayload &payload);
size_t encode_update(const UpdatePayload &payload, uint8_t *output,
                     size_t capacity);
bool decode_update(const uint8_t *data, size_t length, UpdatePayload &payload);
size_t encode_nak(const NakPayload &payload, uint8_t *output, size_t capacity);
bool decode_nak(const uint8_t *data, size_t length, NakPayload &payload);

} // namespace btch::udp
