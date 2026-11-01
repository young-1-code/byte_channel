#include "proto.h"

#include <arpa/inet.h>

#include <array>
#include <cstring>

namespace btch::udp {
namespace {

void Put16(uint8_t *output, uint16_t value) {
  value = htons(value);
  std::memcpy(output, &value, sizeof(value));
}

void Put32(uint8_t *output, uint32_t value) {
  value = htonl(value);
  std::memcpy(output, &value, sizeof(value));
}

void Put64(uint8_t *output, uint64_t value) {
  for (int index = 7; index >= 0; --index) {
    output[7 - index] = static_cast<uint8_t>(value >> (index * 8));
  }
}

uint16_t Get16(const uint8_t *data) {
  uint16_t value;
  std::memcpy(&value, data, sizeof(value));
  return ntohs(value);
}

uint32_t Get32(const uint8_t *data) {
  uint32_t value;
  std::memcpy(&value, data, sizeof(value));
  return ntohl(value);
}

uint64_t Get64(const uint8_t *data) {
  uint64_t value = 0;
  for (int index = 0; index < 8; ++index) {
    value = (value << 8) | data[index];
  }
  return value;
}

} // namespace

uint32_t topic_id(std::string_view topic) {
  uint32_t hash = 2166136261u;
  for (unsigned char character : topic) {
    hash ^= character;
    hash *= 16777619u;
  }
  return hash;
}

uint32_t crc32(const uint8_t *data, size_t length) {
  static const auto table = [] {
    std::array<uint32_t, 256> result{};
    for (uint32_t index = 0; index < result.size(); ++index) {
      uint32_t value = index;
      for (int bit = 0; bit < 8; ++bit) {
        value = (value & 1) ? (value >> 1) ^ 0xedb88320u : value >> 1;
      }
      result[index] = value;
    }
    return result;
  }();

  uint32_t checksum = 0xffffffffu;
  for (size_t index = 0; index < length; ++index) {
    checksum = table[(checksum ^ data[index]) & 255] ^ (checksum >> 8);
  }
  return checksum ^ 0xffffffffu;
}

size_t encode_header(const Header &header, uint8_t *output, size_t capacity) {
  if (capacity < kHeaderLength)
    return 0;
  Put32(output, header.magic);
  output[4] = header.version;
  output[5] = static_cast<uint8_t>(header.command);
  Put16(output + 6, header.header_length);
  Put16(output + 8, header.flags);
  Put16(output + 10, header.reserved);
  Put32(output + 12, header.topic_id);
  Put32(output + 16, header.stream_id);
  Put32(output + 20, header.stream_epoch);
  Put32(output + 24, header.sub_id);
  Put64(output + 28, header.packet_seq);
  Put64(output + 36, header.data_offset);
  Put32(output + 44, header.payload_length);
  Put32(output + 48, header.checksum);
  return kHeaderLength;
}

bool decode_header(const uint8_t *data, size_t length, Header &header) {
  if (length < kHeaderLength || Get32(data) != kMagic || data[4] != kVersion ||
      Get16(data + 6) != kHeaderLength ||
      (Get16(data + 8) & ~uint16_t{15}) != 0) {
    return false;
  }
  const auto command = static_cast<Command>(data[5]);
  if (command < Command::kSetup || command > Command::kExit)
    return false;
  if (Get32(data + 44) > kMaxPayload && command == Command::kData)
    return false;
  header.magic = Get32(data);
  header.version = data[4];
  header.command = command;
  header.header_length = Get16(data + 6);
  header.flags = Get16(data + 8);
  header.reserved = Get16(data + 10);
  header.topic_id = Get32(data + 12);
  header.stream_id = Get32(data + 16);
  header.stream_epoch = Get32(data + 20);
  header.sub_id = Get32(data + 24);
  header.packet_seq = Get64(data + 28);
  header.data_offset = Get64(data + 36);
  header.payload_length = Get32(data + 44);
  header.checksum = Get32(data + 48);
  return true;
}

size_t encode_setup(const SetupPayload &payload, bool has_resume,
                    uint8_t *output, size_t capacity) {
  const size_t length = has_resume ? 20 : 12;
  if (capacity < length)
    return 0;
  Put32(output, payload.sub_id);
  Put32(output + 4, payload.data_ipv4);
  Put16(output + 8, payload.data_port);
  Put16(output + 10, 0);
  if (has_resume)
    Put64(output + 12, payload.resume_offset);
  return length;
}

bool decode_setup(const uint8_t *data, size_t length, bool has_resume,
                  SetupPayload &payload) {
  const size_t expected = has_resume ? 20 : 12;
  if (length != expected || Get16(data + 10) != 0)
    return false;
  payload.sub_id = Get32(data);
  payload.data_ipv4 = Get32(data + 4);
  payload.data_port = Get16(data + 8);
  payload.resume_offset = has_resume ? Get64(data + 12) : 0;
  return true;
}

size_t encode_establish(const EstablishPayload &payload, uint8_t *output,
                        size_t capacity) {
  if (capacity < 12)
    return 0;
  Put32(output, payload.sub_id);
  Put64(output + 4, payload.start_offset);
  return 12;
}

bool decode_establish(const uint8_t *data, size_t length,
                      EstablishPayload &payload) {
  if (length != 12)
    return false;
  payload.sub_id = Get32(data);
  payload.start_offset = Get64(data + 4);
  return true;
}

size_t encode_update(const UpdatePayload &payload, uint8_t *output,
                     size_t capacity) {
  if (capacity < 12)
    return 0;
  Put32(output, payload.sub_id);
  Put64(output + 4, payload.consumed_offset);
  return 12;
}

bool decode_update(const uint8_t *data, size_t length, UpdatePayload &payload) {
  if (length != 12)
    return false;
  payload.sub_id = Get32(data);
  payload.consumed_offset = Get64(data + 4);
  return true;
}

size_t encode_nak(const NakPayload &payload, uint8_t *output, size_t capacity) {
  const size_t encoded_length =
      6u + static_cast<size_t>(payload.range_count) * 12u;
  if (payload.range_count > kMaxNakRanges || capacity < encoded_length) {
    return 0;
  }
  Put32(output, payload.sub_id);
  Put16(output + 4, payload.range_count);
  for (size_t index = 0; index < payload.range_count; ++index) {
    Put64(output + 6 + index * 12, payload.ranges[index].start_offset);
    Put32(output + 14 + index * 12, payload.ranges[index].length);
  }
  return encoded_length;
}

bool decode_nak(const uint8_t *data, size_t length, NakPayload &payload) {
  if (length < 6)
    return false;
  const uint16_t count = Get16(data + 4);
  const size_t encoded_length = 6u + static_cast<size_t>(count) * 12u;
  if (count > kMaxNakRanges || length != encoded_length)
    return false;
  payload.sub_id = Get32(data);
  payload.range_count = count;
  for (size_t index = 0; index < count; ++index) {
    payload.ranges[index].start_offset = Get64(data + 6 + index * 12);
    payload.ranges[index].length = Get32(data + 14 + index * 12);
  }
  return true;
}

} // namespace btch::udp
