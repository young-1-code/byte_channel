#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "../channel_comm.h"

enum class UdpDataMode {
  kMulticast,
  kUnicast,
};

struct UdpPubConfig {
  std::string topic;
  std::string data_address;
  std::string interface_address;
  std::string control_address = "0.0.0.0";
  uint16_t data_port = 0;
  uint16_t control_port = 0;
  UdpDataMode data_mode = UdpDataMode::kMulticast;
  uint32_t max_payload = 1200;
  bool enable_crc32 = true;
};

class UdpPublisher {
private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

public:
  UdpPublisher();
  ~UdpPublisher();

  UdpPublisher(const UdpPublisher &) = delete;
  UdpPublisher &operator=(const UdpPublisher &) = delete;

  int create(const UdpPubConfig &config);
  int try_push(const char *data, int length);
  int get_available_publish_len() const;
  int close();
};
