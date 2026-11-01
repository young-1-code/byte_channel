#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "../channel_comm.h"
#include "udp_pub.h"

struct UdpSubConfig {
  std::string topic;
  std::string data_address;
  std::string interface_address;
  std::string pub_address;
  uint16_t data_port = 0;
  uint16_t control_port = 0;
  uint32_t sub_id = 0;
  UdpDataMode data_mode = UdpDataMode::kMulticast;
  uint32_t max_payload = 1200;
  bool enable_crc32 = true;
  bool has_resume_offset = false;
  uint64_t resume_offset = 0;
};

class UdpSubscriber {
private:
  struct Impl;
  std::unique_ptr<Impl> impl_;

public:
  UdpSubscriber();
  ~UdpSubscriber();

  UdpSubscriber(const UdpSubscriber &) = delete;
  UdpSubscriber &operator=(const UdpSubscriber &) = delete;

  int subscribe(const UdpSubConfig &config);
  int try_pop(char *data, int length);
  int enable_pop_lock_mode(int max_length);
  const char *try_pop_lock(int length);
  int pop_unlock();
  int unsubscribe();
};
