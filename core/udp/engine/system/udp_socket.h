#pragma once

#include <cstddef>
#include <cstdint>
#include <netinet/in.h>
#include <string>

namespace btch::udp::system {

struct UdpSocketConfig {
  uint16_t port = 0;
  std::string bind_address = "0.0.0.0";
  bool reuse_address = true;
  bool non_blocking = true;
  int receive_buffer = 4 * 1024 * 1024;
  int send_buffer = 4 * 1024 * 1024;
};

class UdpSocket {
 public:
  UdpSocket() = default;
  ~UdpSocket();
  UdpSocket(const UdpSocket&) = delete;
  UdpSocket& operator=(const UdpSocket&) = delete;

  int open(const UdpSocketConfig& config);
  int close();
  int fd() const { return fd_; }
  uint16_t local_port() const;
  int send_to(const sockaddr_in& peer, const void* data, size_t length) const;
  int receive(void* data, size_t capacity, sockaddr_in* peer) const;

 private:
  int fd_ = -1;
};

}  // namespace btch::udp::system
