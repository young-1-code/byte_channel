#pragma once

#include <cstddef>
#include <cstdint>

namespace btch::udp::system {

struct UdpEvent {
  int fd = -1;
  uint32_t events = 0;
};

class UdpEventLoop {
public:
  UdpEventLoop() = default;
  ~UdpEventLoop();
  UdpEventLoop(const UdpEventLoop &) = delete;
  UdpEventLoop &operator=(const UdpEventLoop &) = delete;

  int open();
  int add(int fd, uint32_t events);
  int remove(int fd);
  int wait(UdpEvent *output, size_t capacity, int timeout_ms);

private:
  int fd_ = -1;
};

} // namespace btch::udp::system
