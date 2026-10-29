#include "udp_event_loop.h"

#include <sys/epoll.h>
#include <unistd.h>

#include <algorithm>
#include <climits>

namespace btch::udp::system {

UdpEventLoop::~UdpEventLoop() { if (fd_ >= 0) ::close(fd_); }

int UdpEventLoop::open() {
  if (fd_ >= 0) return 0;
  fd_ = ::epoll_create1(EPOLL_CLOEXEC);
  return fd_ >= 0 ? 0 : -1;
}

int UdpEventLoop::add(int fd, uint32_t events) {
  if (fd_ < 0 || fd < 0) return -1;
  epoll_event event{}; event.events = events; event.data.fd = fd;
  return ::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &event);
}

int UdpEventLoop::remove(int fd) { return fd_ >= 0 ? ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr) : -1; }

int UdpEventLoop::wait(UdpEvent* output, size_t capacity, int timeout_ms) {
  if (fd_ < 0 || output == nullptr || capacity == 0) return -1;
  const int limit = capacity > static_cast<size_t>(INT_MAX) ? INT_MAX : static_cast<int>(capacity);
  epoll_event events[64];
  const int count = ::epoll_wait(fd_, events, std::min(limit, 64), timeout_ms);
  for (int i = 0; i < count; ++i) { output[i].fd = events[i].data.fd; output[i].events = events[i].events; }
  return count;
}

}  // namespace btch::udp::system
