#include "udp_socket.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>

namespace btch::udp::system {

UdpSocket::~UdpSocket() { close(); }

int UdpSocket::open(const UdpSocketConfig &config) {
  if (fd_ >= 0)
    return -1;
  fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (fd_ < 0)
    return -1;
  int one = 1;
  if (config.reuse_address)
    ::setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
  if (config.receive_buffer > 0)
    ::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &config.receive_buffer,
                 sizeof(config.receive_buffer));
  if (config.send_buffer > 0)
    ::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &config.send_buffer,
                 sizeof(config.send_buffer));
  sockaddr_in address{};
  address.sin_family = AF_INET;
  if (::inet_pton(AF_INET, config.bind_address.c_str(), &address.sin_addr) !=
      1) {
    close();
    return -1;
  }
  address.sin_port = htons(config.port);
  if (::bind(fd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) <
      0) {
    close();
    return -1;
  }
  if (config.non_blocking) {
    const int flags = ::fcntl(fd_, F_GETFL, 0);
    if (flags < 0 || ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) < 0) {
      close();
      return -1;
    }
  }
  return 0;
}

int UdpSocket::close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
  return 0;
}

uint16_t UdpSocket::local_port() const {
  if (fd_ < 0)
    return 0;
  sockaddr_in address{};
  socklen_t length = sizeof(address);
  if (::getsockname(fd_, reinterpret_cast<sockaddr *>(&address), &length) < 0)
    return 0;
  return ntohs(address.sin_port);
}

int UdpSocket::send_to(const sockaddr_in &peer, const void *data,
                       size_t length) const {
  if (fd_ < 0 || !data)
    return -1;
  const ssize_t result =
      ::sendto(fd_, data, length, 0, reinterpret_cast<const sockaddr *>(&peer),
               sizeof(peer));
  return result == static_cast<ssize_t>(length) ? 0
                                                : (result < 0 ? -errno : -1);
}

int UdpSocket::receive(void *data, size_t capacity, sockaddr_in *peer) const {
  if (fd_ < 0 || !data || !peer)
    return -1;
  socklen_t length = sizeof(*peer);
  const ssize_t result = ::recvfrom(
      fd_, data, capacity, 0, reinterpret_cast<sockaddr *>(peer), &length);
  return result >= 0 ? static_cast<int>(result) : -errno;
}

} // namespace btch::udp::system
