#include "udp_endpoint.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

namespace btch::udp::system {

bool parse_ipv4(const std::string& text, sockaddr_in& address, uint16_t port) {
  address = {};
  address.sin_family = AF_INET;
  address.sin_port = htons(port);
  return ::inet_pton(AF_INET, text.c_str(), &address.sin_addr) == 1;
}

int join_multicast(int fd, const std::string& group, const std::string& interface_address) {
  ip_mreq request{};
  if (::inet_pton(AF_INET, group.c_str(), &request.imr_multiaddr) != 1 ||
      ::inet_pton(AF_INET, interface_address.c_str(), &request.imr_interface) != 1) return -1;
  return ::setsockopt(fd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &request, sizeof(request));
}

int set_multicast_interface(int fd, const std::string& interface_address) {
  in_addr address{};
  if (::inet_pton(AF_INET, interface_address.c_str(), &address) != 1) return -1;
  return ::setsockopt(fd, IPPROTO_IP, IP_MULTICAST_IF, &address, sizeof(address));
}

int set_broadcast(int fd, bool enabled) {
  int value = enabled ? 1 : 0;
  return ::setsockopt(fd, SOL_SOCKET, SO_BROADCAST, &value, sizeof(value));
}

}  // namespace btch::udp::system
