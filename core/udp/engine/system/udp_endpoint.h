#pragma once

#include "udp_socket.h"

#include <string>

namespace btch::udp::system {

bool parse_ipv4(const std::string &text, sockaddr_in &address, uint16_t port);
int join_multicast(int fd, const std::string &group,
                   const std::string &interface_address);
int set_multicast_interface(int fd, const std::string &interface_address);
int set_broadcast(int fd, bool enabled);

} // namespace btch::udp::system
