#include "core/udp/udp_pub.h"
#include "core/udp/udp_sub.h"

#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <thread>

namespace {

uint64_t now_ns() {
  return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                                  std::chrono::steady_clock::now().time_since_epoch())
                                  .count());
}

}  // namespace

int main() {
  UdpPubConfig pub_config;
  pub_config.topic = "udp-example";
  pub_config.data_address = "127.0.0.1";
  pub_config.data_port = 26000;
  pub_config.control_port = 26001;
  pub_config.data_mode = UdpDataMode::kUnicast;

  UdpSubConfig sub_config;
  sub_config.topic = pub_config.topic;
  sub_config.data_address = "127.0.0.1";
  sub_config.pub_address = "127.0.0.1";
  sub_config.data_port = 26002;
  sub_config.control_port = pub_config.control_port;
  sub_config.sub_id = 1;
  sub_config.data_mode = UdpDataMode::kUnicast;

  UdpPublisher publisher;
  UdpSubscriber subscriber;
  if (publisher.create(pub_config) != channel_status::kOk ||
      subscriber.subscribe(sub_config) != channel_status::kOk) {
    std::cerr << "failed to start UDP publisher/subscriber\n";
    return 1;
  }

  // 等待 SETUP/ESTABLISH 完成，避免示例的第一条消息在订阅建立前发送。
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  const char message[] = "hello from UDP NAK";
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  bool received = false;
  while (std::chrono::steady_clock::now() < deadline) {
    if (publisher.try_push(message, static_cast<int>(sizeof(message))) == channel_status::kOk) {
      char output[sizeof(message)]{};
      while (std::chrono::steady_clock::now() < deadline) {
        if (subscriber.try_pop(output, static_cast<int>(sizeof(output))) == channel_status::kOk) {
          std::cout << "received: " << output << " (" << now_ns() << " ns)\n";
          received = std::memcmp(output, message, sizeof(message)) == 0;
          break;
        }
        std::this_thread::yield();
      }
      break;
    }
    std::this_thread::yield();
  }

  subscriber.unsubscribe();
  publisher.close();
  if (!received) {
    std::cerr << "message was not received\n";
    return 1;
  }
  return 0;
}
