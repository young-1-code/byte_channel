#include "channel.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

int main()
{
    Pulisher publisher;
    const std::string topic = "channel_example";
    if (publisher.create(4096, topic, MemoryType::Thread) != 0) return 1;

    Subscriber subscriber;
    const SharedMemoryView view = publisher.shared_memory();
    if (subscriber.subscribe(view) != 0) return 1;

    const char message[] = "hello channel";
    if (publisher.try_push(message, sizeof(message)) != 0) return 1;

    char received[sizeof(message)] = {};
    if (subscriber.try_pop(received, sizeof(received)) != 0 ||
        std::memcmp(received, message, sizeof(message)) != 0)
        return 1;

    std::cout << "received: " << received << '\n';
    return 0;
}
