#include "channel.hpp"

#include <cassert>
#include <cstring>
#include <iostream>

int main()
{
    Pulisher publisher;
    const std::string topic = "channel_example";
    assert(publisher.create(4096, topic, MemoryType::Thread) == 0);

    Subscriber subscriber;
    assert(subscriber.subscribe(publisher.shared_memory()) == 0);

    const char message[] = "hello channel";
    assert(publisher.try_push(message, sizeof(message)) == 0);

    char received[sizeof(message)] = {};
    assert(subscriber.try_pop(received, sizeof(received)) == 0);
    assert(std::memcmp(received, message, sizeof(message)) == 0);

    std::cout << "received: " << received << '\n';
    return 0;
}
