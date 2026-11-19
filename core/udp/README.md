# UDP NAK MVP

UDP 的系统调用层位于 `engine/system/`，只负责 socket、地址、组播和 epoll；Pub/Sub 状态机位于当前目录。

编译 UDP 示例和压测：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --target udp_nak_example udp_nak_stress -j
```

Pub 配置需要提供 `topic`、数据地址和数据端口，以及 Pub control 端口。Sub 配置需要提供相同的 `topic`、Pub 地址、Pub control 端口、`sub_id` 和数据接收配置。组播模式要求 Sub 指定 `interface_address` 并加入同一个组播地址；单播模式下 Sub 可以将 `data_port` 设为 0，系统分配的端口会通过 SETUP 上报给 Pub。

应用只读写纯 payload。协议头、offset、packet sequence、CRC、缺口和 NAK 状态都不进入 SPSC buffer。

`max_payload` 默认 1200 字节，允许配置到 1400 字节；使用时必须结合链路 MTU 避免 IPv4 分片。

Sub 建立后每秒发送 HEARTBEAT，主动退订发送 EXIT；Pub 超过 10 秒未收到 UPDATE 或 HEARTBEAT 会回收该订阅者并发送 RST。

Sub 在收到乱序 DATA 时按 `data_offset` 直写 buffer，但只有连续区间才会发布给应用。应用消费位置通过 UPDATE 发送给 Pub，Pub 以最慢活跃 Sub 的消费位置回收 retention。缺口最多请求两次 NAK；恢复失败后发送 RST 并从缺口位置重新 SETUP，MVP 不允许跳过数据。

单播 Sub 会校验 DATA 包来源地址、topic、stream 和 epoch；组播模式不校验发送方地址。收到的乱序区间有数量上限，超过上限会触发 RST，避免异常流量无限增长内存。网络线程与应用消费线程之间的消费游标重置通过互斥保护，重连时不会并发修改 `try_pop` 的内部状态。
