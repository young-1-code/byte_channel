# UDP NAK 压测与示例

## 编译

在仓库根目录执行：

```bash
g++ -std=c++20 -O2 -Wall -Wextra -Werror -Wno-stringop-truncation -pthread -I. -Icore \
  example/udp_nak_example.cpp \
  core/udp/proto.cpp core/udp/engine/system/udp_socket.cpp \
  core/udp/engine/system/udp_endpoint.cpp core/udp/engine/system/udp_event_loop.cpp \
  core/udp/udp_pub.cpp core/udp/udp_sub.cpp -o /tmp/udp_nak_example

g++ -std=c++20 -O2 -Wall -Wextra -Werror -Wno-stringop-truncation -pthread -I. -Icore \
  benchmark/udp_nak_stress.cpp \
  core/udp/proto.cpp core/udp/engine/system/udp_socket.cpp \
  core/udp/engine/system/udp_endpoint.cpp core/udp/engine/system/udp_event_loop.cpp \
  core/udp/udp_pub.cpp core/udp/udp_sub.cpp -o /tmp/udp_nak_stress
```

## 运行

```bash
/tmp/udp_nak_example
/tmp/udp_nak_stress --seconds 10 --payload 1024
/tmp/udp_nak_stress --seconds 60 --payload 1024 --rate 800
/tmp/udp_nak_stress --seconds 10 --payload 1024 --no-crc
```

压测使用本机 UDP 单播，生产线程持续发布固定长度记录，消费线程校验序号并统计吞吐、背压次数和端到端延迟。`--rate` 按记录/秒限制生产速率，省略或设为 0 表示无限速。压测结束后还会尝试排空最多 5 秒，并输出 `drained=yes/no`；无限速压力下出现 `drained=no` 表示仍有积压，不代表序号已经丢失。`--payload` 是应用记录长度，Pub 会按 UDP `max_payload` 自动分片；`--no-crc` 用于对比 CRC 开销。压测程序返回非零表示没有收到数据或检测到序号异常。
