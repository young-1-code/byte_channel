# UDP NAK 压测与示例

## 编译

在仓库根目录执行：

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

UDP 示例和压测目标分别是 `build/udp_nak_example` 和
`build/udp_nak_stress`；其他 byte channel benchmark 也会同时生成。

## 运行

```bash
/tmp/udp_nak_example
/tmp/udp_nak_stress --seconds 10 --payload 1024
/tmp/udp_nak_stress --seconds 60 --payload 1024 --rate 800
/tmp/udp_nak_stress --seconds 10 --payload 1024 --no-crc
```

压测使用本机 UDP 单播，生产线程持续发布固定长度记录，消费线程校验序号和 payload 全部字节，并统计吞吐、背压次数和端到端延迟。`--rate` 按记录/秒限制生产速率，省略或设为 0 表示无限速。压测结束后还会尝试排空最多 5 秒，并输出 `drained=yes/no`；无限速压力下出现 `drained=no` 表示仍有积压，不代表序号已经丢失。`--payload` 是应用记录长度，Pub 会按 UDP `max_payload` 自动分片；默认启用 CRC，`--no-crc` 仅用于对比 CRC 开销。压测程序返回非零表示没有收到数据或检测到序号、payload 异常。

## byte channel 延迟与缺页压测

```bash
g++ -std=c++20 -O2 -Wall -Wextra -Werror -pthread -I. -Icore -Icore/ipc \
  benchmark/latency_benchmark.cpp -o /tmp/latency_bench
/tmp/latency_bench 30
```

`latency_benchmark` 使用 8 MiB Thread SPSC ring，每 1024 条消息采样一次端到端延迟，输出 p50、p99 和 p999。采样结果用于版本对比，不能替代完整 HDR histogram。

共享内存吞吐与 page fault：

```bash
g++ -std=c++20 -O2 -pthread -I. -Icore -Icore/ipc \
  benchmark/ipc_channel_benchmark.cpp -o /tmp/ipc_channel_bench
sudo perf stat -e task-clock,context-switches,cpu-migrations,\
page-faults,minor-faults,major-faults \
  taskset -c 0,1 /tmp/ipc_channel_bench 10
```

建议分别测试 1 MiB、8 MiB ring，以及 64、256、1024、4096 字节消息。比较代码版本时固定 CPU 集合、运行时长、编译参数和系统负载，并完整跑完测试。`dTLB-loads` 和 `dTLB-load-misses` 是否可用取决于 CPU PMU，ARM 平台可能显示 `<not supported>`。

大页检查：

```bash
grep -E 'ShmemPmdMapped|FilePmdMapped|AnonHugePages' /proc/<pid>/smaps
```

`MADV_HUGEPAGE` 是内核提示，THP 是否合并还取决于系统配置和内存碎片；page fault 下降应通过 `perf stat` 验证。
