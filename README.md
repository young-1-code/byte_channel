# byte_channel

header-only 的 C++17 字节流通道库。基于无锁环形缓冲区提供线程间 / 进程间的高性能单向数据传递，适用于行情、订单等对延迟敏感的字节流分发场景。

## 特性

- **两种通道模型**
  - 通用通道（`Pulisher` / `Subscriber`）：单生产者多消费者，最多 32 个订阅者。内置后台线程跟踪最慢订阅者位置、判活（10 秒超时）并自动剔除失联订阅者
  - SPSC 通道（`SpscPublisher` / `SpscSubscriber`）：单生产者单消费者，无后台线程、不做订阅者扫描，开销更低
- **两种内存模式**：`MemoryType::Thread`（进程内堆内存）与 `MemoryType::Shared`（`/dev/shm` 文件 + `mmap(MAP_SHARED)` 跨进程共享内存，按 topic 定位）
- **无锁实现**：使用 GCC/Clang `__atomic` 内建函数直接操作共享内存中的定宽整数，编译期 `static_assert` 保证无锁
- **两种消费模式**：`try_pop` 拷贝读取；pop-lock 零拷贝读取（`try_pop_lock` 直接返回环形缓冲区内部指针，跨环数据自动整理到内部缓冲区）
- **非阻塞接口**：所有 `try_*` 调用不阻塞，空间 / 数据不足通过返回码通知调用方重试

## 目录结构

```
├── channel.hpp            通用通道：Pulisher / Subscriber，共享内存头部布局 IPCHeadInfo
├── spsc_channel.hpp       SPSC 通道：SpscPublisher / SpscSubscriber
├── mem_region.hpp         MemoryRegion：进程内堆内存 / 共享内存的统一管理与生命周期
├── channel_comm.h         channel_atomic 跨进程原子封装、get_ns_monotonic 时间戳
├── example/               最小使用示例
├── benchmark/             压测程序
```

## 快速开始

```bash
g++ -O2 -std=c++17 -pthread -I. example/channel_example.cpp -o /tmp/channel_example
/tmp/channel_example
```

### 通用通道（进程内，单生产者多消费者）

```cpp
#include "channel.hpp"

Pulisher publisher;
publisher.create(1 << 16, "md_feed", MemoryType::Thread);  // 容量必须是 2 的幂

Subscriber subscriber;
subscriber.subscribe(publisher.shared_memory());          // 进程内直接传内存指针

const char msg[] = "hello channel";
publisher.try_push(msg, sizeof(msg));

char buf[64];
subscriber.try_pop(buf, sizeof(buf));
```

### SPSC 通道（单生产者单消费者）

```cpp
#include "spsc_channel.hpp"

SpscPublisher publisher;
publisher.create(1 << 20, "tick", MemoryType::Thread);

SpscSubscriber subscriber;
subscriber.subscribe(publisher.shared_memory());
```

### 进程间通信（共享内存）

```cpp
// 进程 A：创建共享内存通道，文件位于 /dev/shm/btchanel/md_feed
publisher.create(1 << 20, "md_feed", MemoryType::Shared);

// 进程 B：按 topic 打开同一块共享内存
Subscriber subscriber;
subscriber.subscribe("md_feed");
```

### pop-lock 零拷贝消费

```cpp
subscriber.enable_pop_lock_mode(4096);          // 设定单块最大长度
while (running) {
    if (const char *data = subscriber.try_pop_lock(256)) {
        handle(data, 256);                       // 指针在 pop_unlock 之前有效
        subscriber.pop_unlock();                 // 提交消费位置，释放缓冲区空间
    }
}
```

数据未跨环形缓冲区尾部时 `try_pop_lock` 直接返回缓冲区内部地址（零拷贝）；跨环时复制到内部临时缓冲区后返回。无论哪种情况，处理完成后都必须调用 `pop_unlock()`，生产者的可写空间才会释放。

## 接口一览

生产者（`Pulisher` / `SpscPublisher`）：

| 接口 | 说明 |
| --- | --- |
| `create(buffer_size, topic, type)` | 创建通道，`buffer_size` 必须为 2 的幂 |
| `try_push(data, len)` | 非阻塞写入 |
| `shared_memory()` | 返回底层内存指针，供进程内订阅使用 |
| `get_avialbe_publish_len()`（仅通用通道） | 当前可写字节数 |

消费者（`Subscriber` / `SpscSubscriber`）：

| 接口 | 说明 |
| --- | --- |
| `subscribe(topic)` / `subscribe(ptr)` | 订阅；从订阅时刻的发布位置开始消费，不回放历史数据 |
| `try_pop(data, len)` | 拷贝读取 |
| `enable_pop_lock_mode(max)` | 开启 pop-lock 模式并设定单块上限 |
| `try_pop_lock(len)` / `pop_unlock()` | 零拷贝读取 / 提交消费位置 |
| `unsubscribe()` | 退订（析构时自动调用） |

统一返回码定义在 `channel_comm.h` 的 `channel_status` 命名空间中。适用于 `create`、`try_push`、`try_pop`、`subscribe`、`enable_pop_lock_mode` 和 `pop_unlock`：

| 返回值 | 含义 |
| --- | --- |
| `channel_status::kOk` (`0`) | 成功 |
| `channel_status::kInvalidArgument` (`-1`) | 参数非法或当前状态不允许操作 |
| `channel_status::kWouldBlock` (`-2`) | 暂时不可完成：缓冲区空间不足或数据不足，可稍后重试 |
| `channel_status::kSystemError` (`-3`) | 系统错误或读写位置一致性异常 |
| `channel_status::kNoSubscriber` (`-4`) | 没有可用订阅者槽位 |
| `channel_status::kNotInitialized` (`-5`) | 对象尚未初始化 |
| `channel_status::kAlreadyExists` (`-6`) | 共享内存 topic 已存在 |
| `channel_status::kNoMemory` (`-7`) | 内存分配失败 |

## 约束与注意事项

- 缓冲区容量必须为 2 的幂；单次读写长度不能超过容量；pop-lock 单次长度还须不超过 `enable_pop_lock_mode` 设定的上限
- 两类通道都只支持**一个**生产者；通用通道最多 32 个订阅者
- `try_pop` 与 pop-lock 模式不能混用，存在未 `pop_unlock` 的读取时其他读取调用会失败
- 通用通道生产者后台线程会剔除"阻塞生产且 10 秒无心跳"的订阅者；SPSC 通道不做判活，由调用方保证恰好一个消费者
- 订阅者只消费订阅之后发布的数据，进程重启后需重新订阅
- 仅支持 Linux；共享内存路径默认前缀 `/dev/shm/btchanel/`

## 压测

```bash
# 通用 vs SPSC、copy vs pop_lock，块大小 64 / 256 / 1024 / 4096
g++ -O2 -std=c++17 -pthread -I. benchmark/spsc_pop_lock_benchmark.cpp -o /tmp/pop_lock_bench
/tmp/pop_lock_bench 3 1048576     # 参数：时长（秒）、pop-lock 单块上限

# SPSC 连续压测（周期输出 msg/s 与 MiB/s）
g++ -O2 -std=c++17 -pthread -I. benchmark/channel_benchmark.cpp -o /tmp/channel_bench
/tmp/channel_bench 10 1           # 参数：时长（秒）、报表间隔（秒）

# SPSC 压力测试（含数据完整性校验）
g++ -O2 -std=c++17 -pthread -I. benchmark/spsc_channel_stress.cpp -o /tmp/spsc_stress
/tmp/spsc_stress 10 1

# 进程间共享内存压测：父进程生产、子进程消费
g++ -O2 -std=c++17 -pthread -I. benchmark/ipc_channel_benchmark.cpp -o /tmp/ipc_channel_bench
/tmp/ipc_channel_bench 3             # 参数：时长（秒）
```

输出字段：`interval`、`spsc`（msg/s）、`bandwidth`（MiB/s）、`produced` / `consumed`、重试次数、`status`。

最近一次 3 秒压测中，两种消费模式均通过数据完整性校验；pop_lock 在 64 字节块下吞吐约为普通 `try_pop` 的 2 倍，数据块增大后收益逐步收敛。

## 依赖

- C++17，GCC / Clang（`__atomic` 内建函数）
- Linux（POSIX `mmap`、`/dev/shm`、`clock_gettime`）
- 无第三方依赖，全部实现位于头文件，拷贝 4 个头文件即可集成
