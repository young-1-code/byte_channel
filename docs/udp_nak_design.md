# UDP NAK 低延迟一对多通信 MVP 设计

## 1. 目标与范围

本方案实现一个基于 UDP 的一对多数据分发系统：一个 Pub 向多个 Sub 发布数据，允许数据丢失后通过 NAK 请求重传，并使用 Sub 的应用消费位置对 Pub 做反向流控。

MVP 的目标是先保证以下性质：

- 单 Pub、多 Sub；
- 首次数据使用 UDP 广播或组播；
- NAK 请求使用 UDP 单播，NAK 对应的数据重传使用 UDP 广播或组播；
- Sub 只向应用提交连续、有序的数据；
- Pub 不覆盖任何活跃 Sub 尚未消费完成的数据；
- 慢 Sub 最终会使 Pub 停止写入，而不是静默丢数据；
- Pub 和 Sub 的网络引擎使用 Linux 系统调用实现。

暂不包含压缩、动态多 Topic 管理、跨节点认证和复杂带宽调度。

UDP 对外接口沿用现有 IPC channel 的调用风格：Pub 提供 `create`、`try_push`、`close` 等生产接口，Sub 提供 `subscribe`、`try_pop`、`try_pop_lock`、`pop_unlock`、`unsubscribe` 等消费接口，并继续使用 `channel_status` 返回码。UDP 网络引擎允许使用 `.cpp` 实现，应用线程不直接操作 socket。

实现使用 C++20。现有 `core/ipc/spsc_channel.hpp` 保持不修改；UDP 侧通过派生适配类增加按 offset 写入、连续提交和远端 `sub_pos` 更新能力。

## 2. 核心容量模型

### 2.1 缓冲区容量

MVP 使用：

```text
Pub retention buffer：8 MiB
Sub local buffer：     8 MiB
可用水位：             8 MiB（由 commit/consumed offset 背压保护）
```

8 MiB 是 2 的幂，Pub 和 Sub 使用相同容量。Pub retention buffer 使用单个 8 MiB 环形缓冲，不采用两个 4 MiB segment。可回收空间由 Sub 的 `consumed_offset` 和连续提交位置直接决定，不额外扣除固定安全水位。

### 2.2 两种 offset

Sub 必须维护两个独立位置：

```text
received_offset：UDP engine 已经按序、连续提交到 Sub buffer 的位置
consumed_offset：应用已经消费完成的位置
```

乱序到达但尚未补齐缺口的数据不计入 `received_offset`；这些数据可以已经写入 buffer，但仍处于不可提交状态。

始终满足：

```text
consumed_offset <= received_offset
```

Sub 发给 Pub 的 `UPDATE.offset` 是 `consumed_offset`，不是 UDP engine 的写入位置。只有应用完成处理后，才允许推进 `consumed_offset`。

Pub 根据所有活跃 Sub 的消费位置计算：

```text
reclaim_offset = min(active_sub[i].consumed_offset)
```

只有位于 `reclaim_offset` 之前的数据才允许被覆盖或回收。

## 3. 总体架构

```text
应用线程
    |
    | push frame
    v
Pub retention buffer
    |
    +-- UDP engine 读取已发布数据
    |       |
    |       +-- 广播/组播 DATA
    |       +-- 广播/组播 NAK 重传
    |       +-- 接收 SETUP / UPDATE / NAK
    |
    +-- 每个 Sub 的 consumed_offset
            |
            +-- reclaim_offset = min(consumed_offset)

Sub UDP engine
    |
    +-- 接收 DATA
    +-- 去重、乱序重排、发送 NAK
    +-- 只提交连续数据到本地 SPSC buffer
    |
Sub 应用线程
    +-- 消费本地 SPSC buffer
    +-- 推进 consumed_offset
    +-- 定期发送 UPDATE
```

本地 SPSC 只用于 `Sub UDP engine -> Sub application`。Sub UDP engine 的接收区需要“按 offset 直写 + 连续提交”能力，不能调用普通 `try_push()`。实现上可由 UDP engine 直接取得 buffer 指针写入纯数据，并在 buffer 外维护 valid/range 元数据；只有 `commit_offset` 推进后，才把对应连续区间发布给 App。Pub 的远端保留区不能直接复用只有一个 `sub_pos` 的 SPSC producer，因为 UDP engine 的读取位置和远端 Sub 的消费确认位置不同。

## 4. Pub 端设计

### 4.1 环形保留区

Pub 使用单个 8 MiB 环形保留区，累计 `write_offset` 和 `reclaim_offset` 使用单调递增的字节 offset，实际内存位置通过 `offset & (8MiB - 1)` 映射。应用生产线程只能写入尚未被活跃 Sub 消费确认的空间；UDP engine 只能读取已经发布的前缀。

环形区允许回绕，但回绕前必须满足：

```text
write_offset - reclaim_offset + frame_size <= PUB_WATERMARK
```

其中 `PUB_WATERMARK` 小于 8 MiB，并预留在途包、乱序窗口和定时确认所需的安全余量。不得因为数据已经发出就回收，只有所有活跃 Sub 的 `consumed_offset` 都超过对应数据范围后才能覆盖。

### 4.2 Pub 写入流控

每次写入前检查：

```text
write_offset - reclaim_offset + frame_size
    <= PUB_WATERMARK
```

如果不满足，返回 `WOULD_BLOCK` 或暂停生产。不能覆盖尚未被最慢 Sub 确认的数据，也不能因为 UDP engine 已经发出数据就回收。

如果某个 Sub 长时间不发送 UPDATE，默认 10 秒判定为失联：

1. 停止把它计入 `reclaim_offset`；
2. 发送 `RST` 或 `EXIT`；
3. 回收它的订阅状态。

具体超时值应配置化。

### 4.3 DATA 包与纯数据 buffer

UDP DATA 包由协议头和 payload 组成：

```text
UDP packet = protocol_header + payload
```

协议头携带：

```text
stream_epoch
packet_seq
data_offset
payload_length
checksum
```

`data_offset` 使用累计字节偏移，范围采用半开区间 `[offset, offset + length)`。单个 UDP payload 建议限制为 1200~1400 字节，避免 IP 分片。

Pub/Sub buffer 只保存 payload 纯数据，不保存任何 Pub/Sub 协议头。`stream_epoch`、`packet_seq`、`data_offset`、`payload_length`、校验和、valid 标志和缺口状态都存在 UDP 包头或 buffer 外的索引/状态表中。接收端解析 UDP 头后，把 payload 直接写入 buffer 的目标 offset；应用从 buffer 读到的始终是纯数据。

## 5. Sub 端设计

### 5.1 接收、乱序直写与连续提交

Sub UDP engine 检测到丢包后，后续到达的 DATA 不需要先复制到独立的重排缓存。它们可以根据 `data_offset` 直接写入本地 8 MiB buffer 的预留位置；但是不能因此推进对应用可见的读边界。

Sub 必须维护两个边界和一个有限的缺口表：

```text
write_reserve_offset：已经为网络数据预留的累计写入位置
commit_offset：        从起点开始连续完整的数据边界
missing_ranges：       commit_offset 之后尚未收到的范围
```

接收处理规则：

```text
收到 commit_offset 对应的数据：
    直接写入 buffer
    从缺口表移除已填范围
    向后扫描已经到达的连续范围
    只把连续末端推进到新的 commit_offset

收到大于 commit_offset 的数据：
    直接写入对应 buffer 位置
    标记该范围已到达
    在 missing_ranges 中记录中间缺口
    不推进 commit_offset
    随机延迟后发送 NAK

收到小于或等于 commit_offset 的数据：
    判定为重复包或迟到重传
    校验后丢弃，不再次提交给应用
```

应用只能消费 `commit_offset` 之前的数据，因此丢包发生后，后续数据虽然已经写入本地 buffer，应用仍然只能消费丢包前的数据，绝不能跳过缺口。

这里的 `commit_offset` 才是本地 SPSC 对应用可见的发布位置，也就是之前定义的 `received_offset`。建议将字段命名为 `committed_offset`，避免把“已经收到但可能乱序”的数据误认为连续可消费数据。

直写需要额外的到达状态，不能直接复用只有单一 `pub_pos` 的 SPSC 写入接口。MVP 可以使用固定大小的 packet/range 元数据表记录 `packet_seq`、`data_offset`、`payload_length` 和 valid 标志。只有 UDP engine 线程会修改这些状态，应用线程只读取已经发布的 `commit_offset`。

写入顺序必须是“先确认位置属于当前接收窗口，再 memcpy payload，最后以 release 语义设置 valid；连续扫描完成后再以 release 语义发布 `commit_offset`”。应用以 acquire 语义读取 `commit_offset`，不得根据 UDP 包到达数量或 `write_reserve_offset` 直接读取数据。

缺口表和直写窗口必须有上限，例如 256/1024 个 packet 或固定字节窗口。超过上限时发送 `RST`，重新建立流，不使用无限缓存。

### 5.2 本地 SPSC

Sub 本地使用 8 MiB SPSC buffer：

```text
生产者：Sub UDP engine
消费者：Sub application
```

当本地 buffer 没有足够空间时，UDP engine 不得推进 `commit_offset`，也不得发送已经写入但未连续提交的数据。应用消费完成后才更新 `consumed_offset`。

### 5.3 UPDATE

UPDATE 携带累计消费位置：

```text
stream_epoch
sub_id
consumed_offset
```

发送条件建议为任一满足即发送：

- 自上次 UPDATE 消费达到 64 KiB；
- 距离上次 UPDATE 超过 5 ms；
- Pub 通过控制消息报告接近水位（可选；Sub 不应自行猜测 Pub 的占用量）。

UPDATE 必须单调递增，重复 UPDATE 可以安全处理。

### 5.4 首次建立与重连

Sub 首次发送 `SETUP` 时不携带恢复位置，Pub 在 `ESTABLISH` 中返回当前最新发布位置；Sub 从该位置开始接收，不回放建立之前的数据。

Sub 重连时允许在 `SETUP` 中携带 `resume_offset`。Pub 必须校验该位置是否仍在保留区可恢复范围内：

- 位置有效且未被回收：`ESTABLISH` 返回该位置，Sub 从该位置继续接收；
- 位置早于 `reclaim_offset`、晚于当前已发布位置或发生整数溢出：拒绝恢复并发送 `RST`，Sub 保持未建立状态；
- 旧 `stream_epoch` 的恢复请求、DATA、NAK 和 UPDATE 全部丢弃。

恢复位置只能从缺口起点或更早位置开始，不能从 Pub 当前最新位置跳过尚未恢复的数据。`resume_offset` 由 Sub 的连续可消费位置维护，不能使用乱序写入位置代替。

## 6. NAK 设计

NAK 通过单播发送给 Pub，包含：

```text
stream_epoch
sub_id
expected_seq 或 cumulative_offset
range_count
range[start_offset, length]...
```

每个 NAK 的 range 数量必须有上限，例如 32。Pub 必须拒绝：

- `stream_epoch` 不匹配；
- offset 已经被回收；
- 请求未来尚未发布的数据；
- range 数量或 payload 长度超限；
- 整数溢出或重叠范围异常。

NAK 采用短随机延迟，用于抑制一对多场景下多个 Sub 对同一缺口同时请求。Pub 对 NAK 请求的重传采用广播/组播，因此其他 Sub 也能收到重传包；Sub 检测到缺口时先记录 `missing_ranges`，启动短随机定时器。定时器到期前如果正常 DATA 或其他 Sub 触发的广播重传已经补齐缺口，则取消本地 NAK。定时器到期仍有缺口时，按当前缺口快照发送 NAK；发送后仍保留缺口记录，直到对应范围实际写入并推进 `commit_offset`。

每个缺口最多发送两次 NAK。两次请求都未恢复时进入 `ABANDONED`，但 MVP 不允许跳过缺口：不得推进 `commit_offset`，不得推进 `consumed_offset`，不得把缺口之后的纯数据交给 App。Sub 应发送 `RST` 或进入重新 SETUP 流程；重新建立时必须从缺口起点或更早的可恢复 offset 开始，不能直接从 Pub 当前最新位置开始，否则会形成隐式丢数据。若 Pub 已经无法提供缺口起点，Sub 必须保持失败/断开状态，不能恢复为可消费状态。旧 epoch 的后续 DATA、NAK 和 UPDATE 全部丢弃。

首次 DATA 和 NAK 对应的重传 DATA 都使用广播或组播。这样多个 Sub 可以共享同一份重传，并对同一缺口进行抑制；Pub 仍然根据各 Sub 独立的 `consumed_offset` 做回收流控。

## 7. 协议字段要求

当前 `core/udp/proto.h` 需要补充或明确以下字段：

```text
magic
protocol_version
header_length
command
topic_id
stream_id
stream_epoch
packet_seq
data_offset
payload_length
sub_id
```

协议枚举必须显式指定数值，不能依赖自动递增。`ESTABLISH` 插入已有枚举中会破坏新旧节点兼容性。

所有整数必须通过显式 encode/decode 转换为网络字节序，不能直接把网络缓冲区强制转换为 packed C++ 结构体。`#pragma pack(1)` 不能替代序列化，也可能导致未对齐访问。

## 8. 控制状态机

### Pub

```text
INIT
  -> RUNNING       收到合法 SETUP 后建立 Sub
  -> RUNNING       发布 DATA、处理 UPDATE/NAK
  -> RESETTING     发生 epoch 切换
  -> STOPPED       发送 EXIT 后退出
```

### Sub

```text
INIT
  -> SETUP_SENT
  -> ESTABLISHED   收到 ESTABLISH
  -> RUNNING       接收、重排、消费、发送 UPDATE
  -> RESETTING     收到 RST 或检测到窗口无法恢复
  -> SETUP_SENT    重新建立
```

SETUP、ESTABLISH、RST、HEARTBEAT、EXIT 必须幂等，重复控制包不能破坏状态。首次 SETUP 与携带 `resume_offset` 的重连 SETUP 使用同一消息类型，由字段是否存在区分。Sub 建立后每秒发送 HEARTBEAT；主动退订发送 EXIT；Pub 超过 10 秒未收到 UPDATE 或 HEARTBEAT 时剔除 Sub 并发送 RST。

## 9. Linux 网络引擎

MVP 每个 Pub/Sub 使用一个网络 engine 线程，负责 UDP 收发和协议状态：

```text
socket
bind
setsockopt(SO_RCVBUF / SO_SNDBUF)
epoll
timerfd
recvmmsg
sendmmsg
```

应用线程不直接操作 UDP socket。Pub 应用线程只写发布 buffer；Sub 应用线程只读本地 SPSC 并推进消费位置。线程之间使用原子 offset 或单向消息队列传递状态。

## 10. MVP 验收标准

1. 单 Sub 正常发送、接收和消费，数据顺序与内容一致。
2. 丢弃任意 DATA 包后，Sub 能通过 NAK 恢复并继续按序消费。
3. 重复 DATA 不会重复提交给应用。
4. 多个 Sub 中最慢者决定 Pub 的回收位置。
5. 应用停止消费后，Pub 在接近 8 MiB 未确认数据并扣除安全余量时停止写入，不覆盖旧数据。
6. Sub 重启或 Pub 重置后，旧 epoch 的 DATA/NAK 都会被丢弃。
7. NAK 超范围、未来 offset、非法长度不会触发异常重传。
8. 单个 8 MiB 环形保留区回绕时，未确认数据不会被覆盖。
9. `UPDATE` 只反映应用已经消费完成的 offset。
10. 在最大 UDP payload 下不产生 IP 分片。
11. 缺口连续两次 NAK 未恢复时，Sub 不跳过数据，发送 RST 并重新建立流。

## 11. 关键结论

Pub 8 MiB、Sub 8 MiB、以应用消费位置作为 UPDATE 的设计可以满足 MVP 的重传和流控需求，但必须满足三个条件：

1. Pub 按最慢 Sub 的 `consumed_offset` 保留数据；
2. UDP engine 的发送位置不能代替远端消费确认位置；
3. 当 8 MiB 中没有可回收空间时必须停写或返回 `WOULD_BLOCK`，不能覆盖旧数据。

## 12. 已确认的实现约束

以下约束已经确定，作为 MVP 的实现基线：

### 12.1 语言、目录和分层边界

- 使用 C++20 实现。
- `core/udp/engine/system/` 只处理 UDP 系统调用，不理解 Pub/Sub 协议状态：socket 创建与关闭、bind、非阻塞设置、组播加入、地址转换、`sendto/recvfrom`、`epoll` 和收发缓冲区配置都放在这里。
- `core/udp/` 保存 UDP 公共抽象：协议头和 payload 编解码、命令与 flags、CRC、公共配置、offset/range 类型和传输端点抽象。
- `core/udp/` 保存 Pub/Sub 控制逻辑：发布保留区、订阅者表、流控、SETUP/ESTABLISH、UPDATE、NAK、RST、乱序直写、连续提交和恢复状态机。Pub/Sub 使用独立实现文件，不额外增加 `impl/` 目录。
- `core/udp/udp_pub.h`、`core/udp/udp_sub.h` 提供公共 API；system 目录不提供业务对象。
- `core/udp/proto.h/.cpp` 只负责协议数据和序列化，不调用 socket，不保存运行状态。
- `core/ipc/channel.hpp` 和 `core/ipc/spsc_channel.hpp` 保持不变。UDP 需要的可读/可写长度、消费位置、按 offset 直写和连续提交能力全部通过 `core/udp/udp_spsc_adapter.h` 中的派生类实现，不把 UDP 协议语义塞进 IPC 通道。
- SPSC buffer 只保存纯数据流，协议头、offset、CRC、缺口和重传状态由 `core/udp` 自己维护。

目标目录：

```text
core/udp/
  proto.h                 # 公共协议类型、命令、flags、payload 类型
  proto.cpp               # 网络字节序、CRC、协议编解码
  udp_pub.h               # UdpPublisher 公共 API 与配置
  udp_sub.h               # UdpSubscriber 公共 API 与配置
  udp_pub.cpp             # Pub 状态机、保留区、发送与控制处理
  udp_sub.cpp             # Sub 状态机、缺口表、NAK 与连续提交
  udp_spsc_adapter.h      # UDP 对 SPSC 的适配，不修改 IPC 源码
  engine/system/
    udp_socket.h          # fd 生命周期、bind、选项、send/recv
    udp_socket.cpp
    udp_endpoint.h        # 地址、组播、数据/控制 socket
    udp_endpoint.cpp
```

对外接口采用：

```cpp
UdpPublisher::create(const UdpPubConfig&)
UdpPublisher::try_push(const char*, int)
UdpPublisher::get_available_publish_len()
UdpPublisher::close()

UdpSubscriber::subscribe(const UdpSubConfig&)
UdpSubscriber::try_pop(char*, int)
UdpSubscriber::try_pop_lock(int)
UdpSubscriber::pop_unlock()
UdpSubscriber::unsubscribe()
```

Pub 和 Sub 的应用线程与 UDP engine 都通过 `SpscPublisher/SpscSubscriber` 解耦。UDP engine 使用 `UdpSpscPublisher/UdpSpscSubscriber` 派生适配类提供 `try_write_at(offset, data, len)`、`commit_until(committed_offset)`、消费位置查询和位置重置；通用 IPC 类不增加 UDP 专用接口。

### 12.2 缓冲区与流控

- Pub retention buffer 固定为 8 MiB。
- Sub 本地 buffer 固定为 8 MiB。
- 两端不允许配置其他容量，也不允许配置成不同容量。
- 不额外设置 7 MiB 安全水位；`commit_pos`、`consumed_offset` 和 SPSC 回收位置直接形成背压。
- `try_push` 允许超过单个 UDP payload 的长度，Pub 自动切分为多个 DATA 包。
- `try_push/try_pop` 保持 IPC channel 的纯字节流语义，不保留调用边界。

### 12.3 网络 socket 与地址模式

Pub/Sub 各使用两个 UDP socket：

```text
data_socket
control_socket
```

- DATA 支持 IPv4 组播和 IPv4 单播，由配置中的 `data_mode` 决定。
- 组播模式下，Sub 将 data socket 绑定到 `0.0.0.0:data_port` 并加入指定组播组；`interface_address` 必填且必须是合法 IPv4 地址。
- 单播模式下，Sub 的 data socket 同样绑定 `0.0.0.0:data_port`；`data_port == 0` 时使用系统分配的临时端口，并在 SETUP 中上报实际 endpoint。
- 单播 DATA 的目标 endpoint 由 Sub 在 SETUP 中上报，Pub 为每个 Sub 独立保存。
- Pub control socket 绑定 `0.0.0.0:control_port`。
- Sub control socket 绑定 `0.0.0.0:0`，由系统分配临时端口。
- 控制消息使用单播：SETUP、UPDATE、NAK、ESTABLISH、RST、HEARTBEAT、EXIT。

默认 `max_payload` 为 1200 字节，可配置到 1400 字节，但必须根据实际 MTU 避免 IPv4 分片。

### 12.4 身份与流生命周期

- `topic_id` 使用固定 FNV-1a 32-bit。
- `sub_id` 为调用方传入的稳定 `uint32_t`，在同一 Pub 流内唯一；相同 `sub_id` 的新 SETUP 视为重连并替换旧 endpoint。
- `stream_id` 在 Pub `create()` 时生成随机非零 `uint32_t`，生命周期内保持不变。
- `stream_epoch` 初始值为 1；每次重置或重新建立流时递增，溢出时跳过 0。
- `packet_seq` 在每个 epoch 内从 0 开始递增。
- 首次 SETUP 不携带恢复位置，ESTABLISH 返回 Pub 当前最新发布位置。
- 重连 SETUP 可以携带 `resume_offset`；Pub 校验可恢复范围，失效时发送 RST。

### 12.5 CRC32 与缺口处理

- CRC32 可配置，默认开启。
- 使用 CRC-32/IEEE 802.3 参数和 256 项预生成查找表。
- 校验范围为“基础头中除 checksum 外的全部字段 + payload”。
- CRC32 关闭时 checksum 为 0。
- 每个缺口最多发送 2 次 NAK。
- 两次 NAK 都未恢复时，Sub 不跳过缺口，不推进 `committed_offset` 或 `consumed_offset`，发送 RST 并从缺口起点或更早位置重新 SETUP；若 Pub 已无法恢复该位置，Sub 保持失败状态。
- 缺口、丢包、CRC 错误和重传状态不通过 `UdpSubscriber` 对外暴露。

## 13. 实施计划

1. **协议层**：在 `core/udp/proto.h/.cpp` 定义固定 command、flags、基础头、各 command payload、显式网络字节序编解码、FNV-1a 和查表 CRC32；这一层不接触 socket。
2. **system 纯 I/O 层**：在 `core/udp/engine/system/` 实现 `UdpSocket`、地址解析、组播加入、非阻塞收发和 epoll 事件封装；这一层不解析 command，不维护 Pub/Sub 状态。
3. **UDP 公共抽象层**：在 `core/udp/` 提供 endpoint、packet、控制消息和传输错误的公共类型，隔离控制逻辑与 system 的具体实现。
4. **SPSC 适配层**：在 `core/udp/udp_spsc_adapter.h` 增加适配类，实现按 offset 写入、连续提交和远端 `sub_pos` 更新；不修改 `core/ipc/spsc_channel.hpp`。
5. **Pub 管理逻辑**：在 `core/udp/udp_pub.cpp` 实现应用 SPSC 输入、8 MiB retention ring、DATA 分片、组播/单播发送、SETUP/UPDATE/NAK 管理和按最慢 Sub 回收。
6. **Sub 管理逻辑**：在 `core/udp/udp_sub.cpp` 实现双 socket、DATA 接收、按 offset 乱序写入、连续提交、NAK 两次重试、失败后 RST 和重连恢复。
7. **验证**：完成 C++20 编译检查、协议单元测试、SPSC 适配测试、loopback 收发、丢包重传、重复包、组播/单播和 ring 回绕测试。

