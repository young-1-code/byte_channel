# Byte Channel 项目记忆

## 通道接口

- 生产者统一使用 `try_push(data, len)`。
- 普通消费者统一使用 `try_pop(data, len)`。
- `SpscSubscriber` 和通用 `Subscriber` 都支持 pop-lock 模式：
  1. 订阅成功后调用 `enable_pop_lock_mode(max_block_size)`。
  2. 使用 `try_pop_lock(len)` 获取数据指针。
  3. 数据处理完成后必须调用 `pop_unlock()` 提交消费位置。
- `try_pop()` 与 pop-lock 模式不能交叉使用；存在未释放的 pop-lock 时，普通读取会失败。
- `try_pop_lock()` 返回的连续数据指针直接指向环形缓冲区；跨环数据会复制到内部临时缓冲区。无论数据是否跨环，都必须调用 `pop_unlock()`。
- `pop_unlock()` 成功后才推进订阅者的 `sub_pos`，释放生产者可复用的缓冲区空间。

## 内存管理

- pop-lock 的跨环临时缓冲区使用 `std::unique_ptr<char[]>` 管理。
- 分配使用 `std::nothrow`，失败通过错误码返回。
- 取消订阅时释放临时缓冲区并清理 pending lock 状态。

## 压测

压测程序：`benchmark/spsc_pop_lock_benchmark.cpp`

```bash
g++ -O2 -std=c++17 -pthread -I. \
  benchmark/spsc_pop_lock_benchmark.cpp \
  -o /tmp/spsc_pop_lock_benchmark
/tmp/spsc_pop_lock_benchmark 3 4096
```

默认按 `64 / 256 / 1024 / 4096` 字节递增测试普通 `try_pop` 和 `pop_lock`，输出字段与现有通道压测保持一致：`interval`、`spsc`、`bandwidth`、`produced`、`consumed`、重试次数和状态。

最近一次 3 秒压测中，两种模式均通过数据完整性校验；`pop_lock` 相对普通复制模式在 64 字节数据块下约提升 2 倍，数据块增大后收益逐步收敛。
