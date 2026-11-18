# 同文件双映射设计

共享通道采用一个文件、两个映射：ring data 位于文件 offset 0，`IPCHeadInfo` 位于文件尾部固定的 4 KiB metadata 区。共享头部只保存文件 offset 和长度，不保存进程指针。

## 文件布局

```text
[data_size bytes ring buffer][4096 bytes metadata]
```

`data_size` 按 2 MiB 向上取整；metadata offset 通过 `fstat()` 得到的文件长度计算：`file_size - 4096`。

## 初始化协议

创建端先 `O_CREAT|O_EXCL`、`ftruncate`，再初始化 metadata 和运行时原子字段，最后以 release 语义写入 `init_state=Ready`。订阅端先映射尾部 metadata，以 acquire 语义等待 `Ready`，校验 magic、版本、文件大小、offset、2 MiB 对齐和 checksum 后再映射 data。

`IPCHeadInfo` 中保存 `data_offset`、`data_size`、`file_size`、`page_mode`、`page_size`、`init_state`；禁止保存裸指针。不同进程的 mmap 地址可能不同，只有文件 offset 对所有进程有效。

## API 方向

`MemoryRegion` 应提供独立的 `metadata()`、`data()`、`metadata_size()`、`data_size()`，并分别 `munmap`。channel 内部直接使用两个地址，不再通过 `p_all_buffer + sizeof(IPCHeadInfo)` 推导 data 地址。进程内订阅也传递包含两个地址的 `SharedMemoryView`。

## 大页与预热

只对 data 映射调用 `madvise(MADV_HUGEPAGE)` 或使用 hugetlbfs；创建端在发布前按页触碰 data，避免业务线程首次访问触发缺页。HugeTLB 不足时回退到 THP/普通页，除非配置要求严格失败。
