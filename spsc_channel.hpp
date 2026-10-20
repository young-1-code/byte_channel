#pragma once

#include "channel.hpp"

// SPSC 专用通道：只支持一个生产者和一个消费者。
// 复用 channel.hpp 的共享内存头部布局，但不启动后台线程，也不扫描订阅者。
class SpscPublisher {
private:
    // m_wpos 和 m_rpos 是生产者侧缓存的位置，分别表示已写入和已释放的位置。
    int m_buffer_size = 0;
    int m_mask = 0;
    uint8_t *p_all_buffer = nullptr;
    uint8_t *p_buffer = nullptr;
    IPCHeadInfo *p_head = nullptr;
    MemoryRegion m_memory;
    uint64_t m_wpos = 0;
    uint64_t m_rpos = 0;
    bool m_created = false;

    SubcribeItem *subscriber_item() const
    {
        // SPSC 固定使用第一个订阅槽位，不需要遍历 MAX_SUBCRIBE_NUM。
        return p_head == nullptr ? nullptr : &p_head->sub_item[0];
    }

public:
    ~SpscPublisher() = default;

    int create(int buffer_size, const std::string &topic_name, const MemoryType &type)
    {
        // 环形缓冲区大小必须是 2 的幂，便于使用 mask 计算偏移。
        if (buffer_size <= 0 ||
            (buffer_size & (buffer_size - 1)) != 0 ||
            buffer_size > static_cast<int>(
                std::numeric_limits<int>::max() - sizeof(IPCHeadInfo)) ||
            m_created) {
            return -1;
        }

        // mem head 与数据区连续分配，保持和普通 channel 的布局兼容。
        size_t actual_size = static_cast<size_t>(buffer_size) + sizeof(IPCHeadInfo);
        int result = type == MemoryType::Thread
            ? m_memory.create_thread(actual_size)
            : type == MemoryType::Shared
                ? m_memory.create_shared(topic_name, actual_size)
                : -1;
        if (result != 0) return result == -1 ? -1 : -2;

        p_all_buffer = static_cast<uint8_t *>(m_memory.data());
        p_head = reinterpret_cast<IPCHeadInfo *>(p_all_buffer);
        p_buffer = p_all_buffer + sizeof(IPCHeadInfo);

        channel_atomic::store(&p_head->pub_pos, uint64_t{0});
        channel_atomic::store(&p_head->active_sub_num, uint64_t{0});
        channel_atomic::store(&p_head->curr_ts, get_ns_monotonic());
        p_head->buffer_size = static_cast<uint64_t>(buffer_size);
        p_head->magic_num = MAGIC_HEAD_NUM;
        p_head->version = IPC_HEAD_VERSION;
        p_head->header_size = sizeof(IPCHeadInfo);
        ::strncpy(p_head->topic_name, topic_name.c_str(), sizeof(p_head->topic_name));
        p_head->topic_name[sizeof(p_head->topic_name) - 1] = '\0';

        // 初始化完整订阅槽位，保持共享内存头部格式与 channel.hpp 一致。
        for (auto &item : p_head->sub_item) {
            channel_atomic::store(&item.sub_pos, uint64_t{0}, __ATOMIC_RELAXED);
            channel_atomic::store(&item.curr_ts, uint64_t{0}, __ATOMIC_RELAXED);
            channel_atomic::store(&item.is_active, uint8_t{0}, __ATOMIC_RELAXED);
            item.reverse[0] = '\0';
        }

        m_buffer_size = buffer_size;
        m_mask = buffer_size - 1;
        m_wpos = 0;
        m_rpos = 0;
        m_created = true;
        return 0;
    }

    int publish(const char *data, int len)
    {
        if (!m_created || data == nullptr || len < 1 || len > m_buffer_size) {
            return -1;
        }

        SubcribeItem *item = subscriber_item();
        if (item == nullptr) return -2;

        // 先使用生产者本地缓存；空间不足时再从消费者同步 sub_pos。
        uint64_t available = m_buffer_size - (m_wpos - m_rpos);
        if (available < static_cast<uint64_t>(len)) {
            m_rpos = channel_atomic::load(&item->sub_pos);
            available = m_buffer_size - (m_wpos - m_rpos);
            if (available < static_cast<uint64_t>(len)) return -2;
        }

        // 数据可能跨越环形缓冲区尾部，因此需要拆成两次 memcpy。
        int offset = static_cast<int>(m_wpos & static_cast<uint64_t>(m_mask));
        int right_available = m_buffer_size - offset;
        if (right_available >= len) {
            ::memcpy(p_buffer + offset, data, len);
        } else {
            ::memcpy(p_buffer + offset, data, right_available);
            ::memcpy(p_buffer, data + right_available, len - right_available);
        }

        m_wpos += static_cast<uint64_t>(len);
        channel_atomic::store(&p_head->pub_pos, m_wpos);
        channel_atomic::store(&p_head->curr_ts, get_ns_monotonic());
        return 0;
    }

    void *shared_memory() const { return p_all_buffer; }
    size_t shared_memory_size() const
    {
        return sizeof(IPCHeadInfo) + static_cast<size_t>(m_buffer_size);
    }
};

class SpscSubscriber {
private:
    // 消费者独占 m_rpos；m_wpos 只在本地缓存后从共享头部刷新。
    int m_buffer_size = 0;
    int m_mask = 0;
    uint8_t *p_all_buffer = nullptr;
    uint8_t *p_buffer = nullptr;
    IPCHeadInfo *p_head = nullptr;
    SubcribeItem *item = nullptr;
    MemoryRegion m_memory;
    uint64_t m_wpos = 0;
    uint64_t m_rpos = 0;

    int attach(void *shared_memory)
    {
        if (shared_memory == nullptr || item != nullptr) return -1;

        p_all_buffer = static_cast<uint8_t *>(shared_memory);
        p_head = reinterpret_cast<IPCHeadInfo *>(p_all_buffer);
        if (p_head->magic_num != MAGIC_HEAD_NUM ||
            p_head->version != IPC_HEAD_VERSION ||
            p_head->header_size != sizeof(IPCHeadInfo) ||
            p_head->buffer_size == 0 ||
            p_head->buffer_size > static_cast<uint64_t>(
                std::numeric_limits<int>::max()) ||
            (p_head->buffer_size & (p_head->buffer_size - 1)) != 0) {
            reset();
            return -1;
        }

        // SPSC 不计算 is_active，调用方负责保证只有一个消费者连接。
        SubcribeItem *candidate = &p_head->sub_item[0];
        p_buffer = p_all_buffer + sizeof(IPCHeadInfo);
        m_buffer_size = static_cast<int>(p_head->buffer_size);
        m_mask = m_buffer_size - 1;
        m_rpos = channel_atomic::load(&p_head->pub_pos);
        m_wpos = m_rpos;
        channel_atomic::store(&candidate->sub_pos, m_rpos, __ATOMIC_RELAXED);
        channel_atomic::store(
            &candidate->curr_ts, get_ns_monotonic(), __ATOMIC_RELAXED);
        item = candidate;
        return 0;
    }

    void reset()
    {
        p_all_buffer = nullptr;
        p_buffer = nullptr;
        p_head = nullptr;
        item = nullptr;
        m_buffer_size = 0;
        m_mask = 0;
        m_wpos = 0;
        m_rpos = 0;
    }

public:
    ~SpscSubscriber() { unsubscribe(); }

    int subscribe(const std::string &topic_name)
    {
        int result = m_memory.open_shared(topic_name);
        if (result != 0) return result;
        result = attach(m_memory.data());
        if (result != 0) m_memory = MemoryRegion{};
        return result;
    }

    int subscribe(void *shared_memory) { return attach(shared_memory); }

    int unsubscribe()
    {
        reset();
        return 0;
    }

    int subcribe(char *data, int len)
    {
        if (data == nullptr || len < 1 || len > m_buffer_size || item == nullptr) {
            return -1;
        }

        // curr_ts 仅保留 mem head 的兼容字段；SPSC 不用它判断消费者状态。
        channel_atomic::store(&item->curr_ts, get_ns_monotonic());
        // 没有可读数据时刷新生产者位置，避免每次都访问共享内存。
        uint64_t available = m_wpos - m_rpos;
        if (available < static_cast<uint64_t>(len)) {
            m_wpos = channel_atomic::load(&p_head->pub_pos);
            available = m_wpos - m_rpos;
            if (available < static_cast<uint64_t>(len)) return -2;
        }

        // 数据可能跨越环形缓冲区尾部，需要拆分读取。
        int offset = static_cast<int>(m_rpos & static_cast<uint64_t>(m_mask));
        int right_available = m_buffer_size - offset;
        if (right_available >= len) {
            ::memcpy(data, p_buffer + offset, len);
        } else {
            ::memcpy(data, p_buffer + offset, right_available);
            ::memcpy(data + right_available, p_buffer, len - right_available);
        }

        // 先完成本地消费，再发布新的 sub_pos 给生产者。
        m_rpos += static_cast<uint64_t>(len);
        channel_atomic::store(&item->sub_pos, m_rpos);
        return 0;
    }
};
