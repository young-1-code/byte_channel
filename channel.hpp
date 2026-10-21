#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <thread>

#include "mem_region.hpp"
#include "channel_comm.h"

#define MAGIC_HEAD_NUM 0xFEEFF55FE55F
#define CACHE_LINE_SIZE 64
#define MAX_SUBCRIBE_NUM 32
#define TIME_OUT (10ULL * 1000 * 1000 * 1000) // 10S
#define IPC_HEAD_VERSION 1U

#define likely() 
#define unlikely() 

struct SubcribeItem
{
    uint64_t sub_pos;
    uint64_t curr_ts; // 计算是否 active，超过 10 秒判断消费者关闭
    uint8_t is_active; // 0=free, 1=registering, 2=active
    char reverse[47];//订阅topic名字
};

struct alignas(CACHE_LINE_SIZE) IPCHeadInfo {
    uint64_t magic_num; //魔幻数
    uint32_t version;
    uint32_t header_size;
    uint64_t buffer_size; //存放数据的缓冲区大小
    uint64_t curr_ts; //计算是否 active，超过 10 秒判断 producer 关闭
    char topic_name[48];//订阅topic名字
    alignas(CACHE_LINE_SIZE) uint64_t active_sub_num; //活跃的订阅者数量
    alignas(CACHE_LINE_SIZE) uint64_t pub_pos; //最慢订阅者消费的位置
    SubcribeItem sub_item[MAX_SUBCRIBE_NUM];
};

class Pulisher {
private:
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> min_sub_pos; //最慢订阅者消费的位置
    int         m_buffer_size;
    int         m_mask;
    uint8_t     *p_all_buffer;
    uint8_t     *p_buffer;
    IPCHeadInfo *p_head;
    MemoryRegion m_memory;
    std::thread _th;
    uint64_t    m_wpos, m_rpos;
    std::atomic<bool> m_stop{false};
    bool m_created = false;
public:
    Pulisher()
        : m_buffer_size(0), m_mask(0), p_all_buffer(nullptr), p_buffer(nullptr),
          p_head(nullptr), m_wpos(0), m_rpos(0) {}

    ~Pulisher()
    {
        m_stop.store(true, std::memory_order_release);
        if (_th.joinable()) _th.join();
    }

    // 线程间通信
    int create(int buffer_size, const std::string &topic_name, const MemoryType &type)
    {
        // 1.判断是否是2的此幂
        if (buffer_size <= 0 ||
            (buffer_size & (buffer_size-1)) != 0 ||
            buffer_size > static_cast<int>(std::numeric_limits<int>::max() - sizeof(IPCHeadInfo))) {
            return -1;
        }
        if (m_created) return -1;
        
        int actual_szie = buffer_size + sizeof(IPCHeadInfo);
        if (type == MemoryType::Thread){
            if (m_memory.create_thread(static_cast<size_t>(actual_szie)) != 0) {
                return -2;
            }
        }else if (type == MemoryType::Shared) {
            if (m_memory.create_shared(topic_name, static_cast<size_t>(actual_szie)) != 0) {
                return -2;
            }
        }else{
            // 非法参数
            return -1;
        }
        p_all_buffer = static_cast<uint8_t *>(m_memory.data());
        
        p_head = (IPCHeadInfo*)p_all_buffer;
        channel_atomic::store(&p_head->pub_pos, uint64_t{0});
        channel_atomic::store(&p_head->active_sub_num, uint64_t{0});
        channel_atomic::store(&p_head->curr_ts, get_ns_monotonic());
        p_head->buffer_size = buffer_size;
        p_head->magic_num = MAGIC_HEAD_NUM;
        p_head->version = IPC_HEAD_VERSION;
        p_head->header_size = sizeof(IPCHeadInfo);
        ::strncpy(p_head->topic_name, topic_name.c_str(), sizeof(p_head->topic_name));
        p_head->topic_name[sizeof(p_head->topic_name) - 1] = '\0';
        for (auto &item : p_head->sub_item) {
            channel_atomic::store(&item.sub_pos, uint64_t{0}, __ATOMIC_RELAXED);
            channel_atomic::store(&item.curr_ts, uint64_t{0}, __ATOMIC_RELAXED);
            channel_atomic::store(&item.is_active, uint8_t{0}, __ATOMIC_RELAXED);
            item.reverse[0] = '\0';
        }
        
        //对象中变量
        min_sub_pos.store(0, std::memory_order_release);
        m_buffer_size = buffer_size;
        m_mask = buffer_size - 1;
        m_wpos = 0;
        m_rpos = 0;
        p_buffer = p_all_buffer + sizeof(IPCHeadInfo);
        m_created = true;

        _th = std::thread([this](){
            int buffer_size = p_head->buffer_size;
            int mask = buffer_size - 1;
            uint64_t wpos = 0;
            while(!m_stop.load(std::memory_order_acquire))
            {
                uint64_t min_pos = 0;
                bool has_active_subscriber = false;
                uint64_t rpos = 0;

                for (int i = 0; i < MAX_SUBCRIBE_NUM; ++i) {
                    auto &item = p_head->sub_item[i];

                    if(channel_atomic::load(&item.is_active) != 2) continue;

                    rpos = channel_atomic::load(&item.sub_pos);
                    wpos = channel_atomic::load(&p_head->pub_pos);

                    // 判断是否阻塞生产，判活
                    if ((wpos - rpos) > static_cast<uint64_t>(mask)) {
                        uint64_t last_seen = channel_atomic::load(&item.curr_ts);
                        uint64_t now = get_ns_monotonic();
                        if (now - last_seen > TIME_OUT &&
                            last_seen == channel_atomic::load(&item.curr_ts)) {
                            uint8_t expected = 2;
                            if (channel_atomic::compare_exchange(
                                    &item.is_active, &expected, uint8_t{0})) {
                                channel_atomic::fetch_sub(
                                    &p_head->active_sub_num, uint64_t{1});
                            }
                            continue;
                        }
                    }

                    if (!has_active_subscriber) {
                        min_pos = rpos;
                        has_active_subscriber = true;
                    } else {
                        min_pos = std::min<uint64_t>(min_pos, rpos);
                    }
                }

                if (!has_active_subscriber) {
                    min_pos = channel_atomic::load(&p_head->pub_pos);
                }
                this->min_sub_pos.store(min_pos, std::memory_order_release);

                // 读写频率不高时候可以休眠下，降低CPU负载
                wpos = channel_atomic::load(&p_head->pub_pos);
                if(has_active_subscriber && (wpos - min_pos)*3 > mask) {
                    std::this_thread::sleep_for(std::chrono::nanoseconds(1));
                }
            }
        });
        return 0;
    }

    int try_push(const char* p_data, int len)
    {
        // 参数校验
        if( len > m_buffer_size || p_data == nullptr || len < 1){
            return -1;
        }
        // 最慢订阅者的可读空间长度
        uint64_t avialbe_wlen = m_wpos - m_rpos;
        if (avialbe_wlen > static_cast<uint64_t>(m_buffer_size)) {
            // 系统错误
            return -3;
        }
        // 可写空间长度
        avialbe_wlen = m_buffer_size - avialbe_wlen;
        if (avialbe_wlen < len) {
            //刷新数据
            m_wpos = channel_atomic::load(&p_head->pub_pos);
            m_rpos = min_sub_pos.load(std::memory_order_acquire);
            //再次计算
            avialbe_wlen = m_wpos - m_rpos;
            if(avialbe_wlen > static_cast<uint64_t>(m_buffer_size)) {
                // 系统错误
                return -3;
            }
            avialbe_wlen = m_buffer_size - avialbe_wlen;
            // 再次判断
            if (avialbe_wlen < len) {
                // 可写入空间不足
                return -2;
            }
        }
        
        int wpos = (m_wpos&m_mask);

        int right_avb_len = m_buffer_size - wpos;

        if (right_avb_len >= len) {
            ::memcpy(p_buffer+wpos, p_data, len);
        }else{
            ::memcpy(p_buffer+wpos, p_data, right_avb_len);
            ::memcpy(p_buffer, p_data+right_avb_len, len-right_avb_len);
        }

        channel_atomic::fetch_add(&p_head->pub_pos, static_cast<uint64_t>(len));
        channel_atomic::store(&p_head->curr_ts, get_ns_monotonic());
        m_wpos += len;

        return 0;
    }

    int get_avialbe_publish_len() const
    {
        uint64_t pub_pos = channel_atomic::load(&p_head->pub_pos);
        uint64_t min_pos = min_sub_pos.load(std::memory_order_acquire);
        uint64_t occupied = pub_pos - min_pos;
        if (occupied > static_cast<uint64_t>(m_buffer_size)) return 0;
        return static_cast<int>(m_buffer_size - occupied);
    }

    void *shared_memory() const { return p_all_buffer; }
    size_t shared_memory_size() const { return sizeof(IPCHeadInfo) + m_buffer_size; }
};

class Subscriber 
{
private:
    int         m_buffer_size;
    int         m_mask;
    uint8_t     *p_all_buffer;
    uint8_t     *p_buffer;
    IPCHeadInfo *p_head;
    SubcribeItem *item;
    uint64_t     m_wpos, m_rpos;
    MemoryRegion m_memory;
    std::unique_ptr<char[]> m_single_max_buffer;
    int         m_single_max_size;
    int         m_last_read_size_lock;
public:
    Subscriber()
        : m_buffer_size(0), m_mask(0), p_all_buffer(nullptr), p_buffer(nullptr),
          p_head(nullptr), item(nullptr), m_wpos(0), m_rpos(0),
          m_single_max_size(0), m_last_read_size_lock(0) {}

    ~Subscriber() { unsubscribe(); }

    // 进程间订阅，共享内存
    int subscribe(const std::string &topic_name)
    {
        m_memory.open_shared(topic_name);
        return subscribe(m_memory.data());
    }

    // 进程内订阅
    int subscribe(void *shared_memory)
    {
        if (item != nullptr) return -1;
        if (shared_memory == nullptr) return -1;

        p_all_buffer = static_cast<uint8_t *>(shared_memory);
        p_head = reinterpret_cast<IPCHeadInfo *>(p_all_buffer);
        if (p_head->magic_num != MAGIC_HEAD_NUM ||
            p_head->version != IPC_HEAD_VERSION ||
            p_head->header_size != sizeof(IPCHeadInfo) ||
            p_head->buffer_size == 0 ||
            p_head->buffer_size > static_cast<uint64_t>(std::numeric_limits<int>::max()) ||
            (p_head->buffer_size & (p_head->buffer_size - 1)) != 0) {
            p_all_buffer = nullptr;
            p_head = nullptr;
            return -1;
        }

        p_buffer = p_all_buffer + sizeof(IPCHeadInfo);
        m_buffer_size = static_cast<int>(p_head->buffer_size);
        m_mask = m_buffer_size - 1;
        uint64_t position = channel_atomic::load(&p_head->pub_pos);
        for (auto &candidate : p_head->sub_item) {
            uint8_t expected = 0;
            if (channel_atomic::compare_exchange(
                    &candidate.is_active, &expected, uint8_t{1})) {
                channel_atomic::store(&candidate.sub_pos, position, __ATOMIC_RELAXED);
                channel_atomic::store(&candidate.curr_ts, get_ns_monotonic(), __ATOMIC_RELAXED);
                channel_atomic::store(&candidate.is_active, uint8_t{2});
                item = &candidate;
                channel_atomic::fetch_add(&p_head->active_sub_num, uint64_t{1});
                break;
            }
        }
        if (item == nullptr) {
            p_all_buffer = nullptr;
            p_buffer = nullptr;
            p_head = nullptr;
            m_buffer_size = 0;
            m_mask = 0;
            return -2;
        }
        m_rpos = channel_atomic::load(&item->sub_pos);
        m_wpos = channel_atomic::load(&p_head->pub_pos);
        return 0;
    }

    int unsubscribe()
    {
        if (item != nullptr) {
            if (channel_atomic::exchange(&item->is_active, uint8_t{0}) == 2) {
                channel_atomic::fetch_sub(&p_head->active_sub_num, uint64_t{1});
            }
        }
        m_single_max_buffer.reset();
        m_single_max_size = 0;
        m_last_read_size_lock = 0;
        item = nullptr;
        p_head = nullptr;
        p_buffer = nullptr;
        p_all_buffer = nullptr;
        m_buffer_size = 0;
        m_mask = 0;
        m_wpos = 0;
        m_rpos = 0;
        return 0;
    }

    int enable_pop_lock_mode(int single_max_block_size)
    {
        if (single_max_block_size < 1 ||
            single_max_block_size > m_buffer_size) {
            return -1;
        }
        if (m_single_max_buffer != nullptr) return -2;

        std::unique_ptr<char[]> buffer(
            new (std::nothrow) char[single_max_block_size]);
        if (buffer == nullptr) return -2;

        m_single_max_size = single_max_block_size;
        m_single_max_buffer = std::move(buffer);
        return 0;
    }

    const char* try_pop_lock(int len)
    {
        if (m_single_max_buffer == nullptr ||
            len < 1 || len > m_single_max_size ||
            len > m_buffer_size || item == nullptr || p_head == nullptr ||
            channel_atomic::load(&item->is_active) != 2 ||
            m_last_read_size_lock > 0) {
            return nullptr;
        }

        channel_atomic::store(&item->curr_ts, get_ns_monotonic());

        uint64_t available = m_wpos - m_rpos;
        if (available < static_cast<uint64_t>(len)) {
            m_wpos = channel_atomic::load(&p_head->pub_pos);
            m_rpos = channel_atomic::load(&item->sub_pos);
            available = m_wpos - m_rpos;
            if (available < static_cast<uint64_t>(len)) return nullptr;
        }

        int rpos = static_cast<int>(m_rpos & static_cast<uint64_t>(m_mask));
        int right_available = m_buffer_size - rpos;
        m_last_read_size_lock = len;
        if (right_available >= len) {
            return reinterpret_cast<const char *>(p_buffer + rpos);
        }

        ::memcpy(m_single_max_buffer.get(), p_buffer + rpos, right_available);
        ::memcpy(
            m_single_max_buffer.get() + right_available,
            p_buffer,
            len - right_available);
        return m_single_max_buffer.get();
    }

    int pop_unlock()
    {
        if (m_last_read_size_lock < 1 || item == nullptr || p_head == nullptr ||
            channel_atomic::load(&item->is_active) != 2) {
            return -1;
        }

        m_rpos += static_cast<uint64_t>(m_last_read_size_lock);
        channel_atomic::store(&item->sub_pos, m_rpos);
        m_last_read_size_lock = 0;
        return 0;
    }

    int try_pop(char* p_data, int len)
    {
        if (m_last_read_size_lock > 0) return -1;

        // 参数校验
        if( len > m_buffer_size || p_data == nullptr || len < 1){
            return -1;
        }

        if (item == nullptr || p_head == nullptr ||
            channel_atomic::load(&item->is_active) != 2) {
            return -3;
        }
        channel_atomic::store(&item->curr_ts, get_ns_monotonic());

        //本地缓存加载计算
        uint64_t avialbe_wlen = m_wpos - m_rpos;
        if (avialbe_wlen < len) {
            // 远端数据更新计算
            m_wpos = channel_atomic::load(&p_head->pub_pos);
            m_rpos = channel_atomic::load(&item->sub_pos);
            // 再次计算
            avialbe_wlen = m_wpos - m_rpos;
            // 再次校验
            if (avialbe_wlen < len){
                // 可写入空间不足
                return -2;
            }
        }

        int rpos = m_rpos & m_mask;

        int right_avb_len = m_buffer_size - rpos;

        if (right_avb_len >= len) {
            ::memcpy(p_data, p_buffer+rpos, len);
        }else{
            ::memcpy(p_data, p_buffer+rpos, right_avb_len);
            ::memcpy(p_data+right_avb_len, p_buffer, len-right_avb_len);
        }

        channel_atomic::fetch_add(&item->sub_pos, static_cast<uint64_t>(len));
        m_rpos += len;

        return 0;
    }

};
