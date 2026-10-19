#pragma once

#include <stdint.h>
#include <atomic>
#include <string>
#include <string.h>
#include <thread>
#include <ctime>
#include <algorithm>
#include <chrono>
#include <limits>

#define MAGIC_HEAD_NUM 0xFEEFF55FE55F
#define CACHE_LINE_SIZE 64
#define MAX_SUBCRIBE_NUM 64
#define TIME_OUT (10ULL * 1000 * 1000 * 1000) // 10S

#define likely() 
#define unlikely() 

struct SubcribeItem
{
    std::atomic<uint64_t> sub_pos;
    std::atomic<uint64_t> curr_ts; //计算是否active，超过10s判断消费者关闭
    std::atomic<uint8_t> is_active; // 0=free, 1=registering, 2=active
    char reverse[47];//订阅topic名字
};

struct alignas(CACHE_LINE_SIZE) IPCHeadInfo {
    uint64_t magic_num; //魔幻数
    uint64_t buffer_size; //存放数据的缓冲区大小
    std::atomic<uint64_t>  curr_ts; //计算是否active，超过10s判断produce关闭
    char topic_name[48];//订阅topic名字
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> active_sub_num; //活跃的订阅者数量
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> pub_pos; //最慢订阅者消费的位置
    SubcribeItem sub_item[MAX_SUBCRIBE_NUM];
};

// 获取时间戳
inline uint64_t get_ns_monotonic() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000'000'000ULL + ts.tv_nsec;
}

class Pulisher {
private:
    alignas(CACHE_LINE_SIZE) std::atomic<uint64_t> min_sub_pos; //最慢订阅者消费的位置
    int         m_buffer_size;
    int         m_mask;
    uint8_t     *p_all_buffer;
    uint8_t     *p_buffer;
    IPCHeadInfo *p_head;
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
        delete[] p_all_buffer;
    }

    // 线程间通信
    int create(int buffer_size, std::string &topic_name)
    {
        // 1.判断是否是2的此幂
        if (buffer_size <= 0 ||
            (buffer_size & (buffer_size-1)) != 0 ||
            buffer_size > static_cast<int>(std::numeric_limits<int>::max() - sizeof(IPCHeadInfo))) {
            return -1;
        }
        if (m_created) return -1;
        
        int actual_szie = buffer_size + sizeof(IPCHeadInfo);
        p_all_buffer = new uint8_t[actual_szie];
        
        p_head = (IPCHeadInfo*)p_all_buffer;
        p_head->pub_pos.store(0, std::memory_order_release);
        p_head->active_sub_num.store(0, std::memory_order_release);
        p_head->curr_ts.store(get_ns_monotonic(), std::memory_order_release);
        p_head->buffer_size = buffer_size;
        p_head->magic_num = MAGIC_HEAD_NUM;
        ::strncpy(p_head->topic_name, topic_name.c_str(), sizeof(p_head->topic_name));
        p_head->topic_name[sizeof(p_head->topic_name) - 1] = '\0';
        for (auto &item : p_head->sub_item) {
            item.sub_pos.store(0, std::memory_order_relaxed);
            item.curr_ts.store(0, std::memory_order_relaxed);
            item.is_active.store(0, std::memory_order_relaxed);
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
                wpos = p_head->pub_pos.load(std::memory_order_acquire);
                if(0 == p_head->active_sub_num.load(std::memory_order_acquire)){
                    min_sub_pos.store(wpos, std::memory_order_release);
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                    continue;
                }

                uint64_t min_pos = buffer_size;
                uint64_t rpos = 0;
                wpos = p_head->pub_pos.load(std::memory_order_acquire);

                for (auto &item : p_head->sub_item) {
                    
                    if(item.is_active.load(std::memory_order_acquire) != 2) continue;

                    rpos = item.sub_pos.load(std::memory_order_acquire);
                    min_pos = std::min<uint64_t>(min_pos, rpos);

                    // 判断是否阻塞生产，判活
                    if( (wpos-rpos) > static_cast<uint64_t>(mask) ) {
                        if( (get_ns_monotonic() - item.curr_ts.load(std::memory_order_acquire)) > TIME_OUT ){
                            item.is_active.store(0, std::memory_order_release);
                            p_head->active_sub_num.fetch_sub(1, std::memory_order_acq_rel);
                        }
                    }
                }
                this->min_sub_pos.store(min_pos, std::memory_order_release);

                // 读写频率不高时候可以休眠下，降低CPU负载
                if( (wpos - min_pos)*3 > mask ) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
        });
        return 0;
    }

    int publish(const char* p_data, int len)
    {
        // 参数校验
        if( len > m_buffer_size || p_data == nullptr || len < 1){
            return -1;
        }
        // 最慢订阅者的可读空间长度
        uint64_t avialbe_wlen = m_wpos - m_rpos;
        if(avialbe_wlen > m_buffer_size) {
            // 系统错误
            return -3;
        }
        // 可写空间长度
        avialbe_wlen = m_buffer_size - avialbe_wlen;
        if (avialbe_wlen < len) {
            //刷新数据
            m_wpos = p_head->pub_pos.load(std::memory_order_acquire);
            m_rpos = min_sub_pos.load(std::memory_order_acquire);
            //再次计算
            avialbe_wlen = m_wpos - m_rpos;
            if(avialbe_wlen > m_buffer_size) {
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

        p_head->pub_pos.fetch_add(len, std::memory_order_release);
        p_head->curr_ts.store(get_ns_monotonic(), std::memory_order_release);
        m_wpos += len;

        return 0;
    }

    int get_avialbe_publish_len() const
    {
        uint64_t occupied = p_head->pub_pos.load(std::memory_order_acquire) -
                            min_sub_pos.load(std::memory_order_acquire);
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
public:
    Subscriber()
        : m_buffer_size(0), m_mask(0), p_all_buffer(nullptr), p_buffer(nullptr),
          p_head(nullptr), item(nullptr), m_wpos(0), m_rpos(0) {}

    ~Subscriber() { unsubscribe(); }

    int subscribe(void *shared_memory)
    {
        if (item != nullptr) return -1;
        if (shared_memory == nullptr) return -1;

        p_all_buffer = static_cast<uint8_t *>(shared_memory);
        p_head = reinterpret_cast<IPCHeadInfo *>(p_all_buffer);
        if (p_head->magic_num != MAGIC_HEAD_NUM ||
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
        uint64_t position = p_head->pub_pos.load(std::memory_order_acquire);
        for (auto &candidate : p_head->sub_item) {
            uint8_t expected = 0;
            if (candidate.is_active.compare_exchange_strong(
                    expected, 1, std::memory_order_acq_rel)) {
                candidate.sub_pos.store(position, std::memory_order_relaxed);
                candidate.curr_ts.store(get_ns_monotonic(), std::memory_order_relaxed);
                candidate.is_active.store(2, std::memory_order_release);
                item = &candidate;
                p_head->active_sub_num.fetch_add(1, std::memory_order_acq_rel);
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
        m_rpos = item->sub_pos.load(std::memory_order_acquire);
        m_wpos = p_head->pub_pos.load(std::memory_order_acquire);
        return 0;
    }

    int unsubscribe()
    {
        if (item == nullptr) return 0;
        if (item->is_active.exchange(0, std::memory_order_acq_rel) == 2) {
            p_head->active_sub_num.fetch_sub(1, std::memory_order_acq_rel);
        }
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

    int subcribe(char* p_data, int len)
    {
        // 参数校验
        if( len > m_buffer_size || p_data == nullptr || len < 1){
            return -1;
        }

        if (item == nullptr || p_head == nullptr ||
            item->is_active.load(std::memory_order_acquire) != 2) {
            return -3;
        }
        item->curr_ts.store(get_ns_monotonic(), std::memory_order_release);

        //本地缓存加载计算
        uint64_t avialbe_wlen = m_wpos - m_rpos;
        if (avialbe_wlen < len) {
            // 远端数据更新计算
            m_wpos = p_head->pub_pos.load(std::memory_order_acquire);
            m_rpos = item->sub_pos.load(std::memory_order_acquire);
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

        item->sub_pos.fetch_add(len, std::memory_order_release);
        m_rpos += len;

        return 0;
    }

};

int test(){
   auto v0 =  sizeof(IPCHeadInfo);
   auto v1 =  sizeof(IPCHeadInfo);
}
