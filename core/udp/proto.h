#pragma once
#include <stdint.h>

enum class BtchUdpCmdTpye : uint8_t {
    SETUP = 0u,
    ESTABLISH, // pub 同步指针起始位置
    NAK, //- NAK 使用短随机延迟，- 收到相同范围的重传后取消本地 NAK；广播给所有订阅者，sub 已收到的就drop
    UPDATE, // sub 主动更新自己的消费情况
    DATA, //pub 发送数据
    RST, // 双方都可以发
    HEARTBEAT, //不是一直发，是阻塞时候探测
    EXIT, // 双方都可以发
};

// udp head msg
// 结构体数值统一使用网络字节序
#pragma pack(push, 1)
struct BtchUdpHeadPubMsg {
    uint32_t        topic_id; // 字符串topic的hash
    uint32_t        stream_id; //同一端口，不同的流id区分
    uint32_t        version; //版本号
    uint16_t        payload_size; // 数据长度
    uint64_t        offset; // 数据位置
    BtchUdpCmdTpye  cmd;// cmd: data，rst, exit, heartbeat, ESTABLISH
    uint8_t         reverse[3];
};

struct BtchUdpHeadSubMsg {
    uint32_t        topic_id; // 字符串topic的hash
    uint32_t        stream_id; //同一端口，不同的流id区分
    uint32_t        version; //版本号
    uint32_t        sub_id; // 订阅者id
    uint64_t        offset; // 消费位置
    uint16_t        payload_size; //携带数据大小，默认为0，重传时候数据部分[start(u64),len(u32)]:[offset1，len1],[offset3，len3]，这样表示需要重传的数据块
    BtchUdpCmdTpye  cmd;//cmd: setup, nak, update, exit, heartbeat
    uint8_t         reverse;
};
#pragma pack(pop)
