#pragma once

#include <stdint.h>

enum class BtchUdpCmdTpye : uint8_t {
    SETUP = 0u, 
    NAK,
    UPDATE,
    DATA,
    RST, 
    HEARTBEAT, //不是一直发，是阻塞时候探测
    EXIT,
};

// udp head msg
#pragma pack(push, 1)
struct BtchUdpHeadPubMsg {
    uint32_t        version; //版本号
    uint32_t        payload_size; // 数据长度
    uint64_t        data_seq; // 数据序号
    BtchUdpCmdTpye  cmd;// cmd: data，rst, exit, heartbeat
    uint8_t         reverse[3];
};

struct BtchUdpHeadSubMsg {
    uint32_t        version; //版本号
    uint32_t        sub_id; // 订阅者id
    uint64_t        offset; // 消费位置
    uint16_t        payload_size; //携带数据大小，默认为0
    BtchUdpCmdTpye  cmd;//cmd: setup, nak, update, exit, heartbeat
    uint8_t         reverse;
};
#pragma pack(pop)