#pragma once

#include <stdint.h>
#include <ctime>
#include <type_traits>

namespace channel_status {

inline constexpr int kOk = 0;
inline constexpr int kInvalidArgument = -1;
inline constexpr int kWouldBlock = -2;
inline constexpr int kSystemError = -3;
inline constexpr int kNoSubscriber = -4;
inline constexpr int kNotInitialized = -5;
inline constexpr int kAlreadyExists = -6;
inline constexpr int kNoMemory = -7;

}

// 共享内存中的原子字段必须是固定宽度、无对象生命周期的整数类型。
// 这些封装使用 GCC/Clang __atomic builtin，不依赖 std::atomic 对象布局。
namespace channel_atomic {

template <typename T>
using EnableInteger = typename std::enable_if<std::is_integral<T>::value, T>::type;

template <typename T, EnableInteger<T> = 0>
inline T load(const T *value, int memory_order = __ATOMIC_ACQUIRE)
{
    static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
                  "shared integer must use a lock-free atomic operation");
    return __atomic_load_n(value, memory_order);
}

template <typename T, EnableInteger<T> = 0>
inline void store(T *value, T desired, int memory_order = __ATOMIC_RELEASE)
{
    static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
                  "shared integer must use a lock-free atomic operation");
    __atomic_store_n(value, desired, memory_order);
}

template <typename T, EnableInteger<T> = 0>
inline T fetch_add(T *value, T delta, int memory_order = __ATOMIC_ACQ_REL)
{
    static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
                  "shared integer must use a lock-free atomic operation");
    return __atomic_fetch_add(value, delta, memory_order);
}

template <typename T, EnableInteger<T> = 0>
inline T fetch_sub(T *value, T delta, int memory_order = __ATOMIC_ACQ_REL)
{
    static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
                  "shared integer must use a lock-free atomic operation");
    return __atomic_fetch_sub(value, delta, memory_order);
}

template <typename T, EnableInteger<T> = 0>
inline bool compare_exchange(
    T *value,
    T *expected,
    T desired,
    int success_order = __ATOMIC_ACQ_REL,
    int failure_order = __ATOMIC_ACQUIRE)
{
    static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
                  "shared integer must use a lock-free atomic operation");
    return __atomic_compare_exchange_n(
        value, expected, desired, false, success_order, failure_order);
}

template <typename T, EnableInteger<T> = 0>
inline T exchange(T *value, T desired, int memory_order = __ATOMIC_ACQ_REL)
{
    static_assert(__atomic_always_lock_free(sizeof(T), nullptr),
                  "shared integer must use a lock-free atomic operation");
    return __atomic_exchange_n(value, desired, memory_order);
}

}

// 获取时间戳
inline uint64_t get_ns_monotonic() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1000'000'000ULL + ts.tv_nsec;
}
