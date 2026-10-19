#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <limits>
#include <new>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>

inline constexpr const char *DEFAULT_SHARED_MEMORY_PREFIX = "/dev/shm/btchanel";

// 内存区域类型。Thread 使用进程内堆内存，Shared 使用文件映射内存。
enum class MemoryType : uint8_t {
    Thread,
    Shared
};

// 统一管理进程内内存和跨进程共享内存。
//
// 共享内存使用 /dev/shm 下的普通文件配合 mmap(MAP_SHARED) 实现，因此
// 不同进程只要使用相同的 prefix 和 topic，就会映射到同一块内存。
class MemoryRegion {
private:
    void *m_address = nullptr;
    size_t m_size = 0;
    int m_fd = -1;
    MemoryType m_type = MemoryType::Thread;
    std::string m_name;
    bool m_owner = false;

    void reset() noexcept
    {
        if (m_address == nullptr) return;
        if (m_type == MemoryType::Shared) {
            ::munmap(m_address, m_size);
            if (m_fd >= 0) ::close(m_fd);
            if (m_owner && !m_name.empty()) ::unlink(m_name.c_str());
        } else {
            ::operator delete(m_address);
        }
        m_address = nullptr;
        m_size = 0;
        m_fd = -1;
        m_name.clear();
        m_owner = false;
    }

    static bool valid_topic(const std::string &topic)
    {
        return !topic.empty() && topic != "." && topic != ".." &&
               topic.find('/') == std::string::npos && topic.find('\\') == std::string::npos;
    }

    static bool valid_prefix(const std::string &prefix)
    {
        return !prefix.empty() && prefix[0] == '/' &&
               prefix.find('\0') == std::string::npos;
    }

    static bool ensure_prefix(const std::string &prefix, mode_t mode)
    {
        for (size_t separator = 1; separator <= prefix.size(); ++separator) {
            if (separator != prefix.size() && prefix[separator] != '/') continue;
            std::string directory = prefix.substr(0, separator);
            if (directory.empty()) continue;
            if (::mkdir(directory.c_str(), mode) == 0) continue;
            if (errno != EEXIST) return false;

            struct stat status {};
            if (::stat(directory.c_str(), &status) != 0 || !S_ISDIR(status.st_mode)) {
                return false;
            }
        }
        return true;
    }

    static std::string make_shared_path(
        const std::string &topic,
        const std::string &prefix)
    {
        if (!valid_topic(topic) || !valid_prefix(prefix)) return {};
        return prefix.back() == '/' ? prefix + topic : prefix + "/" + topic;
    }

public:
    // MemoryRegion 不可拷贝，只能移动，避免多个对象重复释放同一映射。
    MemoryRegion() = default;
    MemoryRegion(const MemoryRegion &) = delete;
    MemoryRegion &operator=(const MemoryRegion &) = delete;

    MemoryRegion(MemoryRegion &&other) noexcept { *this = std::move(other); }

    MemoryRegion &operator=(MemoryRegion &&other) noexcept
    {
        if (this == &other) return *this;
        reset();
        m_address = other.m_address;
        m_size = other.m_size;
        m_fd = other.m_fd;
        m_type = other.m_type;
        m_name = std::move(other.m_name);
        m_owner = other.m_owner;
        other.m_address = nullptr;
        other.m_size = 0;
        other.m_fd = -1;
        other.m_name.clear();
        other.m_owner = false;
        return *this;
    }

    // 析构时解除映射；创建者还会删除共享文件名称。
    ~MemoryRegion() { reset(); }

    // 创建当前进程内使用的堆内存。
    int create_thread(size_t size)
    {
        if (size == 0 || m_address != nullptr) return -1;
        void *address = ::operator new(size, std::nothrow);
        if (address == nullptr) return -2;
        m_address = address;
        m_size = size;
        m_type = MemoryType::Thread;
        return 0;
    }

    // 创建共享文件并映射到当前进程。
    // 返回 -2 表示 topic 已经存在，调用方应改用 open_shared。
    int create_shared(
        const std::string &topic,
        size_t size,
        const std::string &prefix = DEFAULT_SHARED_MEMORY_PREFIX,
        mode_t mode = 0660)
    {
        std::string path = make_shared_path(topic, prefix);
        if (path.empty() || size == 0 || m_address != nullptr ||
            size > static_cast<size_t>(std::numeric_limits<off_t>::max())) return -1;
        if (!ensure_prefix(prefix, 0770)) return -1;

        int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, mode);
        if (fd < 0) return errno == EEXIST ? -2 : -3;
        if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
            ::close(fd);
            ::unlink(path.c_str());
            return -4;
        }
        void *address = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (address == MAP_FAILED) {
            ::close(fd);
            ::unlink(path.c_str());
            return -5;
        }
        m_address = address;
        m_size = size;
        m_fd = fd;
        m_type = MemoryType::Shared;
        m_name = path;
        m_owner = true;
        return 0;
    }

    // 打开已经存在的共享文件并映射到当前进程。
    int open_shared(
        const std::string &topic,
        const std::string &prefix = DEFAULT_SHARED_MEMORY_PREFIX)
    {
        std::string path = make_shared_path(topic, prefix);
        if (path.empty() || m_address != nullptr) return -1;
        int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC, 0);
        if (fd < 0) return -2;
        struct stat status {};
        if (::fstat(fd, &status) != 0 || status.st_size <= 0) {
            ::close(fd);
            return -3;
        }
        size_t size = static_cast<size_t>(status.st_size);
        void *address = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (address == MAP_FAILED) {
            ::close(fd);
            return -4;
        }
        m_address = address;
        m_size = size;
        m_fd = fd;
        m_type = MemoryType::Shared;
        m_name = path;
        return 0;
    }

    void *data() const { return m_address; }
    size_t size() const { return m_size; }
    MemoryType type() const { return m_type; }
    bool valid() const { return m_address != nullptr; }
    bool owner() const { return m_owner; }
    int fd() const { return m_fd; }
    const std::string &name() const { return m_name; }

    // 提前删除共享文件名称；已有 mmap 仍然有效，新的 open 将失败。
    void release_shared_name()
    {
        if (m_type == MemoryType::Shared && m_owner && !m_name.empty()) {
            ::unlink(m_name.c_str());
            m_owner = false;
        }
    }
};
