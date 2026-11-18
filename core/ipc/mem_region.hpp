#pragma once

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <fcntl.h>
#include <limits>
#include <new>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <utility>
#include <chrono>
#include <thread>

#include "channel_comm.h"

inline constexpr const char *DEFAULT_SHARED_MEMORY_PREFIX = "/dev/shm/btchanel";
inline constexpr size_t SHARED_METADATA_SIZE = 4096;
inline constexpr size_t SHARED_HUGE_PAGE_SIZE = 2 * 1024 * 1024;

inline size_t align_up_size(size_t value, size_t alignment) {
  return value > std::numeric_limits<size_t>::max() - (alignment - 1)
             ? 0
             : (value + alignment - 1) / alignment * alignment;
}

inline void prefault_mapping(void *address, size_t size) {
  volatile unsigned char *bytes = static_cast<volatile unsigned char *>(address);
  for (size_t offset = 0; offset < size; offset += 4096)
    bytes[offset] = 0;
  if (size != 0) bytes[size - 1] = 0;
}

// Shared IPC normally uses /dev/shm on Linux. Some platforms (including
// macOS) do not provide that mount, but regular files still support the
// MAP_SHARED contract used by this library. Keep the default for existing
// deployments and allow process launchers to select a writable absolute path.
inline std::string shared_memory_prefix() {
  const char *configured = ::getenv("LUMIN_SHARED_MEMORY_PREFIX");
  if (configured != nullptr && configured[0] != '\0')
    return std::string(configured);
  return std::string(DEFAULT_SHARED_MEMORY_PREFIX);
}

// 内存区域类型。Thread 使用进程内堆内存，Shared 使用文件映射内存。
enum class MemoryType : uint8_t { Thread, Shared };

// A shared channel is represented by two mappings.  Raw pointers must never
// be stored in IPC metadata because each process may map the file at a
// different virtual address.
struct SharedMemoryView {
  void *metadata = nullptr;
  void *data = nullptr;
  size_t metadata_size = 0;
  size_t data_size = 0;
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
  void *m_metadata = nullptr;
  size_t m_metadata_size = 0;

  void reset() noexcept {
    if (m_address == nullptr)
      return;
    if (m_type == MemoryType::Shared) {
      if (m_metadata != nullptr && m_metadata != m_address)
        ::munmap(m_metadata, m_metadata_size);
      ::munmap(m_address, m_size);
      if (m_fd >= 0)
        ::close(m_fd);
      if (m_owner && !m_name.empty())
        ::unlink(m_name.c_str());
    } else {
      if (m_metadata != nullptr && m_metadata != m_address)
        ::operator delete(m_metadata);
      ::operator delete(m_address);
    }
    m_address = nullptr;
    m_size = 0;
    m_fd = -1;
    m_name.clear();
    m_owner = false;
    m_metadata = nullptr;
    m_metadata_size = 0;
  }

  static bool valid_topic(const std::string &topic) {
    return !topic.empty() && topic != "." && topic != ".." &&
           topic.find('/') == std::string::npos &&
           topic.find('\\') == std::string::npos;
  }

  static bool valid_prefix(const std::string &prefix) {
    return !prefix.empty() && prefix[0] == '/' &&
           prefix.find('\0') == std::string::npos;
  }

  static bool ensure_prefix(const std::string &prefix, mode_t mode) {
    for (size_t separator = 1; separator <= prefix.size(); ++separator) {
      if (separator != prefix.size() && prefix[separator] != '/')
        continue;
      std::string directory = prefix.substr(0, separator);
      if (directory.empty())
        continue;
      if (::mkdir(directory.c_str(), mode) == 0)
        continue;
      if (errno != EEXIST)
        return false;

      struct stat status {};
      if (::stat(directory.c_str(), &status) != 0 || !S_ISDIR(status.st_mode)) {
        return false;
      }
    }
    return true;
  }

  static std::string make_shared_path(const std::string &topic,
                                      const std::string &prefix) {
    if (!valid_topic(topic) || !valid_prefix(prefix))
      return {};
    return prefix.back() == '/' ? prefix + topic : prefix + "/" + topic;
  }

public:
  // MemoryRegion 不可拷贝，只能移动，避免多个对象重复释放同一映射。
  MemoryRegion() = default;
  MemoryRegion(const MemoryRegion &) = delete;
  MemoryRegion &operator=(const MemoryRegion &) = delete;

  MemoryRegion(MemoryRegion &&other) noexcept { *this = std::move(other); }

  MemoryRegion &operator=(MemoryRegion &&other) noexcept {
    if (this == &other)
      return *this;
    reset();
    m_address = other.m_address;
    m_size = other.m_size;
    m_fd = other.m_fd;
    m_type = other.m_type;
    m_name = std::move(other.m_name);
    m_owner = other.m_owner;
    m_metadata = other.m_metadata;
    m_metadata_size = other.m_metadata_size;
    other.m_address = nullptr;
    other.m_size = 0;
    other.m_fd = -1;
    other.m_name.clear();
    other.m_owner = false;
    other.m_metadata = nullptr;
    other.m_metadata_size = 0;
    return *this;
  }

  // 析构时解除映射；创建者还会删除共享文件名称。
  ~MemoryRegion() { reset(); }

  // 创建当前进程内使用的堆内存。
  int create_thread(size_t size) {
    if (size == 0 || m_address != nullptr)
      return channel_status::kInvalidArgument;
    void *address = ::operator new(size, std::nothrow);
    if (address == nullptr)
      return channel_status::kNoMemory;
    m_address = address;
    m_size = size;
    m_type = MemoryType::Thread;
    return channel_status::kOk;
  }

  int create_thread_split(size_t data_size, size_t metadata_size) {
    if (data_size == 0 || metadata_size == 0 || m_address != nullptr)
      return channel_status::kInvalidArgument;
    void *data = ::operator new(data_size, std::nothrow);
    void *metadata = ::operator new(metadata_size, std::nothrow);
    if (data == nullptr || metadata == nullptr) {
      ::operator delete(data); ::operator delete(metadata);
      return channel_status::kNoMemory;
    }
    m_address = data; m_size = data_size; m_metadata = metadata;
    m_metadata_size = metadata_size; m_type = MemoryType::Thread;
    prefault_mapping(data, data_size);
    prefault_mapping(metadata, metadata_size);
    return channel_status::kOk;
  }

  // 创建共享文件并映射到当前进程。
  // 返回 kAlreadyExists 表示 topic 已经存在，调用方应改用 open_shared。
  int create_shared(const std::string &topic, size_t size,
                    const std::string &prefix = shared_memory_prefix(),
                    mode_t mode = 0660) {
    std::string path = make_shared_path(topic, prefix);
    if (path.empty() || size == 0 || m_address != nullptr ||
        size > static_cast<size_t>(std::numeric_limits<off_t>::max())) {
      return channel_status::kInvalidArgument;
    }
    if (!ensure_prefix(prefix, 0770))
      return channel_status::kSystemError;

    int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, mode);
    if (fd < 0) {
      return errno == EEXIST ? channel_status::kAlreadyExists
                             : channel_status::kSystemError;
    }
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
      ::close(fd);
      ::unlink(path.c_str());
      return channel_status::kSystemError;
    }
    void *address =
        ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (address == MAP_FAILED) {
      ::close(fd);
      ::unlink(path.c_str());
      return channel_status::kSystemError;
    }
    m_address = address;
    m_size = size;
    m_fd = fd;
    m_type = MemoryType::Shared;
    m_name = path;
    m_owner = true;
    return channel_status::kOk;
  }

  // Create a split mapping: data starts at file offset zero and metadata is
  // mapped from the fixed-size tail of the same file.
  int create_shared_split(const std::string &topic, size_t data_size,
                          const std::string &prefix = shared_memory_prefix(),
                          mode_t mode = 0660) {
    std::string path = make_shared_path(topic, prefix);
    if (path.empty() || data_size == 0 || m_address != nullptr ||
        data_size > static_cast<size_t>(std::numeric_limits<off_t>::max()) -
                        SHARED_METADATA_SIZE)
      return channel_status::kInvalidArgument;
    if (!ensure_prefix(prefix, 0770)) return channel_status::kSystemError;
    const size_t mapped_data_size =
        align_up_size(data_size, SHARED_HUGE_PAGE_SIZE);
    if (mapped_data_size == 0 ||
        mapped_data_size > static_cast<size_t>(std::numeric_limits<off_t>::max()) -
                               SHARED_METADATA_SIZE) {
      return channel_status::kInvalidArgument;
    }
    const size_t file_size = mapped_data_size + SHARED_METADATA_SIZE;
    int fd = ::open(path.c_str(), O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC, mode);
    if (fd < 0) return errno == EEXIST ? channel_status::kAlreadyExists
                                        : channel_status::kSystemError;
    if (::ftruncate(fd, static_cast<off_t>(file_size)) != 0) {
      ::close(fd); ::unlink(path.c_str()); return channel_status::kSystemError;
    }
    void *data = ::mmap(nullptr, mapped_data_size, PROT_READ | PROT_WRITE,
                        MAP_SHARED, fd, 0);
    void *metadata = ::mmap(nullptr, SHARED_METADATA_SIZE,
                            PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                            static_cast<off_t>(mapped_data_size));
    if (data == MAP_FAILED || metadata == MAP_FAILED) {
      if (data != MAP_FAILED) ::munmap(data, mapped_data_size);
      if (metadata != MAP_FAILED) ::munmap(metadata, SHARED_METADATA_SIZE);
      ::close(fd); ::unlink(path.c_str()); return channel_status::kSystemError;
    }
    m_address = data; m_size = mapped_data_size; m_metadata = metadata;
    m_metadata_size = SHARED_METADATA_SIZE; m_fd = fd; m_type = MemoryType::Shared;
    m_name = path; m_owner = true;
    ::madvise(data, data_size, MADV_HUGEPAGE);
    prefault_mapping(data, mapped_data_size);
    return channel_status::kOk;
  }

  // 打开已经存在的共享文件并映射到当前进程。
  int open_shared(const std::string &topic,
                  const std::string &prefix = shared_memory_prefix()) {
    std::string path = make_shared_path(topic, prefix);
    if (path.empty() || m_address != nullptr) {
      return channel_status::kInvalidArgument;
    }
    int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC, 0);
    if (fd < 0)
      return channel_status::kSystemError;
    struct stat status {};
    if (::fstat(fd, &status) != 0 || status.st_size <= 0) {
      ::close(fd);
      return channel_status::kSystemError;
    }
    size_t size = static_cast<size_t>(status.st_size);
    void *address =
        ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (address == MAP_FAILED) {
      ::close(fd);
      return channel_status::kSystemError;
    }
    m_address = address;
    m_size = size;
    m_fd = fd;
    m_type = MemoryType::Shared;
    m_name = path;
    return channel_status::kOk;
  }

  int open_shared_split(const std::string &topic,
                        const std::string &prefix = shared_memory_prefix()) {
    std::string path = make_shared_path(topic, prefix);
    if (path.empty() || m_address != nullptr) return channel_status::kInvalidArgument;
    int fd = ::open(path.c_str(), O_RDWR | O_CLOEXEC, 0);
    if (fd < 0) return channel_status::kSystemError;
    struct stat status {};
    if (::fstat(fd, &status) != 0 || status.st_size <= static_cast<off_t>(SHARED_METADATA_SIZE)) {
      ::close(fd); return channel_status::kSystemError;
    }
    size_t data_size = static_cast<size_t>(status.st_size) - SHARED_METADATA_SIZE;
    void *data = ::mmap(nullptr, data_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    void *metadata = ::mmap(nullptr, SHARED_METADATA_SIZE, PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd, static_cast<off_t>(data_size));
    if (data == MAP_FAILED || metadata == MAP_FAILED) {
      if (data != MAP_FAILED) ::munmap(data, data_size);
      if (metadata != MAP_FAILED) ::munmap(metadata, SHARED_METADATA_SIZE);
      ::close(fd); return channel_status::kSystemError;
    }
    m_address = data; m_size = data_size; m_metadata = metadata;
    m_metadata_size = SHARED_METADATA_SIZE; m_fd = fd; m_type = MemoryType::Shared;
    m_name = path;
    ::madvise(data, data_size, MADV_HUGEPAGE);
    return channel_status::kOk;
  }

  void *data() const { return m_address; }
  void *metadata() const { return m_metadata == nullptr ? m_address : m_metadata; }
  SharedMemoryView view() const {
    return {metadata(), data(), m_metadata == nullptr ? m_size : m_metadata_size,
            m_size};
  }
  size_t size() const { return m_size; }
  size_t data_size() const { return m_size; }
  MemoryType type() const { return m_type; }
  bool valid() const { return m_address != nullptr; }
  bool owner() const { return m_owner; }
  int fd() const { return m_fd; }
  const std::string &name() const { return m_name; }

  // 提前删除共享文件名称；已有 mmap 仍然有效，新的 open 将失败。
  void release_shared_name() {
    if (m_type == MemoryType::Shared && m_owner && !m_name.empty()) {
      ::unlink(m_name.c_str());
      m_owner = false;
    }
  }
};
