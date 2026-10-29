#pragma once

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#if defined(__GNUC__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"
#endif
#include "../ipc/channel.hpp"
#include "../ipc/spsc_channel.hpp"
#if defined(__GNUC__)
#pragma GCC diagnostic pop
#endif

class UdpSpscPublisher : public SpscPublisher {
 private:
  uint8_t* all_ = nullptr;
  uint8_t* buffer_ = nullptr;
  IPCHeadInfo* head_ = nullptr;
  uint64_t reserve_end_ = 0;

 public:
  int create(int buffer_size, const std::string& name, const MemoryType& type) {
    const int result = SpscPublisher::create(buffer_size, name, type);
    if (result == channel_status::kOk) {
      all_ = static_cast<uint8_t*>(shared_memory());
      head_ = reinterpret_cast<IPCHeadInfo*>(all_);
      buffer_ = all_ + sizeof(IPCHeadInfo);
      reserve_end_ = 0;
    }
    return result;
  }

  int available_write_len() const {
    auto* all = static_cast<uint8_t*>(shared_memory());
    if (all == nullptr) return 0;
    auto* head = reinterpret_cast<const IPCHeadInfo*>(all);
    const uint64_t published = channel_atomic::load(&head->pub_pos);
    const uint64_t released = channel_atomic::load(&head->sub_item[0].sub_pos);
    const uint64_t used = published - released;
    if (used >= head->buffer_size) return 0;
    return static_cast<int>(head->buffer_size - used);
  }

  int try_write_at(uint64_t offset, const char* data, int length) {
    if (head_ == nullptr || data == nullptr || length < 1 ||
        length > static_cast<int>(head_->buffer_size)) {
      return channel_status::kInvalidArgument;
    }
    auto& subscriber = head_->sub_item[0];
    const uint64_t released = channel_atomic::load(&subscriber.sub_pos);
    if (offset < released ||
        offset + static_cast<uint64_t>(length) >
            released + head_->buffer_size) {
      return channel_status::kWouldBlock;
    }

    const int mask = static_cast<int>(head_->buffer_size - 1);
    const int position = static_cast<int>(offset & mask);
    const int right = static_cast<int>(head_->buffer_size) - position;
    if (right >= length) {
      std::memcpy(buffer_ + position, data, length);
    } else {
      std::memcpy(buffer_ + position, data, right);
      std::memcpy(buffer_, data + right, length - right);
    }
    reserve_end_ = std::max(reserve_end_, offset + length);
    return channel_status::kOk;
  }

  int commit_until(uint64_t offset) {
    if (head_ == nullptr ||
        offset < channel_atomic::load(&head_->pub_pos) ||
        offset > reserve_end_) {
      return channel_status::kInvalidArgument;
    }
    channel_atomic::store(&head_->pub_pos, offset);
    return channel_status::kOk;
  }

  int initialize_position(uint64_t offset) {
    if (head_ == nullptr) return channel_status::kInvalidArgument;
    reserve_end_ = offset;
    channel_atomic::store(&head_->pub_pos, offset);
    channel_atomic::store(&head_->sub_item[0].sub_pos, offset);
    return channel_status::kOk;
  }
};

class UdpSpscSubscriber : public SpscSubscriber {
 private:
  uint8_t* all_ = nullptr;
  uint64_t read_pos_ = 0;
  uint8_t* buffer_ = nullptr;
  uint64_t buffer_size_ = 0;
  int locked_size_ = 0;
  std::vector<uint8_t> lock_buffer_;

 public:
  int subscribe(void* shared_memory) {
    const int result = SpscSubscriber::subscribe(shared_memory);
    if (result == channel_status::kOk) {
      all_ = static_cast<uint8_t*>(shared_memory);
      auto* head = reinterpret_cast<IPCHeadInfo*>(all_);
      read_pos_ = channel_atomic::load(&head->pub_pos);
      buffer_ = all_ + sizeof(IPCHeadInfo);
      buffer_size_ = head->buffer_size;
    }
    return result;
  }

  int available_read_len() const {
    if (all_ == nullptr) return 0;
    auto* head = reinterpret_cast<const IPCHeadInfo*>(all_);
    const uint64_t published = channel_atomic::load(&head->pub_pos);
    const uint64_t available = published - read_pos_;
    if (available > head->buffer_size) return 0;
    return static_cast<int>(available);
  }

  uint64_t consumed_position() const {
    if (all_ == nullptr) return read_pos_;
    auto* head = reinterpret_cast<const IPCHeadInfo*>(all_);
    return channel_atomic::load(&head->sub_item[0].sub_pos);
  }

  int try_pop(char* data, int length) {
    if (all_ == nullptr || data == nullptr || length < 1 ||
        static_cast<uint64_t>(length) > buffer_size_ || locked_size_ != 0) return channel_status::kInvalidArgument;
    auto* head = reinterpret_cast<IPCHeadInfo*>(all_);
    const uint64_t published = channel_atomic::load(&head->pub_pos);
    if (published - read_pos_ < static_cast<uint64_t>(length)) return channel_status::kWouldBlock;
    const size_t pos = static_cast<size_t>(read_pos_ & (buffer_size_ - 1));
    const size_t first = std::min<size_t>(length, buffer_size_ - pos);
    std::memcpy(data, buffer_ + pos, first);
    if (first < static_cast<size_t>(length)) std::memcpy(data + first, buffer_, length - first);
    read_pos_ += static_cast<uint64_t>(length);
    channel_atomic::store(&head->sub_item[0].sub_pos, read_pos_);
    return channel_status::kOk;
  }

  const char* try_pop_lock(int length) {
    if (all_ == nullptr || length < 1 || static_cast<uint64_t>(length) > buffer_size_ || locked_size_ != 0) return nullptr;
    auto* head = reinterpret_cast<IPCHeadInfo*>(all_);
    if (channel_atomic::load(&head->pub_pos) - read_pos_ < static_cast<uint64_t>(length)) return nullptr;
    const size_t pos = static_cast<size_t>(read_pos_ & (buffer_size_ - 1));
    const size_t first = std::min<size_t>(length, buffer_size_ - pos);
    if (first == static_cast<size_t>(length)) { locked_size_ = length; return reinterpret_cast<const char*>(buffer_ + pos); }
    lock_buffer_.resize(static_cast<size_t>(length));
    std::memcpy(lock_buffer_.data(), buffer_ + pos, first);
    std::memcpy(lock_buffer_.data() + first, buffer_, static_cast<size_t>(length) - first);
    locked_size_ = length;
    return reinterpret_cast<const char*>(lock_buffer_.data());
  }

  int pop_unlock() {
    if (locked_size_ == 0 || all_ == nullptr) return channel_status::kInvalidArgument;
    read_pos_ += static_cast<uint64_t>(locked_size_);
    auto* head = reinterpret_cast<IPCHeadInfo*>(all_);
    channel_atomic::store(&head->sub_item[0].sub_pos, read_pos_);
    locked_size_ = 0;
    return channel_status::kOk;
  }

  int reset_consumer_position(uint64_t position) {
    if (all_ == nullptr) return channel_status::kNotInitialized;
    auto* head = reinterpret_cast<IPCHeadInfo*>(all_);
    const uint64_t published = channel_atomic::load(&head->pub_pos);
    if (position > published) return channel_status::kInvalidArgument;
    read_pos_ = position;
    channel_atomic::store(&head->sub_item[0].sub_pos, position);
    return channel_status::kOk;
  }

  int update_sub_pos(uint64_t offset) {
    if (all_ == nullptr) return channel_status::kNotInitialized;
    auto* head = reinterpret_cast<IPCHeadInfo*>(all_);
    auto& subscriber = head->sub_item[0];
    const uint64_t old = channel_atomic::load(&subscriber.sub_pos);
    const uint64_t published = channel_atomic::load(&head->pub_pos);
    if (offset < old || offset > published) {
      return channel_status::kInvalidArgument;
    }
    channel_atomic::store(&subscriber.sub_pos, offset);
    return channel_status::kOk;
  }
};
