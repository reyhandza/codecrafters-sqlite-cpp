#pragma once
#include <arpa/inet.h>
#include <vector>
#include <string>
#include <cstring>

using Bytes = std::vector<uint8_t>;

class Buffer {
public:
  Buffer() = default;
  Buffer(const char* buffer, size_t size) {
    bytes_.assign(buffer, buffer + size);
  }

  Bytes& GetBytes() { return bytes_; }
  size_t GetSize() const { return bytes_.size(); }
  size_t GetOffset() const { return offset_; }
  void SetOffset(size_t n) { offset_ = n; }

  int32_t ReadUnsignVarint() {
    int32_t value = 0;
    uint8_t b;
    int i = 0;

    while (true) {
      b = bytes_[offset_++];
      value |= (b & 0x7f) << (7 * i++);
      if ((b & 0x80) == 0) break;
    }
    
    return value;
  }

  int32_t ReadInt16() {
    int32_t value = 0;
    std::memcpy(&value, &bytes_[offset_], sizeof(int16_t));
    offset_ += sizeof(int16_t);
    return ntohs(value);
  }

  int8_t ReadByte() {
    return bytes_[offset_++];
  }

  std::string ReadString(int32_t n) {
    std::string s(bytes_.begin() + offset_, bytes_.begin() + offset_ + n);
    offset_ += n;
    return s;
  }

  void SkipBytes(size_t n) {
    offset_ += n;
  }

private:
  Bytes bytes_;
  size_t offset_ = 0;
};

