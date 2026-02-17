#include <cstddef>
#include <arpa/inet.h>
#include <cstdint>
#include <cstring>
#include <fstream>
#include <iostream>
#include <ostream>
#include <string>
#include <vector>

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

void handle_client(const std::string& path, const std::string& cmd) {
  std::ifstream database_file(path, std::ios::binary | std::ios::ate);
  if (!database_file) {
    std::cerr << "Failed to open the database file" << std::endl;
  }
  size_t size = database_file.tellg();
  
  char* data = new char[size];
  database_file.seekg(0);
  database_file.read(data, size);

  Buffer bytes(data, size);

  // File header 100 bytes in total
  bytes.SkipBytes(16);   // Skip the first 16 bytes of the header
  uint16_t page_size = bytes.ReadInt16();
  bytes.SkipBytes(82);   // Skip fully the database header

  // B-tree page header (start at offset 100, 8 bytes for leaf pages)
  int8_t page_type = bytes.ReadByte();
  bytes.SkipBytes(2);    // Freeblock
  int16_t num_cells = bytes.ReadInt16();
  bytes.SkipBytes(2);    // Cell size
  bytes.SkipBytes(1);    // Num fragmented free bytes
  // Now at offset 108

  if (cmd == ".dbinfo") {
    std::cout << "database page size: " << page_size << std::endl;
    std::cout << "number of tables: " << num_cells << std::endl;
    return;
  }

  std::vector<uint16_t> cell_pointers;
  for (int i = 0; i < num_cells; i++) {
    cell_pointers.push_back(bytes.ReadInt16());   // Read offset where cells located
  }

  if (cmd == ".tables") {
    std::vector<std::string> table_names;

    for (auto& cell_pointer : cell_pointers) {
      // Jump to the cell
      bytes.SetOffset(cell_pointer);

      // Cell Header
      int32_t cell_payload_size = bytes.ReadUnsignVarint();
      int32_t row_id = bytes.ReadUnsignVarint();

      // Cell Payload: Record header + record payload
      // Record Format:
      //    varint: record_header_size
      //    varints: serial type for each columns
      //    values in order
      size_t record_header_start = bytes.GetOffset();
      int32_t record_header_size = bytes.ReadUnsignVarint();
      size_t record_header_end = record_header_start + record_header_size;

      // sqlite_schema columns: type(0), name(1), tbl_name(2), rootpage(3), sql(4)
      std::vector<int64_t> serial_types;
      while (bytes.GetOffset() < record_header_end) {
        serial_types.push_back(bytes.ReadUnsignVarint());
      }

      for (int i = 0; i < serial_types.size(); i++) {
        int st = serial_types[i];

        if (st == 0) { }
        else if (st >= 1 && st <= 4) { bytes.SkipBytes(st); }      // 1-4 byte integer
        else if (st == 5) { bytes.SkipBytes(6); }                  // 6-byte integer
        else if (st == 6 || st == 7) { bytes.SkipBytes(8); }       // 8-byte integer
        else if (st >= 12) { 
          bool is_text = st % 2 == 1;
          int32_t len = is_text ? (st - 13) / 2 : (st - 12) / 2;
          if (i == 2 && is_text) {
            std::string tbl_name = bytes.ReadString(len);

            if (tbl_name.substr(0, 7) != "sqlite_") {
              table_names.push_back(tbl_name);
            }
          }
          else { bytes.SkipBytes(len); }
        }
      }
    }

    for (int i = 0; i < table_names.size(); i++) {
      if (i > 0) std::cout << " ";
      std::cout << table_names[i];
    }
    std::cout << std::endl;
  }
}

int main(int argc, char *argv[]) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  std::cerr << "Logs from your program will appear here" << std::endl;

  if (argc != 3) {
    std::cerr << "Expected two arguments" << std::endl;
    return 1;
  }

  std::string database_file_path = argv[1];
  std::string command = argv[2];

  handle_client(database_file_path, command);

  return 0;
}
