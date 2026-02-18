#include <fstream>
#include <iostream>
#include "buffer.hpp"
#include "sqlite.hpp"

Buffer ReadDB(const std::string& path) {
  std::ifstream database_file(path, std::ios::binary | std::ios::ate);
  if (!database_file) {
    std::cerr << "Failed to open the database file" << std::endl;
  }
  size_t size = database_file.tellg();
  
  std::vector<char> data(size);
  database_file.seekg(0);
  database_file.read(data.data(), size);

  Buffer bytes(data.data(), size);

  return bytes;
}

void handle_client(const std::string& cmd, Buffer& bytes) {
  // File header 100 bytes in total
  bytes.SkipBytes(16);   // Skip the first 16 bytes of the header
  uint16_t page_size = bytes.ReadInt16();
  bytes.SkipBytes(82);   // Skip fully the database header

  // B-tree page header (start at offset 100, 8 bytes for leaf pages)
  PageHeader ph = ParsePageHeader(bytes);

  // Now at offset 108
  std::vector<uint16_t> cell_pointers;
  for (int i = 0; i < ph.num_cells; i++) {
    cell_pointers.push_back(bytes.ReadInt16());   // Read offset where cells located
  }

  if (cmd == ".dbinfo") {
    std::cout << "database page size: " << page_size << std::endl;
    std::cout << "number of tables: " << ph.num_cells << std::endl;
    return;
  }

  if (cmd == ".tables") {
    std::vector<std::string> table_names;

    for (auto& cell_pointer : cell_pointers) {
      // Jump to the cell
      bytes.SetOffset(cell_pointer);
      // Cell Header
      ParseCellHeader(bytes);

      // Cell Payload: Record header + record payload
      // Record Format:
      //    varint: record_header_size
      //    varints: serial type for each columns
      //    values in order
      RecordHeader rh = ParseRecordHeader(bytes);
      table_names.push_back(rh.tbl_name);
    
      for (int i = 0; i < table_names.size(); i++) {
        if (i > 0) std::cout << " ";
        std::cout << table_names[i];
      }
    }
    std::cout << std::endl;
    return;
  }

  // cmd input must be lowercase
  if (cmd.substr(0, 15) == "select count(*)") {
    size_t pos = cmd.find("from ");
    std::string target_tbl_name = cmd.substr(pos + 5);

    // Just in case
    while (!target_tbl_name.empty() && (target_tbl_name.back() == ' '
          || target_tbl_name.back() == '\n' 
          || target_tbl_name.back() == '\r')) {

      target_tbl_name.pop_back();
    }

    std::string tbl_name;
    int32_t rootpage;

    for (auto& cell_pointer : cell_pointers) {
      // Jump to the cell
      bytes.SetOffset(cell_pointer);
      ParseCellHeader(bytes);
      RecordHeader rh = ParseRecordHeader(bytes);
      rootpage = rh.rootpage;

      if (rh.tbl_name == target_tbl_name) {
        break;
      }
    }

    int32_t page = (rootpage - 1) * page_size;
    bytes.SetOffset(page);

    // Header target table
    PageHeader header = ParsePageHeader(bytes);
    std::cout << header.num_cells << std::endl;
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

  Buffer buffer = ReadDB(database_file_path);
  handle_client(command, buffer);

  return 0;
}
