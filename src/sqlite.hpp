#pragma once
#include <cstddef>
#include <cstdint>
#include "buffer.hpp"

struct PageHeader {
  int8_t page_type;
  int16_t freeblock = 0;
  int16_t num_cells;
  int16_t cell_size;
  int8_t num_fragmented;
};

struct CellHeader {
  int32_t payload_size;
  int32_t row_id;
};

struct RecordHeader {
  std::string type;
  std::string name;
  std::string tbl_name;
  int32_t rootpage;
  std::string sql;
};

PageHeader ParsePageHeader(Buffer& buf) {
  PageHeader ph;
  ph.page_type = buf.ReadByte();
  ph.freeblock = buf.ReadInt16();
  ph.num_cells= buf.ReadInt16();
  ph.cell_size = buf.ReadInt16();
  ph.num_fragmented = buf.ReadByte();

  return ph;
}

CellHeader ParseCellHeader(Buffer& buf) {
  CellHeader ch;
  ch.payload_size = buf.ReadUnsignVarint();
  ch.row_id = buf.ReadUnsignVarint();
  return ch;
}

size_t GetTypeLength(int32_t st) {
    if (st == 0 || st == 8 || st == 9) { return 0; }  // Null
    else if (st >= 1 && st <= 4) { return st; }       // 1-4 byte integer
    else if (st == 5) { return 6; }                   // 6-byte integer
    else if (st == 6 || st == 7) { return 8; }        // 8-byte integer
    else if (st >= 12) { return (st % 2 == 1) ? (st - 13) / 2 : (st - 12) /2; } // String / BLOB
    return 0;
}

RecordHeader ParseRecordHeader(Buffer& buf) {
  RecordHeader schema;

  // Record Header
  size_t record_header_start = buf.GetOffset();
  int32_t record_header_size = buf.ReadUnsignVarint();
  size_t record_header_end = record_header_start + record_header_size;

  // sqlite_schema columns/record header: type(0), name(1), tbl_name(2), rootpage(3), sql(4)
  std::vector<int64_t> serial_types;
  while (buf.GetOffset() < record_header_end) {
    serial_types.push_back(buf.ReadUnsignVarint());
  }

  for (int i = 0; i < serial_types.size(); i++) {
    int st = serial_types[i];
    size_t len = GetTypeLength(st);  

    if (i == 0) { schema.type = buf.ReadString(len); }
    else if (i == 1) { schema.name = buf.ReadString(len); }
    else if (i == 2) { schema.tbl_name = buf.ReadString(len); }
    else if (i == 3) { 
      schema.rootpage = 0;
      for (int b = 0; b < st; b++){
        schema.rootpage = (schema.rootpage << 8) | (uint8_t)buf.ReadByte();
      }
    }
    else if (i == 4) { schema.sql = buf.ReadString(len); }
    else { buf.SkipBytes(len); }
  }

  return schema;
}
