#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
      uint32_t size = 0;
      MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM); //�� ���� д�뵽 ǰ�ߣ����������Ϊ uint32_t ����
      size += sizeof(uint32_t);
      MACH_WRITE_UINT32(buf+size, columns_.size());
      size += sizeof(uint32_t);
      for (int i = 0; i < columns_.size(); ++i) {
            size += columns_[i]->SerializeTo(buf+size);
      }
      MACH_WRITE_TO(bool, buf+size, is_manage_);
      size += sizeof(bool);
      return size;
}

uint32_t Schema::GetSerializedSize() const {
      uint32_t size = 0;
      size += sizeof(uint32_t) * 2; //magic_num & size
      for (int i = 0; i < columns_.size(); ++i) {
            size += columns_[i]->GetSerializedSize();
      }
      size += sizeof(bool);
      return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
//      ASSERT(schema != nullptr, "Invalid schema before deserialize.");
      uint32_t size = 0;
      uint32_t magic_num = MACH_READ_UINT32(buf);
      size += sizeof(uint32_t);
      ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid schema magic number.");

      uint32_t column_size = MACH_READ_UINT32(buf+size);
      size += sizeof(uint32_t);
      std::vector<Column*> columns;
      for (int i = 0; i < column_size; ++i) {
          Column* column = new Column("default", TypeId::kTypeInt, 0, false, false);
          size += Column::DeserializeFrom(buf+size, column);
          columns.push_back(column); //û��ָ�룬����ֱ��ǳ����
      }
      bool is_manage = MACH_READ_FROM(bool, buf+size);
      size += sizeof(bool);
      schema = new Schema(columns, is_manage);
      return size;
}