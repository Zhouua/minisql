#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  // Field Nums
  MACH_WRITE_UINT32(buf, fields_.size());
  size += sizeof(uint32_t);
  // Null bitmap
  uint64_t bit = 0, flag = 1;
  for(int i = 0; i < fields_.size(); i++){
    if(!fields_[i]->IsNull()) bit^=flag;
    flag<<=1; 
  }
  MACH_WRITE_TO(uint64_t, buf+size, bit);
  size += sizeof(uint64_t);
  // Field-N
  for (int i = 0; i < fields_.size(); ++i) {
    size += fields_[i]->SerializeTo(buf + size);
  }
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
//  ASSERT(fields_.empty(), "Non empty field in row.");
  uint32_t size = 0;
  // Field Nums
  uint32_t num_fields = MACH_READ_UINT32(buf);
  size += sizeof(uint32_t);
  // Null bitmap
  uint64_t bit;
  bit = MACH_READ_FROM(uint64_t, buf+size);
  size += sizeof(uint64_t);
  fields_.clear();
  for (int j = 0;  j < num_fields; j++) {
    Field *myfield= nullptr;
    if (bit & 1) { // not null
      size += Field::DeserializeFrom(buf + size,schema->GetColumn(j)->GetType() , &myfield, false);
    }
    else size += Field::DeserializeFrom(buf + size,schema->GetColumn(j)->GetType() , &myfield, true);
    fields_.push_back(myfield);
    bit >>= 1;
  }
  return size;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t size = 0;
  // Field Nums
  size += sizeof(uint32_t);
  // Null bitmap
  size += sizeof(uint64_t);
  // Field-N
  for (int i = 0; i < fields_.size(); ++i) {
    if (!fields_[i]->IsNull()) size += fields_[i]->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
