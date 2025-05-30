#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}


Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t size = 0;
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM); 
  size += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+size, name_.length());
  MACH_WRITE_STRING(buf+size+4, name_);
  size += MACH_STR_SERIALIZED_SIZE(name_); //4+SIZEOF
  MACH_WRITE_TO(TypeId, buf+size, type_);
  size += sizeof(TypeId);
  if(type_ == TypeId::kTypeChar){
    MACH_WRITE_UINT32(buf+size, len_);
    size += sizeof(uint32_t);
  }
  MACH_WRITE_UINT32(buf+size, table_ind_);
  size += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf+size, nullable_);
  size += sizeof(bool);
  MACH_WRITE_TO(bool, buf+size, unique_);
  size += sizeof(bool);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size = 0;
  size += sizeof(uint32_t);
  size += MACH_STR_SERIALIZED_SIZE(name_);
  size += sizeof(TypeId);
  if(type_ == TypeId::kTypeChar){
    size += sizeof(uint32_t);
  }
  size += sizeof(uint32_t);
  size += sizeof(bool);
  size += sizeof(bool);
  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  ASSERT(column != nullptr, "Invalid column before deserialize.");
  uint32_t size = 0;
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == COLUMN_MAGIC_NUM, "Wrong magic number.");
  size += sizeof(uint32_t);

  uint32_t name_len = MACH_READ_UINT32(buf+size);
  std::string name = new char[name_len];
  uint32_t i = 0;
  for (; i < name_len; ++i) {
    name[i] = MACH_READ_FROM(char, buf+size+i);
  }
  name[i] = '\0';
  size += name_len;

  TypeId type = MACH_READ_FROM(TypeId, buf+size);
  size += sizeof(TypeId);
  uint32_t len = 0;
  if(type == TypeId::kTypeChar){
    len = MACH_READ_UINT32(buf+size);
    size += sizeof(uint32_t);
  }
  uint32_t table_ind = MACH_READ_UINT32(buf+size);
  size += sizeof(uint32_t);
  bool nullable = MACH_READ_FROM(bool, buf+size);
  size += sizeof(bool);
  bool unique = MACH_READ_FROM(bool, buf+size);
  size += sizeof(bool);

  if(type == TypeId::kTypeChar)
    column = new Column(name,type,len,table_ind,nullable,unique);
  else column = new Column(name,type,table_ind,nullable,unique);
  return size;
}
