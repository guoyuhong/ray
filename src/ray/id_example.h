#pragma once

#include <arcos/common/macros.h>
#include <arcos/common/enums.h>
#include <dsn/service_api_c.h>

#include <cstring>
#include <random>
#include <string>
#include <vector>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ __FUNCTION__


template <typename T>
class BaseId {
 public:
  class VId {
   public:
    const char* operator()() const { return buffer; }

   public:
    char buffer[2 * sizeof(T) + 1];
  };

 public:
  bool is_nil() const;
  bool operator==(const BaseId& rhs) const;
  bool operator!=(const BaseId& rhs) const;
  bool operator< (const BaseId& rhs) const;

  std::string binary() const;
  std::string hex() const;
  VId to_vstring() const;

  const uint8_t* data() const { return reinterpret_cast<const uint8_t*>(this); }
  uint8_t* mutable_data() { return reinterpret_cast<uint8_t*>(this); }

  static size_t size() { return sizeof(T); }
  static const T& nil() { return *reinterpret_cast<const T*>(kNilId.data()); }
  static const T* from_binary(const void* data, size_t size);
  static const T& from_binary_unsafe(const void* data);
  static T from_binary(const std::string& data);
  static T from_hex(const char* hex_str, size_t hex_size);
};

// ObjectId contains TaskId contains BatchId contains AppId
//  160 bits

// ObjectId := AppId ## TaskId ## ObjectUniqueness ## QosType

#pragma pack(push, 1)
// app id is allocated by app proxy
struct AppId : public BaseId<AppId> {
  AppId() = default;
  AppId(uint16_t val) : id(val) {}  // NOLINT
  uint16_t val() const { return id; }
  size_t hash() const { return static_cast<size_t>(id); }

  unsigned int id : 16;

  static AppId from_random();
};
static_assert(sizeof(struct AppId) == sizeof(char[2]), "invalid size");

struct BatchId : public BaseId<BatchId> {
  size_t hash() const { return (app_id.hash() << 32) + batch_id; }

  struct AppId app_id;
  unsigned int batch_id : 32;

  static BatchId from_random();
};
static_assert(sizeof(struct BatchId) == sizeof(char[6]), "invalid size");

struct TaskId : public BaseId<TaskId> {
  size_t hash() const { return static_cast<size_t>(unique); }

  struct BatchId batch_id;
  uint64_t unique : 64;
  static TaskId from_random();
};
static_assert(sizeof(struct TaskId) == sizeof(char[14]), "invalid size");

struct TaskOptions : public BaseId<TaskOptions> {
  union {
    struct {
      unsigned int is_direct : 1;
      unsigned int is_servitor_call : 1;
      unsigned int is_rpc_call : 1;
      unsigned int priority : 2;
      unsigned int call_type : 3;  // used by each language separately
      unsigned int result_qos_type : 3;  // see  /ref state_type in enums.h
      unsigned int func_type : 4;  // see /ref function_type in enums.h
      unsigned int is_gc_ignored: 1;
      unsigned int is_delete_after_get:1;
      unsigned int is_ignore_put_result:1;
      unsigned int is_persist_in_eager_mode:1;
      unsigned int is_direct_push:1;
      unsigned int padding : 12;
      unsigned int timeout_milliseconds : 32;
    } flags;
    uint64_t value;
  };
};

static_assert(sizeof(struct TaskOptions) == sizeof(uint64_t), "invalid size");

struct ObjectId : public BaseId<ObjectId> {
  size_t hash() const {
    return task_id.hash() ^ (flags.is_put ? index : ~index);
  }

  struct TaskId task_id;
  unsigned int index : 16;  // < return or put index
  struct {
    unsigned int is_put : 1;  // 0 for return, 1 for put
    unsigned int qos_type : 3;
    unsigned int is_direct : 1;
    unsigned int is_gc_ignored: 1;  // not manage ref-count if true
    unsigned int is_delete_after_get: 1;  // whether to delete after first using
    unsigned int is_ignore_put_result: 1;  // not actually put if true
    unsigned int is_persist_in_eager_mode:1;
    unsigned int is_direct_push:1;
    unsigned int padding : 22;
  } flags;

  state_type QosType() const {
    return static_cast<state_type>(flags.qos_type);
  }

  bool is_gc_ignored() const {
    return flags.is_gc_ignored;
  }

  bool is_delete_after_get() const {
    return flags.is_delete_after_get;
  }

  bool is_ignore_put_result() const {
    return flags.is_ignore_put_result;
  }

  static ObjectId from_random();
  static ObjectId build(TaskId task_id,
                        TaskOptions opts, bool is_put, int index);
};
static_assert(sizeof(struct ObjectId) == sizeof(char[20]), "invalid size");
