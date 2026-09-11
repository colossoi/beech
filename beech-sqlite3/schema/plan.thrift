// SQLite access-plan transport. Generate with Apache Thrift 0.24.0.
namespace rs plan

struct SearchSlot {
  1: required i32 key_part,
  2: required i32 column,
  // 0=Unknown, 1=Eq, 2=Gt, 3=Le, 4=Lt, 5=Ge, 6=IsNull, 7=IsNotNull.
  // Search slots only accept comparison operators 1..=5.
  3: required i32 op,
  4: required i32 argv_index
}

struct AccessPlan {
  1: required binary table_id,
  2: required list<SearchSlot> search,
  3: required bool preserves_order,
  4: required double estimated_cost,
  5: required i64 estimated_rows
}
