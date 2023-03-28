/*
FYP : 22013

Module:
    Server Skeleton - Common

Description:
    Header file defining the common functions to be shared across the programme.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include <string>
#include <vector>

#include "../third_party/json.hpp"
#include "SCD001_common_types.h"

#ifndef COMMON_METHOD_DEFINITIONS
#define COMMON_METHOD_DEFINITIONS

using json = nlohmann::json;

extern Global_Data GD;

uint64_t ext2int(uint64_t);

uint64_t int2ext(void*, uint64_t);

lg::vertex_t put_livegraph_vertex(std::string_view);

bool put_livegraph_edge(lg::vertex_t, lg::vertex_t, lg::label_t);


/*
Description:
    This function is called by the analytical queries for processing.

    The implementation of this function is taken from the evaluation experiments
    conducted for the Sortledton paper. The original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/
template <typename T>
std::vector<std::pair<uint64_t, T>> translate(void* transaction, const T* __restrict data, uint64_t data_sz) {
  assert(transaction != nullptr && "Transaction object not specified");
  std::vector<std::pair<uint64_t, T>> output(data_sz);

  for(uint64_t logical_id = 0; logical_id < data_sz; logical_id++){
    uint64_t external_id = int2ext(transaction, logical_id);
    if(external_id == std::numeric_limits<uint64_t>::max()) {
      output[logical_id] = std::make_pair(std::numeric_limits<uint64_t>::max(), std::numeric_limits<T>::max());
    } else {
      output[logical_id] = std::make_pair(external_id, data[logical_id]);
    }
  }
  return output;
}

#endif
