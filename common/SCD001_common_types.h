/*
FYP : 22013

Module:
    Server Skeleton - Common

Description:
    Defines the common data types to be shared across the programme.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include <atomic>
#include <cassert>
#include <cmath>
#include <fstream>
#include <iostream>
#include <limits>
#include <mutex>
#include <sstream>
#include "tbb/concurrent_hash_map.h"
#include <thread>
#include <unordered_set>
#include <unordered_map>
#include <vector>

#include "../third_party/livegraph.hpp"
#include "../third_party/gapbs.hpp"


#ifndef COMMON_TYPE_DEFINITIONS
#define COMMON_TYPE_DEFINITIONS

struct global_data{
  //data structures to be shared
  lg::Graph* LiveGraph;
  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>* investor_vertex_dictionary;
  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>* tradebook_vertex_dictionary;
  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>* company_vertex_dictionary;
  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>* freshness_score_vertex_dictionary;
  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>* trade_vertex_dictionary;

  uint64_t m_num_vertices;
  uint64_t m_num_edges;
};

typedef struct global_data Global_Data;

#endif
