/*
FYP : 22013

Module:
    Server Skeleton - AQ_5 Query Handler

Description:
    Defines the functions that execute the AQ_5 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "AQH005_cdlp_handler.h"

using namespace std;


unique_ptr<uint64_t[]> do_cdlp(lg::Transaction& transaction, uint64_t max_vertex_id, bool is_graph_directed, uint64_t max_iterations) {
  unique_ptr<uint64_t[]> ptr_labels0 { new uint64_t[max_vertex_id] };
  unique_ptr<uint64_t[]> ptr_labels1 { new uint64_t[max_vertex_id] };
  uint64_t* labels0 = ptr_labels0.get();
  uint64_t* labels1 = ptr_labels1.get();

  #pragma omp parallel for
  for(uint64_t v = 0; v < max_vertex_id; v++) {
    string_view payload = transaction.get_vertex(v);
    if(payload.empty()){
      labels0[v] = labels1[v] = numeric_limits<uint64_t>::max();
    } else {
      labels0[v] = *reinterpret_cast<const uint64_t*>(payload.data());
    }
  }

  bool change = true;
  uint64_t current_iteration = 0;
  while(current_iteration < max_iterations && change) {
    change = false;

    #pragma omp parallel for schedule(dynamic, 64) shared(change)
    for(uint64_t v = 0; v < max_vertex_id; v++) {
      if(labels0[v] == numeric_limits<uint64_t>::max()) continue;

      unordered_map<uint64_t, uint64_t> histogram;

      auto iterator = transaction.get_edges(v, 0);
      while(iterator.valid()) {
        uint64_t u = iterator.dst_id();
        histogram[labels0[u]]++;
        iterator.next();
      }

      uint64_t label_max = numeric_limits<int64_t>::max();
      uint64_t count_max = 0;
      for(const auto pair : histogram){
        if(pair.second > count_max || (pair.second == count_max && pair.first < label_max)) {
          label_max = pair.first;
          count_max = pair.second;
        }
      }

      labels1[v] = label_max;
      change |= (labels0[v] != labels1[v]);
    }

    std::swap(labels0, labels1);
    current_iteration++;
  }

  if(labels0 == ptr_labels0.get()){
    return ptr_labels0;
  } else {
    return ptr_labels1;
  }
}
