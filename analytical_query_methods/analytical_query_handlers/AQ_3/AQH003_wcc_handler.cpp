/*
FYP : 22013

Module:
    Server Skeleton - AQ_3 Query Handler

Description:
    Defines the functions that execute the AQ_3 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "AQH003_wcc_handler.h"

using namespace std;

unique_ptr<uint64_t[]> do_wcc(lg::Transaction& transaction, uint64_t max_vertex_id) {
  unique_ptr<uint64_t[]> ptr_components { new uint64_t[max_vertex_id] };
  uint64_t* comp = ptr_components.get();

  #pragma omp parallel for
  for (uint64_t n = 0; n < max_vertex_id; n++) {
    if(transaction.get_vertex(n).empty()) {
      comp[n] = numeric_limits<uint64_t>::max();
    } else {
      comp[n] = n;
    }
  }

  bool change = true;
  while (change) {
    change = false;

    #pragma omp parallel for schedule(dynamic, 64)
    for (uint64_t u = 0; u < max_vertex_id; u++) {
      if(comp[u] == numeric_limits<uint64_t>::max()) continue;

      auto iterator = transaction.get_edges(u, 0);
      while(iterator.valid()) {
        uint64_t v = iterator.dst_id();

        uint64_t comp_u = comp[u];
        uint64_t comp_v = comp[v];
        if (comp_u != comp_v) {
          uint64_t high_comp = std::max(comp_u, comp_v);
          uint64_t low_comp = std::min(comp_u, comp_v);
          if (high_comp == comp[high_comp]) {
            change = true;

            comp[high_comp] = low_comp;
          }
        }

        iterator.next();
      }
    }

    #pragma omp parallel for schedule(dynamic, 64)
    for (uint64_t n = 0; n < max_vertex_id; n++) {
      if(comp[n] == numeric_limits<uint64_t>::max()) continue;

      while (comp[n] != comp[comp[n]]) {
        comp[n] = comp[comp[n]];
      }
    }
  }

  return ptr_components;
}
