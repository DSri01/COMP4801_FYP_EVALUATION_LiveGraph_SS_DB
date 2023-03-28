/*
FYP : 22013

Module:
    Server Skeleton - AQ_4 Query Handler

Description:
    Defines the functions that execute the AQ_4 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "AQH004_lcc_handler.h"

using namespace std;

unique_ptr<double[]> do_lcc_undirected(lg::Transaction& transaction, uint64_t max_vertex_id) {
  unique_ptr<double[]> ptr_lcc { new double[max_vertex_id] };
  double* lcc = ptr_lcc.get();
  unique_ptr<uint32_t[]> ptr_degrees_out { new uint32_t[max_vertex_id] };
  uint32_t* __restrict degrees_out = ptr_degrees_out.get();

  #pragma omp parallel for schedule(dynamic, 4096)
  for(uint64_t v = 0; v < max_vertex_id; v++) {
    bool vertex_exists = !transaction.get_vertex(v).empty();
    if(!vertex_exists){
      lcc[v] = numeric_limits<double>::signaling_NaN();
    } else {
      {
        uint32_t count = 0;
        auto iterator = transaction.get_edges(v, 0);
        while(iterator.valid()){ count ++; iterator.next(); }
        degrees_out[v] = count;
      }
    }
  }


  #pragma omp parallel for schedule(dynamic, 64)
  for(uint64_t v = 0; v < max_vertex_id; v++) {
    if(degrees_out[v] == numeric_limits<uint32_t>::max()) continue;

    lcc[v] = 0.0;
    uint64_t num_triangles = 0;

    uint64_t v_degree_out = degrees_out[v];
    if(v_degree_out < 2) continue;

    unordered_set<uint64_t> neighbours;

    {
      auto iterator1 = transaction.get_edges(v, 0);
      while(iterator1.valid()) {
        uint64_t u = iterator1.dst_id();
        neighbours.insert(u);
        iterator1.next();
      }
    }


    auto iterator1 = transaction.get_edges(v, 0);
    while(iterator1.valid()) {
      uint64_t u = iterator1.dst_id();

      assert(neighbours.count(u) == 1 && "The set `neighbours' should contain all neighbours of v");


      auto iterator2 = transaction.get_edges(u, 0);
      while(iterator2.valid()) {
        uint64_t w = iterator2.dst_id();


        if(neighbours.count(w) == 1) {
          num_triangles++;
        }

        iterator2.next();
      }

      iterator1.next();
    }


    uint64_t max_num_edges = v_degree_out * (v_degree_out -1);
    lcc[v] = static_cast<double>(num_triangles) / max_num_edges;
  }

  return ptr_lcc;
}
