/*
FYP : 22013

Module:
    Server Skeleton - AQ_2 Query Handler

Description:
    Defines the functions that execute the AQ_2 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "AQH002_pr_handler.h"

using namespace std;

unique_ptr<double[]> do_pagerank(lg::Transaction& transaction, uint64_t num_vertices, uint64_t max_vertex_id, uint64_t num_iterations, double damping_factor) {
  const double init_score = 1.0 / num_vertices;
  const double base_score = (1.0 - damping_factor) / num_vertices;

  unique_ptr<double[]> ptr_scores{ new double[max_vertex_id]() };
  unique_ptr<uint64_t[]> ptr_degrees{ new uint64_t[max_vertex_id]() };
  double* scores = ptr_scores.get();
  uint64_t* __restrict degrees = ptr_degrees.get();


  #pragma omp parallel for
  for(uint64_t v = 0; v < max_vertex_id; v++) {
    scores[v] = init_score;

    if(!transaction.get_vertex(v).empty()){
      uint64_t degree = 0;
      auto iterator = transaction.get_edges(v, 0);
      while(iterator.valid()){ degree++; iterator.next(); }
      degrees[v] = degree;
    } else {
      degrees[v] = numeric_limits<uint64_t>::max();
    }
  }

  gapbs::pvector<double> outgoing_contrib(max_vertex_id, 0.0);

  for(uint64_t iteration = 0; iteration < num_iterations; iteration++) {
    double dangling_sum = 0.0;

    #pragma omp parallel for reduction(+:dangling_sum)
    for(uint64_t v = 0; v < max_vertex_id; v++){
      uint64_t out_degree = degrees[v];
      if(out_degree == numeric_limits<uint64_t>::max()){
        continue;
      } else if (out_degree == 0){
        dangling_sum += scores[v];
      } else {
        outgoing_contrib[v] = scores[v] / out_degree;
      }
    }

    dangling_sum /= num_vertices;

    #pragma omp parallel for schedule(dynamic, 64)
    for(uint64_t v = 0; v < max_vertex_id; v++) {
      if(degrees[v] == numeric_limits<uint64_t>::max()){ continue; }

      double incoming_total = 0;
      auto iterator = transaction.get_edges(v, 0);
      while(iterator.valid()){
        uint64_t u = iterator.dst_id();
        incoming_total += outgoing_contrib[u];
        iterator.next();
      }

      scores[v] = base_score + damping_factor * (incoming_total + dangling_sum);
    }
  }

  return ptr_scores;
}
