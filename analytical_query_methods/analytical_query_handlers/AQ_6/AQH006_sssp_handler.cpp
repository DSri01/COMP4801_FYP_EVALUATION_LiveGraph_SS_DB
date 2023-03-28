/*
FYP : 22013

Module:
    Server Skeleton - AQ_6 Query Handler

Description:
    Defines the functions that execute the AQ_6 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "AQH006_sssp_handler.h"

using namespace std;

using NodeID = uint64_t;
using WeightT = double;
size_t kMaxBin = numeric_limits<size_t>::max()/2;

gapbs::pvector<WeightT> do_sssp(lg::Transaction& transaction, uint64_t num_edges, uint64_t max_vertex_id, uint64_t source, double delta) {
  gapbs::pvector<WeightT> dist(max_vertex_id, numeric_limits<WeightT>::infinity());
  dist[source] = 0;
  gapbs::pvector<NodeID> frontier(num_edges);

  size_t shared_indexes[2] = {0, kMaxBin};
  size_t frontier_tails[2] = {1, 0};
  frontier[0] = source;

  #pragma omp parallel
  {
    vector<vector<NodeID> > local_bins(0);
    size_t iter = 0;

    while (shared_indexes[iter&1] != kMaxBin) {
      size_t &curr_bin_index = shared_indexes[iter&1];
      size_t &next_bin_index = shared_indexes[(iter+1)&1];
      size_t &curr_frontier_tail = frontier_tails[iter&1];
      size_t &next_frontier_tail = frontier_tails[(iter+1)&1];
      #pragma omp for nowait schedule(dynamic, 64)
      for (size_t i=0; i < curr_frontier_tail; i++) {
        NodeID u = frontier[i];
        if (dist[u] >= delta * static_cast<WeightT>(curr_bin_index)) {
          auto iterator = transaction.get_edges(u, 0);
          while(iterator.valid()) {
            uint64_t v = iterator.dst_id();
            string_view payload = iterator.edge_data();
            double w = *reinterpret_cast<const double*>(payload.data());

            WeightT old_dist = dist[v];
            WeightT new_dist = dist[u] + w;
            if (new_dist < old_dist) {
              bool changed_dist = true;
              while (!gapbs::compare_and_swap(dist[v], old_dist, new_dist)) {
                old_dist = dist[v];
                if (old_dist <= new_dist) {
                  changed_dist = false;
                  break;
                }
              }
              if (changed_dist) {
                size_t dest_bin = new_dist/delta;
                if (dest_bin >= local_bins.size()) {
                  local_bins.resize(dest_bin+1);
                }
                local_bins[dest_bin].push_back(v);
              }
            }
            iterator.next();
          }
        }
      }

      for (size_t i=curr_bin_index; i < local_bins.size(); i++) {
        if (!local_bins[i].empty()) {
          #pragma omp critical
          next_bin_index = min(next_bin_index, i);
          break;
        }
      }

      #pragma omp barrier
      #pragma omp single nowait
      {
        curr_bin_index = kMaxBin;
        curr_frontier_tail = 0;
      }

      if (next_bin_index < local_bins.size()) {
        size_t copy_start = gapbs::fetch_and_add(next_frontier_tail, local_bins[next_bin_index].size());
        copy(local_bins[next_bin_index].begin(), local_bins[next_bin_index].end(), frontier.data() + copy_start);
        local_bins[next_bin_index].resize(0);
      }

      iter++;
      #pragma omp barrier
    }
  }

  return dist;
}
