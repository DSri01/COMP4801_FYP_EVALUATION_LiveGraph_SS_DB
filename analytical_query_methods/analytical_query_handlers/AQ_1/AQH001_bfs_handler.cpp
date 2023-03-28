/*
FYP : 22013

Module:
    Server Skeleton - AQ_1 Query Handler

Description:
    Defines the functions that execute the AQ_1 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "AQH001_bfs_handler.h"

using namespace std;

int64_t do_bfs_BUStep(lg::Transaction& transaction, uint64_t max_vertex_id, int64_t* distances, int64_t distance, gapbs::Bitmap &front, gapbs::Bitmap &next) {
  int64_t awake_count = 0;
  next.reset();

  #pragma omp parallel for schedule(dynamic, 1024) reduction(+ : awake_count)
  for (uint64_t u = 0; u < max_vertex_id; u++) {
    if(distances[u] == numeric_limits<int64_t>::max()) continue;
    if (distances[u] < 0){
      auto iterator = transaction.get_edges(u, 0);
      while(iterator.valid()){
        uint64_t dst = iterator.dst_id();
        if(front.get_bit(dst)) {
          distances[u] = distance;
          awake_count++;
          next.set_bit(u);
          break;
        }
        iterator.next();
      }
    }
  }
  return awake_count;
}

int64_t do_bfs_TDStep(lg::Transaction& transaction, uint64_t max_vertex_id, int64_t* distances, int64_t distance, gapbs::SlidingQueue<int64_t>& queue) {
  int64_t scout_count = 0;

  #pragma omp parallel reduction(+ : scout_count)
  {
    gapbs::QueueBuffer<int64_t> lqueue(queue);

    #pragma omp for schedule(dynamic, 64)
    for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) {
      int64_t u = *q_iter;

      auto iterator = transaction.get_edges(u, 0);
      while(iterator.valid()){
        uint64_t dst = iterator.dst_id();

        int64_t curr_val = distances[dst];
        if (curr_val < 0 && gapbs::compare_and_swap(distances[dst], curr_val, distance)) {
          lqueue.push_back(dst);
          scout_count += -curr_val;
        }

        iterator.next();
      }
    }

    lqueue.flush();
  }

  return scout_count;
}

void do_bfs_QueueToBitmap(lg::Transaction& transaction, uint64_t max_vertex_id, const gapbs::SlidingQueue<int64_t> &queue, gapbs::Bitmap &bm) {
  #pragma omp parallel for
  for (auto q_iter = queue.begin(); q_iter < queue.end(); q_iter++) {
    int64_t u = *q_iter;
    bm.set_bit_atomic(u);
  }
}

void do_bfs_BitmapToQueue(lg::Transaction& transaction, uint64_t max_vertex_id, const gapbs::Bitmap &bm, gapbs::SlidingQueue<int64_t> &queue) {
  #pragma omp parallel
  {
    gapbs::QueueBuffer<int64_t> lqueue(queue);
    #pragma omp for
    for (uint64_t n=0; n < max_vertex_id; n++)
      if (bm.get_bit(n))
        lqueue.push_back(n);
    lqueue.flush();
  }
  queue.slide_window();
}

std::unique_ptr<int64_t[]> do_bfs_init_distances(lg::Transaction& transaction, uint64_t max_vertex_id) {
  unique_ptr<int64_t[]> distances{ new int64_t[max_vertex_id] };
  #pragma omp parallel for
  for (uint64_t n = 0; n < max_vertex_id; n++) {
    if(transaction.get_vertex(n).empty()){
      distances[n] = numeric_limits<int64_t>::max();
    } else {
      uint64_t out_degree = 0;
      auto iterator = transaction.get_edges(n, 0);
      while(iterator.valid()){
        out_degree++;
        iterator.next();
      }
      distances[n] = out_degree != 0 ? - out_degree : -1;
    }
  }

  return distances;
}

std::unique_ptr<int64_t[]> do_bfs(lg::Transaction& transaction, uint64_t num_vertices, uint64_t num_edges, uint64_t max_vertex_id, uint64_t root) {
  int alpha = 15; int beta = 18;
  unique_ptr<int64_t[]> ptr_distances = do_bfs_init_distances(transaction, max_vertex_id);
  int64_t* __restrict distances = ptr_distances.get();
  distances[root] = 0;

  gapbs::SlidingQueue<int64_t> queue(max_vertex_id);
  queue.push_back(root);
  queue.slide_window();
  gapbs::Bitmap curr(max_vertex_id);
  curr.reset();
  gapbs::Bitmap front(max_vertex_id);
  front.reset();
  int64_t edges_to_check = num_edges;

  int64_t scout_count = 0;
  {
    auto iterator = transaction.get_edges(root, 0);
    while(iterator.valid()){ scout_count++; iterator.next(); }
  }
  int64_t distance = 1;

  while (!queue.empty()) {
    if (scout_count > edges_to_check / alpha) {
      int64_t awake_count, old_awake_count;
      do_bfs_QueueToBitmap(transaction, max_vertex_id, queue, front);
      awake_count = queue.size();
      queue.slide_window();
      do {
        old_awake_count = awake_count;
        awake_count = do_bfs_BUStep(transaction, max_vertex_id, distances, distance, front, curr);
        front.swap(curr);
        distance++;
      } while ((awake_count >= old_awake_count) || (awake_count > (int64_t) num_vertices / beta));
      do_bfs_BitmapToQueue(transaction, max_vertex_id, front, queue);
      scout_count = 1;
    } else {
      edges_to_check -= scout_count;
      scout_count = do_bfs_TDStep(transaction, max_vertex_id, distances, distance, queue);
      queue.slide_window();
      distance++;
    }
  }

  return ptr_distances;
}
