/*
FYP : 22013

Module:
    Server Skeleton - AQ_1 Query Handler

Description:
    Header file for the functions that execute the AQ_1 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "../../../common/SCD001_common_types.h"
#include "../../../common/SCD002_common_methods.h"
#include "../../../third_party/json.hpp"

#ifndef AQH001_BFS_HANDLER
#define AQH001_BFS_HANDLER

int64_t do_bfs_BUStep(lg::Transaction& , uint64_t , int64_t*, int64_t, gapbs::Bitmap &, gapbs::Bitmap &);

int64_t do_bfs_TDStep(lg::Transaction& , uint64_t , int64_t*, int64_t distance, gapbs::SlidingQueue<int64_t>&);

void do_bfs_QueueToBitmap(lg::Transaction& , uint64_t , const gapbs::SlidingQueue<int64_t> &, gapbs::Bitmap &);

void do_bfs_BitmapToQueue(lg::Transaction&, uint64_t, const gapbs::Bitmap& , gapbs::SlidingQueue<int64_t> &);

std::unique_ptr<int64_t[]> do_bfs_init_distances(lg::Transaction&, uint64_t);

std::unique_ptr<int64_t[]> do_bfs(lg::Transaction&, uint64_t, uint64_t, uint64_t, uint64_t);

#endif
