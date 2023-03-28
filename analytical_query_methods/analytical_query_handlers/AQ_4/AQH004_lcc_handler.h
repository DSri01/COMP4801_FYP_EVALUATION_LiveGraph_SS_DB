/*
FYP : 22013

Module:
    Server Skeleton - AQ_4 Query Handler

Description:
    Header file for the functions that execute the AQ_4 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "../../../common/SCD001_common_types.h"
#include "../../../common/SCD002_common_methods.h"
#include "../../../third_party/json.hpp"

#ifndef AQH004_LCC_HANDLER
#define AQH004_LCC_HANDLER

std::unique_ptr<double[]> do_lcc_undirected(lg::Transaction&, uint64_t);

#endif
