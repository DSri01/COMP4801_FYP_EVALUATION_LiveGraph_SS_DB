/*
FYP : 22013

Module:
    Server Skeleton - AQ_6 Query Handler

Description:
    Header file for the functions that execute the AQ_6 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "../../../common/SCD001_common_types.h"
#include "../../../common/SCD002_common_methods.h"
#include "../../../third_party/json.hpp"

#ifndef AQH006_SSSP_HANDLER
#define AQH006_SSSP_HANDLER

using NodeID = uint64_t;
using WeightT = double;

gapbs::pvector<WeightT> do_sssp(lg::Transaction&, uint64_t, uint64_t, uint64_t, double);

#endif
