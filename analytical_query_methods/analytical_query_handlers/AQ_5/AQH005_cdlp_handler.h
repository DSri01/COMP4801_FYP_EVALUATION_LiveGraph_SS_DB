/*
FYP : 22013

Module:
    Server Skeleton - AQ_5 Query Handler

Description:
    Header file for the functions that execute the AQ_5 query.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "../../../common/SCD001_common_types.h"
#include "../../../common/SCD002_common_methods.h"
#include "../../../third_party/json.hpp"

#ifndef AQH005_CDLP_HANDLER
#define AQH005_CDLP_HANDLER

std::unique_ptr<uint64_t[]> do_cdlp(lg::Transaction&, uint64_t, bool, uint64_t);

#endif
