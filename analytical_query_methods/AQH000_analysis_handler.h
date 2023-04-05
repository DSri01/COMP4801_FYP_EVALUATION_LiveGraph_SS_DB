/*
FYP : 22013

Module:
    Server Skeleton - Analytical Query Handlers

Description:
    Header file for the functions that serve the analytical queries sent by the
    Hybrid Query Driver.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "../common/SCD001_common_types.h"
#include "../common/SCD002_common_methods.h"
#include "../third_party/json.hpp"

#include "analytical_query_handlers/AQ_1/AQH001_bfs_handler.h"
#include "analytical_query_handlers/AQ_2/AQH002_pr_handler.h"
#include "analytical_query_handlers/AQ_3/AQH003_wcc_handler.h"
#include "analytical_query_handlers/AQ_4/AQH004_lcc_handler.h"
#include "analytical_query_handlers/AQ_5/AQH005_cdlp_handler.h"
#include "analytical_query_handlers/AQ_6/AQH006_sssp_handler.h"

#include <map>
#include <string>

using json = nlohmann::json;

extern Global_Data GD;

#ifndef AQH000_ANALYSIS_HANDLER
#define AQH000_ANALYSIS_HANDLER

#define MAX_INVESTORS 200000
#define MAX_FRIENDS 40000

std::map<std::string, unsigned long> read_freshness_score_transaction_ids(void*, unsigned long, int);

void perform_AQ_1(json, json*);

void perform_AQ_2(json, json*);

void perform_AQ_3(json, json*);

void perform_AQ_4(json, json*);

void perform_AQ_5(json, json*);

void perform_AQ_6(json, json*);

#endif
