/*
FYP : 22013

Module:
    Server Skeleton - Transaction Query Handlers

Description:
    Header file for the functions that serve the transactional queries sent by
    the Hybrid Query Driver.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "../common/SCD001_common_types.h"
#include "../common/SCD002_common_methods.h"
#include "../third_party/json.hpp"

#include <map>
#include <string>
#include <vector>

using json = nlohmann::json;

extern Global_Data GD;

#ifndef TQH000_TRANSACTION_HANDLER
#define TQH000_TRANSACTION_HANDLER

bool update_freshness_score_vertex(void*, lg::vertex_t, unsigned long);

void perform_TQ_1_A(json, json*);

void perform_TQ_1_B(json, json*);

void perform_TQ_2(json, json*);

void perform_TQ_3(json, json*);

void perform_TQ_4(json, json*);

void perform_TQ_5(json, json*);

void perform_TQ_6(json, json*);

void perform_TQ_7(json, json*);

void perform_TQ_8(json, json*);

#endif
