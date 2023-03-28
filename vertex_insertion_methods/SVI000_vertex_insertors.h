/*
FYP : 22013

Module:
    Server Skeleton - Vertex Insertors

Description:
    Header file for the functions that insert the vertices for the base data of
    the benchmark.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "../common/SCD001_common_types.h"
#include "../common/SCD002_common_methods.h"
#include "../third_party/json.hpp"

using json = nlohmann::json;

extern Global_Data GD;

#ifndef SVI000_VERTEX_INSERTORS
#define SVI000_VERTEX_INSERTORS

void insert_investor_vertex(json, json*);

void insert_tradebook_vertex(json, json*);

void insert_company_vertex(json, json*);

void insert_freshness_score_vertex(json, json*);

#endif
