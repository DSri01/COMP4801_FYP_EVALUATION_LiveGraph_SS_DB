/*
FYP : 22013

Module:
    Server Skeleton - Edge Insertors

Description:
    Header file for the functions that insert the edges for the base data of
    the benchmark.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "../common/SCD001_common_types.h"
#include "../common/SCD002_common_methods.h"

#include "../third_party/json.hpp"

using json = nlohmann::json;

extern Global_Data GD;

#ifndef SEI000_EDGE_INSERTORS
#define SEI000_EDGE_INSERTORS

void insert_friend_edge(json, json*);

void insert_mirror_edge(json, json*);

void insert_hasbook_edge(json, json*);

#endif
