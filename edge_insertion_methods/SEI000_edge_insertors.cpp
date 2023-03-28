/*
FYP : 22013

Module:
    Server Skeleton - Edge Insertors

Description:
    Defines the functions that insert the edges for the base data of the
    benchmark.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "SEI000_edge_insertors.h"

/*
Description:
    Inserts the friend edge for the base data.
*/
void insert_friend_edge(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long source_investor_id = request["SOURCE_ID"].get<unsigned long>();
  unsigned long destination_investor_id = request["DESTINATION_ID"].get<unsigned long>();
  /*--------------------------------------------------------------------------*/
  /*INSERT friend EDGE HERE*/

  //INSERT UNDIRECTED friend EDGE BETWEEN GIVEN INVESTOR IDs

  uint64_t source = (uint64_t) source_investor_id;
  uint64_t destination = (uint64_t) destination_investor_id;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor1, accessor2;

  bool found = false;
  bool success = false;
  int found_count = 0;

  if (GD.investor_vertex_dictionary->find(accessor1, source)) {
    found_count++;
  }

  if (GD.investor_vertex_dictionary->find(accessor2, destination)) {
    found_count++;
  }

  if (found_count == 2) {
    found = true;
  }

  if (found) {
    lg::vertex_t internal_source = accessor1->second;
    lg::vertex_t internal_destination = accessor2->second;

    success = put_livegraph_edge(internal_source, internal_destination, 0);
  }

  /*--------------------------------------------------------------------------*/

  /*UPDATE SUCCESS IF INSERTION SUCCESSFUL*/
  response["SUCCESS"] = success;

  *response_pointer = response;
}

/*
Description:
    Inserts the mirror edge for the base data.
*/
void insert_mirror_edge(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long source_tradebook_id = request["SOURCE_ID"].get<unsigned long>();
  unsigned long destination_tradebook_id = request["DESTINATION_ID"].get<unsigned long>();
  /*--------------------------------------------------------------------------*/
  /*INSERT mirror EDGE HERE*/

  //INSERT UNDIRECTED mirror EDGE BETWEEN GIVEN TRADEBOOK IDs

  int64_t source = (uint64_t) source_tradebook_id;
  uint64_t destination = (uint64_t) destination_tradebook_id;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor1, accessor2;

  bool found = false;
  bool success = false;
  int found_count = 0;

  if (GD.tradebook_vertex_dictionary->find(accessor1, source)) {
    found_count++;
  }

  if (GD.tradebook_vertex_dictionary->find(accessor2, destination)) {
    found_count++;
  }

  if (found_count == 2) {
    found = true;
  }

  if (found) {
    lg::vertex_t internal_source = accessor1->second;
    lg::vertex_t internal_destination = accessor2->second;

    success = put_livegraph_edge(internal_source, internal_destination, 1);
  }
  /*--------------------------------------------------------------------------*/

  /*UPDATE SUCCESS IF INSERTION SUCCESSFUL*/
  response["SUCCESS"] = success;

  *response_pointer = response;
}

/*
Description:
    Inserts the hasbook edge for the base data.
*/
void insert_hasbook_edge(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long source_investor_id = request["SOURCE_ID"].get<unsigned long>();
  unsigned long destination_tradebook_id = request["TRADEBOOK_ID"].get<unsigned long>();
  /*--------------------------------------------------------------------------*/
  /*INSERT hasBook EDGE HERE*/

  //INSERT UNDIRECTED hasBook EDGE BETWEEN GIVEN INVESTOR ID and TRADEBOOK ID

  uint64_t source = (uint64_t) source_investor_id;
  uint64_t destination = (uint64_t) destination_tradebook_id;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor1, accessor2;

  bool found = false;
  bool success = false;
  int found_count = 0;

  if (GD.investor_vertex_dictionary->find(accessor1, source)) {
    found_count++;
  }

  if (GD.tradebook_vertex_dictionary->find(accessor2, destination)) {
    found_count++;
  }

  if (found_count == 2) {
    found = true;
  }

  if (found) {
    lg::vertex_t internal_source = accessor1->second;
    lg::vertex_t internal_destination = accessor2->second;

    success = put_livegraph_edge(internal_source, internal_destination, 2);
  }

  /*--------------------------------------------------------------------------*/
  /*UPDATE SUCCESS IF INSERTION SUCCESSFUL*/
  response["SUCCESS"] = success;

  *response_pointer = response;
}
