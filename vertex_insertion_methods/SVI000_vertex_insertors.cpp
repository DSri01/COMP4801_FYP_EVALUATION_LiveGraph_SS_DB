/*
FYP : 22013

Module:
    Server Skeleton - Vertex Insertors

Description:
    Defines the functions that insert the vertices for the base data of the
    benchmark.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "SVI000_vertex_insertors.h"

/*
Description:
    Inserts the investor vertex for the base data.
*/
void insert_investor_vertex(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long new_investor_vertex_id = request["ID"].get<unsigned long>();
  std::string investor_name = request["NAME"].get<std::string>();

  /*--------------------------------------------------------------------------*/
  /*INSERT INVESTOR VERTEX HERE*/

  //INSERT investor VERTEX
  //INSERT investor_ID
  //INSERT investor_name

  json vertex_data;
  vertex_data["ID"] = new_investor_vertex_id;
  vertex_data["NAME"] = investor_name;

  std::string vertex_data_string = vertex_data.dump();
  const char* vertex_data_char_array = vertex_data_string.c_str();

  //lg_vertex_data will be placed in LiveGraph
  std::string_view lg_vertex_data {vertex_data_char_array, vertex_data_string.length() + 1};

  uint64_t external_id = (uint64_t) new_investor_vertex_id;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::accessor accessor;
  bool inserted = GD.investor_vertex_dictionary->insert(accessor, external_id);

  if (inserted) {
    lg::vertex_t internal_id = 0;

    internal_id = put_livegraph_vertex(lg_vertex_data);

    accessor->second = internal_id;
    GD.m_num_vertices++;
  }

  /*--------------------------------------------------------------------------*/

  /*UPDATE SUCCESS IF INSERTION SUCCESSFUL*/
  response["SUCCESS"] = inserted;

  *response_pointer = response;
}

/*
Description:
    Inserts the tradeBook vertex for the base data.
*/
void insert_tradebook_vertex(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long new_tradebook_vertex_id = request["ID"].get<unsigned long>();
  int tradebook_amount = request["AMOUNT"].get<int>();
  /*--------------------------------------------------------------------------*/
  /*INSERT TRADEBOOK VERTEX HERE*/

  //INSERT tradeBook VERTEX
  //INSERT tradeBook_ID
  //INSERT investment_amount

  json vertex_data;
  vertex_data["ID"] = new_tradebook_vertex_id;
  vertex_data["AMOUNT"] = tradebook_amount;

  std::string vertex_data_string = vertex_data.dump();
  const char* vertex_data_char_array = vertex_data_string.c_str();

  //lg_vertex_data will be placed in LiveGraph
  std::string_view lg_vertex_data {vertex_data_char_array, vertex_data_string.length() + 1};

  uint64_t external_id = (uint64_t) new_tradebook_vertex_id;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::accessor accessor;
  bool inserted = GD.tradebook_vertex_dictionary->insert(accessor, external_id);

  if (inserted) {
    lg::vertex_t internal_id = 0;

    internal_id = put_livegraph_vertex(lg_vertex_data);

    accessor->second = internal_id;
    GD.m_num_vertices++;
  }

  /*--------------------------------------------------------------------------*/

  /*UPDATE SUCCESS IF INSERTION SUCCESSFUL*/
  response["SUCCESS"] = inserted;

  *response_pointer = response;
}

/*
Description:
    Inserts the company vertex for the base data.
*/
void insert_company_vertex(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long new_company_vertex_id = request["ID"].get<unsigned long>();
  std::string company_name = request["NAME"].get<std::string>();
  /*--------------------------------------------------------------------------*/
  /*INSERT COMPANY VERTEX HERE*/

  //INSERT company VERTEX
  //INSERT company_ID
  //INSERT company_name

  json vertex_data;
  vertex_data["ID"] = new_company_vertex_id;
  vertex_data["NAME"] = company_name;

  std::string vertex_data_string = vertex_data.dump();
  const char* vertex_data_char_array = vertex_data_string.c_str();

  //lg_vertex_data will be placed in LiveGraph
  std::string_view lg_vertex_data {vertex_data_char_array, vertex_data_string.length() + 1};

  uint64_t external_id = (uint64_t) new_company_vertex_id;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::accessor accessor;

  bool inserted = GD.company_vertex_dictionary->insert(accessor, external_id);

  if (inserted) {
    lg::vertex_t internal_id = 0;

    internal_id = put_livegraph_vertex(lg_vertex_data);

    accessor->second = internal_id;
    GD.m_num_vertices++;
  }

  /*--------------------------------------------------------------------------*/

  /*UPDATE SUCCESS IF INSERTION SUCCESSFUL*/
  response["SUCCESS"] = inserted;

  *response_pointer = response;
}

/*
Description:
    Inserts the freshness score vertex for the base data.
*/
void insert_freshness_score_vertex(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long new_fs_vertex_id = request["ID"].get<unsigned long>();
  unsigned long transaction_ID = request["TRANSACTION_ID"].get<unsigned long>();
  /*--------------------------------------------------------------------------*/
  /*INSERT FRESHNESS SCORE VERTEX HERE*/

  //INSERT freshness_score VERTEX
  //INSERT transaction_ID

  json vertex_data;
  vertex_data["ID"] = new_fs_vertex_id;
  vertex_data["TRANSACTION_ID"] = transaction_ID;

  std::string vertex_data_string = vertex_data.dump();
  const char* vertex_data_char_array = vertex_data_string.c_str();

  //lg_vertex_data will be placed in LiveGraph
  std::string_view lg_vertex_data {vertex_data_char_array, vertex_data_string.length() + 1};

  uint64_t external_id = (uint64_t) new_fs_vertex_id;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::accessor accessor;
  bool inserted = GD.freshness_score_vertex_dictionary->insert(accessor, external_id);

  if (inserted) {
    lg::vertex_t internal_id = 0;

    internal_id = put_livegraph_vertex(lg_vertex_data);

    accessor->second = internal_id;
    GD.m_num_vertices++;
  }

  /*--------------------------------------------------------------------------*/

  /*UPDATE SUCCESS IF INSERTION SUCCESSFUL*/
  response["SUCCESS"] = inserted;

  *response_pointer = response;
}
