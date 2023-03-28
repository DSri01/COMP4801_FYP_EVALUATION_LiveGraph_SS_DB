/*
FYP : 22013

Module:
    Server Skeleton - Common

Description:
    Defines the common functions to be shared across the programme.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "SCD002_common_methods.h"

using json = nlohmann::json;

extern Global_Data GD;

/*
Description:
    This function is called by the analytical queries for processing.

    The implementation of this function is taken from the evaluation experiments
    conducted for the Sortledton paper. The original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/
uint64_t ext2int(uint64_t external_vertex_id) {
  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
  if (GD.investor_vertex_dictionary->find(accessor, external_vertex_id)){
    return accessor->second;
  }
  else {
    return NULL;
  }
}

/*
Description:
    This function is called by the analytical queries for processing.

    The implementation of this function is inspired from the evaluation
    experiments conducted for the Sortledton paper. The original code can be
    found at:
    https://github.com/PerFuchs/gfe_driver
*/
uint64_t int2ext(void* opaque_transaction, uint64_t internal_vertex_id) {
  auto transaction = reinterpret_cast<lg::Transaction*>(opaque_transaction);
  std::string_view payload = transaction->get_vertex(internal_vertex_id);
  if(payload.empty()){
    return std::numeric_limits<uint64_t>::max();
  }
  else {
    const char* json_char_array = (reinterpret_cast<const char*>(payload.data()));
    std::string json_string(json_char_array);
    json vertex_data;
    vertex_data = json::parse(json_string);
    unsigned long external_id = vertex_data["ID"].get<unsigned long>();
    return (uint64_t)external_id;
  }
}

/*
Description:
    This function inserts a new vertex in the LiveGraph system and returns the
    internal ID of this newly inserted vertex.

    This function is to be called only for the insertion of the base data
    vertices.
*/
lg::vertex_t put_livegraph_vertex(std::string_view lg_vertex_data) {
  lg::vertex_t internal_id = 0;
  bool done = false;

  do {
    try{
      auto tx = GD.LiveGraph->begin_transaction();
      internal_id = tx.new_vertex();
      tx.put_vertex(internal_id, lg_vertex_data);
      tx.commit();
      done = true;
    } catch(lg::Transaction::RollbackExcept& e) {
      //retry
    }
  } while(!done);

  return internal_id;
}

/*
Description:
    This function inserts a new edge in the LiveGraph system and returns if the
    edge was successfully inserted.

    This function is to be called only for the insertion of the base data edges.
*/
bool put_livegraph_edge(lg::vertex_t source, lg::vertex_t destination, lg::label_t label) {
  bool done = false;
  do {
    try {
      auto tx = GD.LiveGraph->begin_transaction();

      // insert the new edge only if it doesn't already exist
      auto lg_weight = tx.get_edge(source, label, destination);
      if(lg_weight.size() > 0){ // the edge already exists
        tx.abort();
        return false;
      }

      std::string_view weight { "1" };
      tx.put_edge(source, label, destination, weight);
      tx.put_edge(destination, label, source, weight);

      tx.commit();
      GD.m_num_edges++;
      done = true;
    } catch (lg::Transaction::RollbackExcept& exc){
      // retry ...
    }
  } while(!done);

  return true;
}
