/*
FYP : 22013

Module:
    Server Skeleton - Analytical Query Handlers

Description:
    Defines the functions that serve the analytical queries sent by the Hybrid
    Query Driver.

    This version is edited to work with the LiveGraph graph storage system.

    The implementation for all the analytical queries with LiveGraph is taken
    from the evaluation experiments conducted for the Sortledton paper. The
    original code can be found at:
    https://github.com/PerFuchs/gfe_driver
*/

#include "AQH000_analysis_handler.h"

/*
Description:
    Common function to read the transaction IDs stored in the freshness score
    vertices within the same transaction for all analytical queries.
*/
std::map<std::string, unsigned long> read_freshness_score_transaction_ids(void* opaque_transaction, unsigned long first_fs_vertex_id, int number_of_transactional_threads) {
  auto transaction = reinterpret_cast<lg::Transaction*>(opaque_transaction);

  std::map <std::string, unsigned long> freshness_score_vertex_transaction_id_map;
  for (int i = 0; i < number_of_transactional_threads; i++) {
    std::string key = "thread";
    key = key + std::to_string(i);

    unsigned long current_fs_vertex_id = first_fs_vertex_id + i;

    uint64_t external_vertex_id = (uint64_t) current_fs_vertex_id;

    lg::vertex_t internal_id = 0;

    //READ from LG
    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.freshness_score_vertex_dictionary->find(accessor, external_vertex_id)){
      internal_id = accessor->second;
    }
    else {
      freshness_score_vertex_transaction_id_map["READ_FAIL"] = 0;
      return freshness_score_vertex_transaction_id_map;
    }

    std::string_view payload = transaction->get_vertex(internal_id);

    if (payload.empty()) {
      freshness_score_vertex_transaction_id_map["READ_FAIL"] = 0;
      return freshness_score_vertex_transaction_id_map;
    }
    else {
      const char* json_char_array = (reinterpret_cast<const char*>(payload.data()));
      std::string json_string(json_char_array);
      json vertex_data;
      vertex_data = json::parse(json_string);

      //checking if the FS vertex is correctly retrieved
      if (vertex_data["ID"].get<unsigned long>() != current_fs_vertex_id) {
        freshness_score_vertex_transaction_id_map["READ_FAIL"] = 0;
        return freshness_score_vertex_transaction_id_map;
      }
      else {
        freshness_score_vertex_transaction_id_map[key] = vertex_data["TRANSACTION_ID"].get<unsigned long>();
      }
    }
  }
  return freshness_score_vertex_transaction_id_map;
}

/*
Description:
    Serves analytical query AQ_1.
*/
void perform_AQ_1(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long bfs_root_vertex_id = request["ROOT_VERTEX_ID"].get<unsigned long>();

  lg::Transaction transaction = GD.LiveGraph->begin_read_only_transaction();
  /*--------------------------------------------------------------------------*/
  /*PERFORM AQ_1: BFS HERE*/

  uint64_t max_vertex_id = MAX_INVESTORS;
  uint64_t num_vertices = MAX_INVESTORS;
  uint64_t num_edges = MAX_FRIENDS;

  uint64_t root = (uint64_t) ext2int((uint64_t) bfs_root_vertex_id);

  std::unique_ptr<int64_t[]> ptr_result = do_bfs(transaction, (uint64_t) num_vertices, (uint64_t) num_edges, (uint64_t) max_vertex_id, (uint64_t) root);

  auto external_ids = translate(&transaction, ptr_result.get(), max_vertex_id);
  /*--------------------------------------------------------------------------*/

  /****************************************************************************/
  /*PERFORM FRESHNESS SCORE STEP FOR ANALYTICAL QUERIES HERE*/

  /*READ THE TRANSACTION IDS OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
  TRANSACTIONAL THREAD ID AND PUT IT IN THE MAP: freshness_score_vertex_transaction_id_map*/

  std::map <std::string, unsigned long> freshness_score_vertex_transaction_id_map;

  unsigned long first_fs_vertex_id = request["FIRST_FRESHNESS_SCORE_ID"].get<unsigned long>();
  int number_of_transactional_threads = request["NUMBER_OF_TRANSACTIONAL_THREADS"].get<int>();

  freshness_score_vertex_transaction_id_map = read_freshness_score_transaction_ids(&transaction, first_fs_vertex_id, number_of_transactional_threads);

  /****************************************************************************/
  response["FRESHNESS_SCORE_TRANSACTION_IDS"] = freshness_score_vertex_transaction_id_map;

  /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
  if (freshness_score_vertex_transaction_id_map.find("READ_FAIL") == freshness_score_vertex_transaction_id_map.end()) {
    response["SUCCESS"] = true && true;
  }
  else{
    response["SUCCESS"] = false;
  }

  transaction.abort();

  *response_pointer = response;
}

/*
Description:
    Serves analytical query AQ_2.
*/
void perform_AQ_2(json request, json* response_pointer){
  json response = *response_pointer;

  int pr_max_iters = request["PR_MAX_ITERS"].get<int>();
  double pr_damping_factor = request["PR_DAMPING_FACTOR"].get<double>();

  lg::Transaction transaction = GD.LiveGraph->begin_read_only_transaction();
  /*--------------------------------------------------------------------------*/
  /*PERFORM AQ_2: PageRank HERE*/

  uint64_t max_vertex_id = MAX_INVESTORS;
  uint64_t num_vertices = MAX_INVESTORS;
  uint64_t num_edges = MAX_FRIENDS;


  std::unique_ptr<double[]> ptr_result = do_pagerank(transaction, num_vertices, max_vertex_id, (uint64_t) pr_max_iters, pr_damping_factor);

  auto external_ids = translate(&transaction, ptr_result.get(), max_vertex_id);

  /*--------------------------------------------------------------------------*/

  /****************************************************************************/
  /*PERFORM FRESHNESS SCORE STEP FOR ANALYTICAL QUERIES HERE*/

  /*READ THE TRANSACTION IDS OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
  TRANSACTIONAL THREAD ID AND PUT IT IN THE MAP: freshness_score_vertex_transaction_id_map*/

  std::map <std::string, unsigned long> freshness_score_vertex_transaction_id_map;

  unsigned long first_fs_vertex_id = request["FIRST_FRESHNESS_SCORE_ID"].get<unsigned long>();
  int number_of_transactional_threads = request["NUMBER_OF_TRANSACTIONAL_THREADS"].get<int>();

  freshness_score_vertex_transaction_id_map = read_freshness_score_transaction_ids(&transaction, first_fs_vertex_id, number_of_transactional_threads);
  /****************************************************************************/

  response["FRESHNESS_SCORE_TRANSACTION_IDS"] = freshness_score_vertex_transaction_id_map;

  /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
  if (freshness_score_vertex_transaction_id_map.find("READ_FAIL") == freshness_score_vertex_transaction_id_map.end()) {
    response["SUCCESS"] = true && true;
  }
  else{
    response["SUCCESS"] = false;
  }

  transaction.abort();

  *response_pointer = response;
}

/*
Description:
    Serves analytical query AQ_3.
*/
void perform_AQ_3(json request, json* response_pointer){
  json response = *response_pointer;

  lg::Transaction transaction = GD.LiveGraph->begin_read_only_transaction();
  /*--------------------------------------------------------------------------*/
  /*PERFORM AQ_3: WCC HERE*/

  uint64_t max_vertex_id = MAX_INVESTORS;
  uint64_t num_vertices = MAX_INVESTORS;
  uint64_t num_edges = MAX_FRIENDS;


  std::unique_ptr<uint64_t[]> ptr_components = do_wcc(transaction, max_vertex_id);

  auto external_ids = translate(&transaction, ptr_components.get(), max_vertex_id);
  /*--------------------------------------------------------------------------*/

  /****************************************************************************/
  /*PERFORM FRESHNESS SCORE STEP FOR ANALYTICAL QUERIES HERE*/

  /*READ THE TRANSACTION IDS OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
  TRANSACTIONAL THREAD ID AND PUT IT IN THE MAP: freshness_score_vertex_transaction_id_map*/

  std::map <std::string, unsigned long> freshness_score_vertex_transaction_id_map;

  unsigned long first_fs_vertex_id = request["FIRST_FRESHNESS_SCORE_ID"].get<unsigned long>();
  int number_of_transactional_threads = request["NUMBER_OF_TRANSACTIONAL_THREADS"].get<int>();

  freshness_score_vertex_transaction_id_map = read_freshness_score_transaction_ids(&transaction, first_fs_vertex_id, number_of_transactional_threads);

  /****************************************************************************/

  response["FRESHNESS_SCORE_TRANSACTION_IDS"] = freshness_score_vertex_transaction_id_map;

  /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
  if (freshness_score_vertex_transaction_id_map.find("READ_FAIL") == freshness_score_vertex_transaction_id_map.end()) {
    response["SUCCESS"] = true && true;
  }
  else{
    response["SUCCESS"] = false;
  }

  transaction.abort();

  *response_pointer = response;
}

/*
Description:
    Serves analytical query AQ_4.
*/
void perform_AQ_4(json request, json* response_pointer){
  json response = *response_pointer;

  lg::Transaction transaction = GD.LiveGraph->begin_read_only_transaction();
  /*--------------------------------------------------------------------------*/
  /*PERFORM AQ_4: LCC HERE*/

  uint64_t max_vertex_id = MAX_INVESTORS;
  uint64_t num_vertices = MAX_INVESTORS;
  uint64_t num_edges = MAX_FRIENDS;


  std::unique_ptr<double[]> scores = do_lcc_undirected(transaction, max_vertex_id);

  auto external_ids = translate(&transaction, scores.get(), max_vertex_id);
  /*--------------------------------------------------------------------------*/

  /****************************************************************************/
  /*PERFORM FRESHNESS SCORE STEP FOR ANALYTICAL QUERIES HERE*/

  /*READ THE TRANSACTION IDS OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
  TRANSACTIONAL THREAD ID AND PUT IT IN THE MAP: freshness_score_vertex_transaction_id_map*/

  std::map <std::string, unsigned long> freshness_score_vertex_transaction_id_map;

  unsigned long first_fs_vertex_id = request["FIRST_FRESHNESS_SCORE_ID"].get<unsigned long>();
  int number_of_transactional_threads = request["NUMBER_OF_TRANSACTIONAL_THREADS"].get<int>();

  freshness_score_vertex_transaction_id_map = read_freshness_score_transaction_ids(&transaction, first_fs_vertex_id, number_of_transactional_threads);

  /****************************************************************************/

  response["FRESHNESS_SCORE_TRANSACTION_IDS"] = freshness_score_vertex_transaction_id_map;

  /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
  if (freshness_score_vertex_transaction_id_map.find("READ_FAIL") == freshness_score_vertex_transaction_id_map.end()) {
    response["SUCCESS"] = true && true;
  }
  else{
    response["SUCCESS"] = false;
  }

  transaction.abort();

  *response_pointer = response;
}

/*
Description:
    Serves analytical query AQ_5.
*/
void perform_AQ_5(json request, json* response_pointer){
  json response = *response_pointer;

  int cdlp_max_iters = request["CDLP_MAX_ITERS"].get<int>();

  lg::Transaction transaction = GD.LiveGraph->begin_read_only_transaction();
  /*--------------------------------------------------------------------------*/
  /*PERFORM AQ_5: CDLP HERE*/

  uint64_t max_vertex_id = MAX_INVESTORS;
  uint64_t num_vertices = MAX_INVESTORS;
  uint64_t num_edges = MAX_FRIENDS;


  std::unique_ptr<uint64_t[]> labels = do_cdlp(transaction, max_vertex_id, false, (uint64_t) cdlp_max_iters);

  auto external_ids = translate(&transaction, labels.get(), max_vertex_id);
  /*--------------------------------------------------------------------------*/

  /****************************************************************************/
  /*PERFORM FRESHNESS SCORE STEP FOR ANALYTICAL QUERIES HERE*/

  /*READ THE TRANSACTION IDS OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
  TRANSACTIONAL THREAD ID AND PUT IT IN THE MAP: freshness_score_vertex_transaction_id_map*/

  std::map <std::string, unsigned long> freshness_score_vertex_transaction_id_map;

  unsigned long first_fs_vertex_id = request["FIRST_FRESHNESS_SCORE_ID"].get<unsigned long>();
  int number_of_transactional_threads = request["NUMBER_OF_TRANSACTIONAL_THREADS"].get<int>();

  freshness_score_vertex_transaction_id_map = read_freshness_score_transaction_ids(&transaction, first_fs_vertex_id, number_of_transactional_threads);

  /****************************************************************************/

  response["FRESHNESS_SCORE_TRANSACTION_IDS"] = freshness_score_vertex_transaction_id_map;

  /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
  if (freshness_score_vertex_transaction_id_map.find("READ_FAIL") == freshness_score_vertex_transaction_id_map.end()) {
    response["SUCCESS"] = true && true;
  }
  else{
    response["SUCCESS"] = false;
  }

  transaction.abort();

  *response_pointer = response;
}

/*
Description:
    Serves analytical query AQ_6.
*/
void perform_AQ_6(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long sssp_root_vertex_id = request["ROOT_VERTEX_ID"].get<unsigned long>();

  lg::Transaction transaction = GD.LiveGraph->begin_read_only_transaction();
  /*--------------------------------------------------------------------------*/
  /*PERFORM AQ_6: SSSP HERE*/

  uint64_t max_vertex_id = MAX_INVESTORS;
  uint64_t num_vertices = MAX_INVESTORS;
  uint64_t num_edges = MAX_FRIENDS;

  uint64_t root = (uint64_t) ext2int((uint64_t) sssp_root_vertex_id);

  double delta = 2.0; // same value used in the GAPBS, at least for most graphs
  auto distances = do_sssp(transaction, num_edges, max_vertex_id, root, delta);

  auto external_ids = translate(&transaction, distances.data(), max_vertex_id);

  /*--------------------------------------------------------------------------*/

  /****************************************************************************/
  /*PERFORM FRESHNESS SCORE STEP FOR ANALYTICAL QUERIES HERE*/

  /*READ THE TRANSACTION IDS OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
  TRANSACTIONAL THREAD ID AND PUT IT IN THE MAP: freshness_score_vertex_transaction_id_map*/

  std::map <std::string, unsigned long> freshness_score_vertex_transaction_id_map;

  unsigned long first_fs_vertex_id = request["FIRST_FRESHNESS_SCORE_ID"].get<unsigned long>();
  int number_of_transactional_threads = request["NUMBER_OF_TRANSACTIONAL_THREADS"].get<int>();

  freshness_score_vertex_transaction_id_map = read_freshness_score_transaction_ids(&transaction, first_fs_vertex_id, number_of_transactional_threads);

  /****************************************************************************/

  response["FRESHNESS_SCORE_TRANSACTION_IDS"] = freshness_score_vertex_transaction_id_map;

  /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
  if (freshness_score_vertex_transaction_id_map.find("READ_FAIL") == freshness_score_vertex_transaction_id_map.end()) {
    response["SUCCESS"] = true && true;
  }
  else{
    response["SUCCESS"] = false;
  }

  transaction.abort();

  *response_pointer = response;
}
