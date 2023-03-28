/*
FYP : 22013

Module:
    Server Skeleton - Transaction Query Handlers

Description:
    Defines the functions that serve the transactional queries sent by the
    Hybrid Query Driver.

    This version is edited to work with the LiveGraph graph storage system.
*/

#include "TQH000_transaction_handler.h"

/*
Description:
    Common function to update the freshness score vertex within the same
    transaction for all transactional queries.
*/
bool update_freshness_score_vertex(void* opaque_transaction, unsigned long fs_vertex_to_update, unsigned long fs_transaction_id) {
  auto transaction = reinterpret_cast<lg::Transaction*>(opaque_transaction);

  lg::vertex_t internal_id = 0;

  uint64_t external_vertex_id = (uint64_t) fs_vertex_to_update;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
  if (GD.freshness_score_vertex_dictionary->find(accessor, external_vertex_id)){
    internal_id = accessor->second;
  }
  else {
    return false;
  }

  std::string_view payload = transaction->get_vertex(internal_id);

  if (payload.empty()) {
    return false;
  }
  else {
    const char* json_char_array = (reinterpret_cast<const char*>(payload.data()));
    std::string json_string(json_char_array);
    json vertex_data;
    vertex_data = json::parse(json_string);

    //checking if the FS vertex is correctly retrieved
    if (vertex_data["ID"].get<unsigned long>() != fs_vertex_to_update) {
      return false;
    }
    else {
      vertex_data["TRANSACTION_ID"] = fs_transaction_id;

      std::string vertex_data_string = vertex_data.dump();
      const char* vertex_data_char_array = vertex_data_string.c_str();

      //lg_vertex_data will be placed in LiveGraph
      std::string_view lg_vertex_data {vertex_data_char_array, vertex_data_string.length() + 1};

      bool done = false;
      do {
        try{
          transaction->put_vertex(internal_id, lg_vertex_data);
          done = true;
        } catch(lg::Transaction::RollbackExcept& e) {
          //retry
        }
      } while(!done);

      return true;
    }
  }
  return false;
}

/*
Description:
    Serves transactional query TQ_1_A.
*/
void perform_TQ_1_A(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long new_trade_id = request["NEW_TRADE_ID"].get<unsigned long>();
  unsigned long original_investor_ID = request["ORIGINAL_INVESTOR_ID"].get<unsigned long>();
  uint64_t original_timestamp = request["ORIGINAL_TIMESTAMP"].get<uint64_t>();

  std::map <std::string, unsigned long> company_id_map = request["COMPANY_ID_MAP"].get<std::map <std::string, unsigned long>>();
  std::map <std::string, int> company_quantity_map = request["COMPANY_QUANTITY_MAP"].get<std::map <std::string, int>>();
  std::map <std::string, bool> company_action_map = request["COMPANY_ACTION_MAP"].get<std::map <std::string, bool>>();

  response["SUCCESS"] = false;

  int attempts = 0;
  /*--------------------------------------------------------------------------*/
  /*PERFORM TQ_1_A: MAKE ORIGINAL TRADE HERE*/

  //ADD TRADE VERTEX
  //ADD trades EDGES
  //ADD contains EDGES

  auto tx = GD.LiveGraph->begin_transaction();

  //Insert TRADE VERTEX

  json new_trade_vertex_data;
  new_trade_vertex_data["ID"] = new_trade_id;
  new_trade_vertex_data["ORIGINAL_INVESTOR_ID"] = original_investor_ID;
  new_trade_vertex_data["ORIGINAL_TIMESTAMP"] = original_timestamp;

  std::string vertex_data_string = new_trade_vertex_data.dump();
  const char* vertex_data_char_array = vertex_data_string.c_str();

  //lg_vertex_data will be placed in LiveGraph
  std::string_view lg_vertex_data {vertex_data_char_array, vertex_data_string.length() + 1};

  uint64_t trade_external_vertex_id = (uint64_t) new_trade_id;

  lg::vertex_t new_trade_internal_id = 0;

  tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::accessor accessor;
  bool inserted = GD.trade_vertex_dictionary->insert(accessor, trade_external_vertex_id);

  if (inserted) {
    bool tq_1_a_success = false;
    try{
      attempts++;
      new_trade_internal_id = tx.new_vertex();
      tx.put_vertex(new_trade_internal_id, lg_vertex_data);
      accessor->second = new_trade_internal_id;
      GD.m_num_vertices++;
      //Insert trades EDGE both directions with company
      bool inserted_all_trades_edges = true;
      for (auto const& x :company_id_map) {
        bool inserted_this_trades_edge = false;
        std::string key = x.first;

        unsigned long company_external_id = x.second;
        int quantity = company_quantity_map[x.first];
        bool action = company_action_map[x.first];

        json trades_edge_data;
        trades_edge_data["QUANTITY"] = quantity;
        trades_edge_data["ACTION"] = action;

        std::string trades_edge_data_string = trades_edge_data.dump();
        const char* trades_edge_data_char_array = trades_edge_data_string.c_str();

        std::string_view lg_trades_edge_data {trades_edge_data_char_array, trades_edge_data_string.length() + 1};

        //GET company internal id
        lg::vertex_t company_internal_id = 0;

        tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor company_accessor;
        if (GD.company_vertex_dictionary->find(company_accessor, company_external_id)) {
          company_internal_id = company_accessor->second;

          if (tx.get_edge(company_internal_id, 4, new_trade_internal_id).size() <= 0) {
            tx.put_edge(company_internal_id, 4, new_trade_internal_id, lg_trades_edge_data);
            tx.put_edge(new_trade_internal_id, 4, company_internal_id, lg_trades_edge_data);
            inserted_this_trades_edge = true;
          }
        }
        inserted_all_trades_edges = inserted_all_trades_edges && inserted_this_trades_edge;
      }

      if (inserted_all_trades_edges) {
        //Insert contains EDGE both directions with tradebooks
        tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor investor_accessor;
        if (GD.investor_vertex_dictionary->find(investor_accessor, original_investor_ID)) {
          lg::vertex_t original_investor_internal_id = investor_accessor->second;

          auto original_tradebook_iterator = tx.get_edges(original_investor_internal_id, 2);

          int original_tradebook_edge_count = 0;

          lg::vertex_t original_tradebook_internal_id = 0;

          while (original_tradebook_iterator.valid()) {
            original_tradebook_edge_count++;
            original_tradebook_internal_id = original_tradebook_iterator.dst_id();
            original_tradebook_iterator.next();
          }

          if (original_tradebook_edge_count == 1) {
            // add contains edge here

            json contains_edge_data;
            contains_edge_data["TIMESTAMP"] = original_timestamp;

            std::string contains_edge_data_string = contains_edge_data.dump();
            const char* contains_edge_data_char_array = contains_edge_data_string.c_str();

            std::string_view lg_contains_edge_data {contains_edge_data_char_array, contains_edge_data_string.length() + 1};

            if (tx.get_edge(original_tradebook_internal_id, 3, new_trade_internal_id).size() <= 0) {
              tx.put_edge(original_tradebook_internal_id, 3, new_trade_internal_id, lg_contains_edge_data);
              tx.put_edge(new_trade_internal_id, 3, original_tradebook_internal_id, lg_contains_edge_data);
              tq_1_a_success = true;
            }
            // add contains edge for iterated tradebooks

            auto mirror_iterator = tx.get_edges(original_tradebook_internal_id, 1);

            while (mirror_iterator.valid()) {
              lg::vertex_t mirrorer_tradebook_internal_id = mirror_iterator.dst_id();
              if (tx.get_edge(mirrorer_tradebook_internal_id, 3, new_trade_internal_id).size() <= 0) {
                tx.put_edge(mirrorer_tradebook_internal_id, 3, new_trade_internal_id, lg_contains_edge_data);
                tx.put_edge(new_trade_internal_id, 3, mirrorer_tradebook_internal_id, lg_contains_edge_data);
              }
              else {
                tq_1_a_success = false;
                break;
              }
              mirror_iterator.next();
            }
          }
        }
      }

      /*--------------------------------------------------------------------------*/

      /****************************************************************************/

      /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

      /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
      TRANSACTIONAL THREAD ID*/
      unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
      unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

      bool fs_update_success = false;
      fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
      /****************************************************************************/

      /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
      response["SUCCESS"] = fs_update_success && tq_1_a_success;
      if (response["SUCCESS"].get<bool>()) {
        tx.commit();
      }
      else {
        tx.abort();
      }

    } catch(lg::Transaction::RollbackExcept& e) {
      std::cout<<"TQ1_A ROLLBACK EXCEPTION:"<<e.what()<<"\n";

      response["SUCCESS"] = false;
    }
  }

  *response_pointer = response;
}

/*
Description:
    Serves transactional query TQ_1_B.
*/
void perform_TQ_1_B(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long trade_id_to_copy = request["TRADE_ID_TO_COPY"].get<unsigned long>();
  unsigned long copier_investor_ID = request["COPIER_INVESTOR_ID"].get<unsigned long>();
  uint64_t current_timestamp = request["CURRENT_TIMESTAMP"].get<uint64_t>();
  try{
    bool tq_1_b_success = false;

    /*--------------------------------------------------------------------------*/
    /*PERFORM TQ_1_B: MAKE COPY TRADE HERE*/

    //ADD contains EDGES (OR UPDATE THEM IF THEY ALREADY EXIST)

    uint64_t trade_external_id = (uint64_t) trade_id_to_copy;

    uint64_t copier_investor_external_id = (uint64_t) copier_investor_ID;

    lg::vertex_t trade_internal_id = 0;

    lg::vertex_t copier_investor_internal_id = 0;

    auto tx = GD.LiveGraph->begin_transaction();

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor investor_accessor;
    if(GD.investor_vertex_dictionary->find(investor_accessor, copier_investor_external_id)) {
      copier_investor_internal_id = investor_accessor->second;

        tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor trade_accessor;
        if (GD.trade_vertex_dictionary->find(trade_accessor, trade_external_id)) {
          trade_internal_id = trade_accessor->second;

        auto original_tradebook_iterator = tx.get_edges(copier_investor_internal_id, 2);

        int original_tradebook_edge_count = 0;

        lg::vertex_t original_tradebook_internal_id = 0;

        while (original_tradebook_iterator.valid()) {
          original_tradebook_edge_count++;
          original_tradebook_internal_id = original_tradebook_iterator.dst_id();
          original_tradebook_iterator.next();
        }

        if (original_tradebook_edge_count == 1) {
          // add contains edge here

          json contains_edge_data;
          contains_edge_data["TIMESTAMP"] = current_timestamp;

          std::string contains_edge_data_string = contains_edge_data.dump();
          const char* contains_edge_data_char_array = contains_edge_data_string.c_str();

          std::string_view lg_contains_edge_data {contains_edge_data_char_array, contains_edge_data_string.length() + 1};

          if (tx.get_edge(original_tradebook_internal_id, 3, trade_internal_id).size() > 0) {
            tx.del_edge(trade_internal_id, 3, original_tradebook_internal_id);
            tx.del_edge(original_tradebook_internal_id, 3, trade_internal_id);
          }

          tx.put_edge(original_tradebook_internal_id, 3, trade_internal_id, lg_contains_edge_data, true);
          tx.put_edge(trade_internal_id, 3, original_tradebook_internal_id, lg_contains_edge_data, true);

          tq_1_b_success = true;

          // add contains edge for iterated tradebooks

          auto mirror_iterator = tx.get_edges(original_tradebook_internal_id, 1);

          while (mirror_iterator.valid()) {
            lg::vertex_t mirrorer_tradebook_internal_id = mirror_iterator.dst_id();

            if (tx.get_edge(trade_internal_id, 3, mirrorer_tradebook_internal_id).size() > 0) {
              tx.del_edge(trade_internal_id, 3, mirrorer_tradebook_internal_id);
              tx.del_edge(mirrorer_tradebook_internal_id, 3, trade_internal_id);
            }

            tx.put_edge(mirrorer_tradebook_internal_id, 3, trade_internal_id, lg_contains_edge_data, true);
            tx.put_edge(trade_internal_id, 3, mirrorer_tradebook_internal_id, lg_contains_edge_data, true);

            mirror_iterator.next();
          }
        }
      }
    }
    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/

    /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
    response["SUCCESS"] = fs_update_success && tq_1_b_success;

    if (response["SUCCESS"].get<bool>()) {
      tx.commit();
    }
    else {
      tx.abort();
    }

  } catch(lg::Transaction::RollbackExcept& e) {
    std::cout<<"TQ1_B ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    response["SUCCESS"] = false;
  }

  *response_pointer = response;
}

/*
Description:
    Serves transactional query TQ_2.
*/
void perform_TQ_2(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long read_investor_ID = request["READ_INVESTOR_ID"].get<unsigned long>();

  try {
    bool tq_2_success = false;
    /*--------------------------------------------------------------------------*/
    /*PERFORM TQ_2: READ INVESTOR PROFILE HERE*/

    //READ DATA AND UPDATE IT IN THE RESPONSE

    lg::vertex_t internal_id = 0;

    uint64_t external_vertex_id = (uint64_t) read_investor_ID;

    auto tx = GD.LiveGraph->begin_transaction();

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.investor_vertex_dictionary->find(accessor, external_vertex_id)) {

      internal_id = accessor->second;

      std::string_view payload = tx.get_vertex(internal_id);

      if (!payload.empty()) {
        const char* json_char_array = (reinterpret_cast<const char*>(payload.data()));
        std::string json_string(json_char_array);
        json investor_vertex_data;
        investor_vertex_data = json::parse(json_string);

        if (investor_vertex_data["ID"].get<unsigned long>() == read_investor_ID) {
          response["INVESTOR_NAME"] = investor_vertex_data["NAME"].get<std::string>();

          //Look for tradebook

          auto iterator = tx.get_edges(internal_id, 2);

          int tradebook_edge_count = 0;

          int investment_amount = 0;

          while(iterator.valid()) {
            tradebook_edge_count++;
            uint64_t tradebook_internal_id = iterator.dst_id();

            std::string_view tradebook_payload = tx.get_vertex(tradebook_internal_id);

            if (!tradebook_payload.empty()) {
              const char* tb_json_char_array = (reinterpret_cast<const char*>(tradebook_payload.data()));
              std::string tb_json_string(tb_json_char_array);
              json tradebook_vertex_data;
              tradebook_vertex_data = json::parse(tb_json_string);

              investment_amount = tradebook_vertex_data["AMOUNT"].get<int>();
            }

            iterator.next();
          }

          if (tradebook_edge_count == 1) {
            response["INVESTMENT_AMOUNT"] = investment_amount;
            tq_2_success = true;
          }
        }
      }
    }
    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/

    if (fs_update_success && tq_2_success) {
      tx.commit();
    }
    else{
      response["INVESTOR_NAME"] = "investor_name";
      response["INVESTMENT_AMOUNT"] = 0;
      tx.abort();
    }

    response["SUCCESS"] = fs_update_success && tq_2_success;

  } catch (lg::Transaction::RollbackExcept& e){
    std::cout<<"TQ2 ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    response["INVESTOR_NAME"] = "investor_name";
    response["INVESTMENT_AMOUNT"] = 0;
    response["SUCCESS"] = false;
  }

  *response_pointer = response;
}

/*
Description:
    Serves transactional query TQ_3.
*/
void perform_TQ_3(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long read_investor_ID = request["READ_INVESTOR_ID"].get<unsigned long>();

  try{
    bool tq_3_success = false;
    /*--------------------------------------------------------------------------*/
    /*PERFORM TQ_3: READ INVESTOR'S MOST RECENT TRADE HERE*/

    //READ DATA AND UPDATE IT IN THE RESPONSE

    lg::vertex_t investor_internal_id = 0;

    uint64_t investor_external_vertex_id = (uint64_t) read_investor_ID;

    auto tx = GD.LiveGraph->begin_transaction();

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.investor_vertex_dictionary->find(accessor, investor_external_vertex_id)) {

      investor_internal_id = accessor->second;

      //Look for tradebook
      auto hasbook_iterator = tx.get_edges(investor_internal_id, 2);

      int tradebook_edge_count = 0;

      lg::vertex_t tradebook_internal_id = 0;

      while(hasbook_iterator.valid()) {
        tradebook_edge_count++;
        tradebook_internal_id = hasbook_iterator.dst_id();
        hasbook_iterator.next();
      }

      if (tradebook_edge_count == 1) {
        //look for trade
        auto contains_iterator = tx.get_edges(tradebook_internal_id, 3);

        int trade_edge_count = 0;

        lg::vertex_t result_recent_trade_internal_id = 0;

        uint64_t latest_timestamp = 0;

        while (contains_iterator.valid()) {
          trade_edge_count++;

          lg::vertex_t trade_internal_id = contains_iterator.dst_id();

          std::string_view contains_edge_payload = tx.get_edge(tradebook_internal_id, 3, trade_internal_id);

          if (!contains_edge_payload.empty()) {
            const char* ce_json_char_array = (reinterpret_cast<const char*>(contains_edge_payload.data()));
            std::string ce_json_string(ce_json_char_array);
            json contains_edge_data;
            contains_edge_data = json::parse(ce_json_string);

            if (contains_edge_data["TIMESTAMP"].get<uint64_t>() > latest_timestamp) {
              result_recent_trade_internal_id = trade_internal_id;
              latest_timestamp = contains_edge_data["TIMESTAMP"].get<uint64_t>();
            }
          }
          contains_iterator.next();
        }

        if (trade_edge_count == 0) {
          response["HAS_TRADED"] = false;
          response["LATEST_TRADE_ID"] = 0;
          tq_3_success = true;
        }
        else{
          response["HAS_TRADED"] = true;
          std::string_view result_trade_payload = tx.get_vertex(result_recent_trade_internal_id);

          if (!result_trade_payload.empty()) {
            const char* rt_json_char_array = (reinterpret_cast<const char*>(result_trade_payload.data()));
            std::string rt_json_string(rt_json_char_array);
            json trade_vertex_data;
            trade_vertex_data = json::parse(rt_json_string);

            response["LATEST_TRADE_ID"] = trade_vertex_data["ID"].get<unsigned long>();
            tq_3_success = true;
          }
          else {
            tq_3_success = false;
          }
        }
      }
    }


    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/

    /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/

    if (fs_update_success && tq_3_success) {
      tx.commit();
    }
    else{
      response["HAS_TRADED"] = false;
      response["LATEST_TRADE_ID"] = 0;
      tx.abort();
    }

    response["SUCCESS"] = fs_update_success && tq_3_success;

    *response_pointer = response;
  } catch (lg::Transaction::RollbackExcept& e){
    std::cout<<"TQ3 ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    response["SUCCESS"] = false;
    response["HAS_TRADED"] = false;
    response["LATEST_TRADE_ID"] = 0;

    *response_pointer = response;
  }
}

/*
Description:
    Serves transactional query TQ_4.
*/
void perform_TQ_4(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long read_company_ID = request["READ_COMPANY_ID"].get<unsigned long>();

  bool tq4_success = false;
  /*--------------------------------------------------------------------------*/
  /*PERFORM TQ_4: READ COMPANY PROFILE HERE*/

  //READ DATA AND UPDATE IT IN THE RESPONSE

  try{
    auto tx = GD.LiveGraph->begin_transaction();

    lg::vertex_t company_internal_id = 0;

    uint64_t company_external_vertex_id = (uint64_t) read_company_ID;

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.company_vertex_dictionary->find(accessor, company_external_vertex_id)) {
      company_internal_id = accessor->second;

      std::string_view company_payload = tx.get_vertex(company_internal_id);

      if (!company_payload.empty()) {
        const char* c_json_char_array = (reinterpret_cast<const char*>(company_payload.data()));
        std::string c_json_string(c_json_char_array);
        json company_vertex_data;
        company_vertex_data = json::parse(c_json_string);
        if (company_vertex_data["ID"].get<unsigned long>() == read_company_ID) {
          response["COMPANY_NAME"] = company_vertex_data["NAME"].get<std::string>();
          tq4_success = true;
        }
      }
    }
    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/

    /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
    response["SUCCESS"] = fs_update_success && tq4_success;
    if (response["SUCCESS"].get<bool>()) {
      tx.commit();
    }
    else {
      response["COMPANY_NAME"] = "read_company_name";
      tx.abort();
    }
    *response_pointer = response;
  } catch (lg::Transaction::RollbackExcept& e){
    std::cout<<"TQ4 ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    response["SUCCESS"] = false;
    response["COMPANY_NAME"] = "read_company_name";

    *response_pointer = response;
  }
}

/*
Description:
    Serves transactional query TQ_5.
*/
void perform_TQ_5(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long read_investor_ID = request["READ_INVESTOR_ID"].get<unsigned long>();

  bool tq5_success = false;
  /*--------------------------------------------------------------------------*/
  /*PERFORM TQ_5: READ ALL MIRRORING INVESTORS HERE*/

  //READ DATA AND STORE THE IDs IN THE VECTOR: mirror_id_vector
  std::vector <unsigned long> mirror_id_vector;

  try{

    auto tx = GD.LiveGraph->begin_transaction();

    lg::vertex_t orig_investor_internal_id = 0;

    uint64_t orig_investor_external_id = (uint64_t) read_investor_ID;

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.investor_vertex_dictionary->find(accessor, orig_investor_external_id)) {
      orig_investor_internal_id = accessor->second;

      auto original_tradebook_iterator = tx.get_edges(orig_investor_internal_id, 2);

      int tradebook_edge_count = 0;

      lg::vertex_t original_tradebook_id = 0;

      while (original_tradebook_iterator.valid()) {
        tradebook_edge_count++;
        original_tradebook_id = original_tradebook_iterator.dst_id();
        original_tradebook_iterator.next();
      }

      if (tradebook_edge_count == 1) {
        tq5_success = true;

        auto mirrorer_iterator = tx.get_edges(original_tradebook_id, 1);

        while (mirrorer_iterator.valid()) {

          lg::vertex_t mirrorer_tradebook_internal_id = mirrorer_iterator.dst_id();

          auto mirrorer_hasbook_iterator = tx.get_edges(mirrorer_tradebook_internal_id, 2);

          lg::vertex_t mirrorer_investor_internal_id = 0;

          while(mirrorer_hasbook_iterator.valid()) {
            mirrorer_investor_internal_id = mirrorer_hasbook_iterator.dst_id();
            mirrorer_hasbook_iterator.next();
          }

          std::string_view investor_payload = tx.get_vertex(mirrorer_investor_internal_id);

          if (!investor_payload.empty()) {
            const char* i_json_char_array = (reinterpret_cast<const char*>(investor_payload.data()));
            std::string i_json_string(i_json_char_array);
            json investor_vertex_data;
            investor_vertex_data = json::parse(i_json_string);

            mirror_id_vector.push_back(investor_vertex_data["ID"].get<unsigned long>());
          }

          mirrorer_iterator.next();
        }
      }
    }
    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/

    response["SUCCESS"] = fs_update_success && tq5_success;

    if (response["SUCCESS"].get<bool>()) {
      tx.commit();
      response["MIRRORING_INVESTOR_LIST"] = mirror_id_vector;
    }
    else {
      tx.abort();
      std::vector <unsigned long> empty_mirror_id_vector;
      response["MIRRORING_INVESTOR_LIST"] = empty_mirror_id_vector;
    }

    *response_pointer = response;

  } catch (lg::Transaction::RollbackExcept& e){
    std::cout<<"TQ5 ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    response["SUCCESS"] = false;
    std::vector <unsigned long> empty_mirror_id_vector;
    response["MIRRORING_INVESTOR_LIST"] = empty_mirror_id_vector;

    *response_pointer = response;
  }
}

/*
Description:
    Serves transactional query TQ_6.
*/
void perform_TQ_6(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long read_investor_ID = request["READ_INVESTOR_ID"].get<unsigned long>();
  try{
    bool tq_6_success = false;
    /*--------------------------------------------------------------------------*/
    /*PERFORM TQ_6: READ MOST RECENT TRADE OF IMMEDIATE FRIENDS HERE*/

    //READ DATA AND STORE THE IDs IN THE VECTOR: friend_trade_id_vector

    auto tx = GD.LiveGraph->begin_transaction();

    std::vector <unsigned long> friend_trade_id_vector;

    lg::vertex_t original_investor_internal_id = 0;

    uint64_t original_investor_external_id = (uint64_t) read_investor_ID;

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.investor_vertex_dictionary->find(accessor, original_investor_external_id)) {
      original_investor_internal_id = accessor->second;

      tq_6_success = true;

      auto friend_iterator = tx.get_edges(original_investor_internal_id, 0);

      while (friend_iterator.valid()) {
        lg::vertex_t current_friend_internal_id = friend_iterator.dst_id();

        lg::vertex_t current_tradebook_internal_id = 0;

        auto hasbook_iterator = tx.get_edges(current_friend_internal_id, 2);

        int tradebook_edge_count = 0;

        while (hasbook_iterator.valid()) {
          tradebook_edge_count++;
          current_tradebook_internal_id = hasbook_iterator.dst_id();
          hasbook_iterator.next();
        }

        if (tradebook_edge_count == 1) {
          auto contains_iterator = tx.get_edges(current_tradebook_internal_id, 3);

          int trade_edge_count = 0;

          lg::vertex_t result_recent_trade_internal_id = 0;

          uint64_t latest_timestamp = 0;

          while (contains_iterator.valid()) {
            trade_edge_count++;

            lg::vertex_t trade_internal_id = contains_iterator.dst_id();

            std::string_view contains_edge_payload = tx.get_edge(current_tradebook_internal_id, 3, trade_internal_id);

            if (!contains_edge_payload.empty()) {
              const char* ce_json_char_array = (reinterpret_cast<const char*>(contains_edge_payload.data()));
              std::string ce_json_string(ce_json_char_array);
              json contains_edge_data;
              contains_edge_data = json::parse(ce_json_string);

              if (contains_edge_data["TIMESTAMP"].get<uint64_t>() > latest_timestamp) {
                result_recent_trade_internal_id = trade_internal_id;
                latest_timestamp = contains_edge_data["TIMESTAMP"].get<uint64_t>();
              }
            }
            contains_iterator.next();
          }

          if (trade_edge_count > 0) {
            std::string_view result_trade_payload = tx.get_vertex(result_recent_trade_internal_id);

            if (!result_trade_payload.empty()) {
              const char* rt_json_char_array = (reinterpret_cast<const char*>(result_trade_payload.data()));
              std::string rt_json_string(rt_json_char_array);
              json trade_vertex_data;
              trade_vertex_data = json::parse(rt_json_string);

              friend_trade_id_vector.push_back(trade_vertex_data["ID"].get<unsigned long>());
            }
          }
        }

        friend_iterator.next();
      }
    }

    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/

    response["FRIEND_TRADE_LIST"] = friend_trade_id_vector;

    /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
    response["SUCCESS"] = fs_update_success && tq_6_success;

    if (response["SUCCESS"].get<bool>()) {
      tx.commit();
    }
    else{
      std::vector <unsigned long> empty_friend_trade_id_vector;
      response["FRIEND_TRADE_LIST"] = empty_friend_trade_id_vector;
      response["SUCCESS"] = false;
      tx.abort();
    }

  } catch (lg::Transaction::RollbackExcept& e){
    std::cout<<"TQ6 ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    std::vector <unsigned long> empty_friend_trade_id_vector;
    response["FRIEND_TRADE_LIST"] = empty_friend_trade_id_vector;
    response["SUCCESS"] = false;
  }

  *response_pointer = response;
}

/*
Description:
    Serves transactional query TQ_7.
*/
void perform_TQ_7(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long update_investor_ID = request["UPDATE_INVESTOR_ID"].get<unsigned long>();
  int new_investment_amount = request["NEW_INVESTMENT_AMOUNT"].get<int>();

  try{
    bool tq_7_success = false;
    /*--------------------------------------------------------------------------*/
    /*PERFORM TQ_7: UPDATE INVESTMENT AMOUNT HERE*/

    //UPDATE TradeBook investment amount WITH NEW AMOUNT

    auto tx = GD.LiveGraph->begin_transaction();

    lg::vertex_t investor_internal_id = 0;

    uint64_t investor_external_vertex_id = (uint64_t) update_investor_ID;

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.investor_vertex_dictionary->find(accessor, investor_external_vertex_id)) {
      investor_internal_id = accessor->second;

      auto hasbook_iterator = tx.get_edges(investor_internal_id, 2);

      int tradebook_edge_count = 0;

      lg::vertex_t tradebook_internal_id = 0;

      while(hasbook_iterator.valid()) {
        tradebook_edge_count++;
        tradebook_internal_id = hasbook_iterator.dst_id();
        hasbook_iterator.next();
      }

      if (tradebook_edge_count == 1) {
        std::string_view tradebook_stale_payload = tx.get_vertex(tradebook_internal_id);

        if (!tradebook_stale_payload.empty()) {
          const char* json_char_array = (reinterpret_cast<const char*>(tradebook_stale_payload.data()));
          std::string json_string(json_char_array);
          json tradebook_vertex_data;
          tradebook_vertex_data = json::parse(json_string);

          tradebook_vertex_data["AMOUNT"] = new_investment_amount;

          std::string vertex_data_string = tradebook_vertex_data.dump();
          const char* vertex_data_char_array = vertex_data_string.c_str();

          //lg_vertex_data will be placed in LiveGraph
          std::string_view lg_vertex_data {vertex_data_char_array, vertex_data_string.length() + 1};

          tx.put_vertex(tradebook_internal_id, lg_vertex_data);

          tq_7_success = true;
        }
      }
    }

    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/


    /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
    response["SUCCESS"] = fs_update_success && tq_7_success;

    if (response["SUCCESS"].get<bool>()) {
      tx.commit();
    }
    else{
      tx.abort();
    }

  } catch (lg::Transaction::RollbackExcept& e){
    std::cout<<"TQ7 ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    response["SUCCESS"] = false;
  }

  *response_pointer = response;
}

/*
Description:
    Serves transactional query TQ_8.
*/
void perform_TQ_8(json request, json* response_pointer){
  json response = *response_pointer;

  unsigned long source_tradebook_ID = request["SOURCE_TRADEBOOK_ID"].get<unsigned long>();
  unsigned long destination_tradebook_ID = request["DESTINATION_TRADEBOOK_ID"].get<unsigned long>();
  try {
    bool tq_8_success = false;
    /*--------------------------------------------------------------------------*/
    /*PERFORM TQ_8: REMOVE MIRROR EDGE HERE*/

    //REMOVE mirrors EDGE in both directions

    auto tx = GD.LiveGraph->begin_transaction();

    uint64_t source_external_id = (uint64_t) source_tradebook_ID;

    uint64_t destination_external_id = (uint64_t) destination_tradebook_ID;

    lg::vertex_t source_internal_id = 0;

    lg::vertex_t destination_internal_id = 0;

    int found_vertex_count = 0;

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor;
    if (GD.tradebook_vertex_dictionary->find(accessor, source_external_id)) {
      source_internal_id = accessor->second;
      found_vertex_count++;
    }

    tbb::concurrent_hash_map<uint64_t, lg::vertex_t>::const_accessor accessor2;
    if (GD.tradebook_vertex_dictionary->find(accessor2, destination_external_id)) {
      destination_internal_id = accessor2->second;
      found_vertex_count++;
    }

    if (found_vertex_count == 2) {
      tq_8_success = true;
      tx.del_edge(source_internal_id, 1, destination_internal_id);
      tx.del_edge(destination_internal_id, 1, source_internal_id);
    }

    /*--------------------------------------------------------------------------*/

    /****************************************************************************/

    /*PERFORM FRESHNESS SCORE STEP FOR TRANSACTIONS HERE*/

    /*UPDATE THE TRANSACTION ID OF THE FRESHNESS SCORE VERTEX CORRESPONDING TO THE
    TRANSACTIONAL THREAD ID*/
    unsigned long fs_vertex_to_update = request["FRESHNESS_SCORE_ID"].get<unsigned long>();
    unsigned long fs_transaction_id = request["FS_TRANSACTION_ID"].get<unsigned long>();

    bool fs_update_success = false;
    fs_update_success = update_freshness_score_vertex(&tx, fs_vertex_to_update, fs_transaction_id);
    /****************************************************************************/


    /*UPDATE SUCCESS IF BOTH OPERATIONS ARE SUCCESSFUL*/
    response["SUCCESS"] = fs_update_success && tq_8_success;
    if (response["SUCCESS"].get<bool>()) {
      tx.commit();
    }
    else{
      tx.abort();
    }

  } catch (lg::Transaction::RollbackExcept& e){
    std::cout<<"TQ8 ROLLBACK EXCEPTION:"<<e.what()<<"\n";

    response["SUCCESS"] = false;
  }

  *response_pointer = response;
}
