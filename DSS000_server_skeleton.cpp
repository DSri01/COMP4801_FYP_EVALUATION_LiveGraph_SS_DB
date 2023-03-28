/*
FYP : 22013

Module:
    Server Skeleton - LiveGraph Version

Description:
    Server Skeleton for the evaluation phase of the project. This module assists
    in extending the HTAP Graph Database Storage Systems to build purpose-built
    HTAP Graph Database System for evaluation by providing a uniform interface
    for interaction for the Hybrid Query Driver.

    This file contains the main function for the Server Skeleton.

    This version is edited to work with the LiveGraph graph storage system.

*/

#include "DSS000_server_skeleton.h"

/*
Description:
    Sends the reply to the query sent by Hybrid Query Driver
*/
void send_reply(int socket_fd, json response) {
  std::string reply_message = response.dump() + "|";
  int number_of_bytes_to_send = reply_message.length();
  while (number_of_bytes_to_send > 0){

    int start_pos = reply_message.length() - number_of_bytes_to_send;
    std::string remaining_substring = reply_message.substr(start_pos, number_of_bytes_to_send);

    int sent_bytes = send(socket_fd, remaining_substring.c_str(), std::strlen(remaining_substring.c_str()), 0);
    number_of_bytes_to_send = number_of_bytes_to_send - sent_bytes;
  }
  close(socket_fd);
}

/*
Description:
    Defines the job of the thread spawned to serve the query received from the
    Hybrid Query Driver.
*/
void* thread_job(void* data) {
  thread_data* thr_data = (thread_data*) data;
  char buffer[1] = {0};

  std::string json_req_string = "";

  // read the json message for the query
  while (true) {
    int value_from_socket = read(thr_data->socket_fd, buffer, 1);
    if (buffer[0] == '|') {
      break;
    }
    else {
      json_req_string = json_req_string + buffer[0];
    }
  }

  // determine the operation type and call the function for that operation
  int operation_id = -1;
  json request_query = json::parse(json_req_string);

  operation_id =  request_query["OPERATION_ID"].get<int>();

  json response;
  response["SUCCESS"] = false;

  switch (operation_id) {

    /**************************************************************************/
    /*BASE DATA LOADING: VERTEX INSERTION*/
    case 1:
      insert_investor_vertex(request_query, &response);
      break;

    case 2:
      insert_tradebook_vertex(request_query, &response);
      break;

    case 3:
      insert_company_vertex(request_query, &response);
      break;

    case 4:
      insert_freshness_score_vertex(request_query, &response);
      break;

    /**************************************************************************/
    /*BASE DATA LOADING: EDGE INSERTION*/
    case 5:
      insert_friend_edge(request_query, &response);
      break;

    case 6:
      insert_mirror_edge(request_query, &response);
      break;

    case 7:
      insert_hasbook_edge(request_query, &response);
      break;
    /**************************************************************************/
    /*TRANSACTIONAL QUERY PROCESSING*/
    case 8:
      perform_TQ_1_A(request_query, &response);
      break;

    case 9:
      perform_TQ_1_B(request_query, &response);
      break;

    case 10:
      perform_TQ_2(request_query, &response);
      break;

    case 11:
      perform_TQ_3(request_query, &response);
      break;

    case 12:
      perform_TQ_4(request_query, &response);
      break;

    case 13:
      perform_TQ_5(request_query, &response);
      break;

    case 14:
      perform_TQ_6(request_query, &response);
      break;

    case 15:
      perform_TQ_7(request_query, &response);
      break;

    case 16:
      perform_TQ_8(request_query, &response);
      break;
    /**************************************************************************/
    /*ANALYTICAL QUERY PROCESSING*/
    case 17:
      perform_AQ_1(request_query, &response);
      break;

    case 18:
      perform_AQ_2(request_query, &response);
      break;

    case 19:
      perform_AQ_3(request_query, &response);
      break;

    case 20:
      perform_AQ_4(request_query, &response);
      break;

    case 21:
      perform_AQ_5(request_query, &response);
      break;

    case 22:
      perform_AQ_6(request_query, &response);
      break;

    case 30:
      //STOP THE SERVER
      exit(0);
      break;
    /**************************************************************************/
    default:
      // UNDEFINED BEHAVIOUR
      std::cout<<"ERROR:UNRECOGNIZED QUERY TYPE"<<"\n";
  }

  // send the reply returned after the query was processed
  send_reply(thr_data->socket_fd, response);

  delete thr_data;

  pthread_exit(NULL);
}

/*
Description:
    Accepts a connection request from the Hybrid Query Driver and spawns a thread
    to serve the query sent by the Hybrid Query Driver.
*/
int main (int argc, char** argv) {

  // set up socket to listen to connection requests
  int main_server_fd;
  struct sockaddr_in address;
  int opt = 1;
  int addrlen = sizeof(address);

  if ((main_server_fd = socket(AF_INET, SOCK_STREAM, 0)) == 0) {
    perror("Socket Allocation Error");
    exit(EXIT_FAILURE);
  }

  if (setsockopt(main_server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &opt, sizeof(opt))) {
    perror("Socket Option Error");
    exit(EXIT_FAILURE);
  }
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(DB_SERVER_PORT_NUMBER);

  if (bind(main_server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
    perror("Socket bind failed");
    exit(EXIT_FAILURE);
  }

  if (listen(main_server_fd, 32767) < 0) {
    perror("Socket listen error");
    exit(EXIT_FAILURE);
  }

  /*
  keep accepting connection requests and spawn a thread to serve each connection
  */
  while (true)
  {
    int new_socket = -1;
    if ((new_socket = accept(main_server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen)) < 0) {
      perror("Socket accept error");
      exit(EXIT_FAILURE);
    }

    thread_data* new_request_handler_thread_data = new thread_data();
    new_request_handler_thread_data->socket_fd = new_socket;

    pthread_t request_handler_thread;
    pthread_create(&request_handler_thread, NULL, (*thread_job), (void*) new_request_handler_thread_data);

    if (pthread_detach(request_handler_thread) != 0) {
      perror("Thread Detach Error");
      exit(EXIT_FAILURE);
    }
  }
}
