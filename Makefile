# FYP : 22013
# Description:
# 							Makefile for the Server Skeleton - LiveGraph Version

FLAGS = --std=c++11

SCD002_common_methods.o: common/SCD002_common_methods.cpp common/SCD002_common_methods.h common/SCD001_common_types.h
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

AQH001_bfs_handler.o: analytical_query_methods/analytical_query_handlers/AQ_1/AQH001_bfs_handler.cpp analytical_query_methods/analytical_query_handlers/AQ_1/AQH001_bfs_handler.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

AQH002_pr_handler.o: analytical_query_methods/analytical_query_handlers/AQ_2/AQH002_pr_handler.cpp analytical_query_methods/analytical_query_handlers/AQ_2/AQH002_pr_handler.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

AQH003_wcc_handler.o: analytical_query_methods/analytical_query_handlers/AQ_3/AQH003_wcc_handler.cpp analytical_query_methods/analytical_query_handlers/AQ_3/AQH003_wcc_handler.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

AQH004_lcc_handler.o: analytical_query_methods/analytical_query_handlers/AQ_4/AQH004_lcc_handler.cpp analytical_query_methods/analytical_query_handlers/AQ_4/AQH004_lcc_handler.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

AQH005_cdlp_handler.o: analytical_query_methods/analytical_query_handlers/AQ_5/AQH005_cdlp_handler.cpp analytical_query_methods/analytical_query_handlers/AQ_5/AQH005_cdlp_handler.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

AQH006_sssp_handler.o: analytical_query_methods/analytical_query_handlers/AQ_6/AQH006_sssp_handler.cpp analytical_query_methods/analytical_query_handlers/AQ_6/AQH006_sssp_handler.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

AQH000_analysis_handler.o: analytical_query_methods/AQH000_analysis_handler.cpp analytical_query_methods/AQH000_analysis_handler.h common/SCD001_common_types.h SCD002_common_methods.o AQH001_bfs_handler.o AQH001_bfs_handler.o AQH003_wcc_handler.o AQH004_lcc_handler.o AQH005_cdlp_handler.o AQH006_sssp_handler.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

SEI000_edge_insertors.o: edge_insertion_methods/SEI000_edge_insertors.cpp edge_insertion_methods/SEI000_edge_insertors.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

TQH000_transaction_handler.o: transactional_query_methods/TQH000_transaction_handler.cpp transactional_query_methods/TQH000_transaction_handler.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

SVI000_vertex_insertors.o: vertex_insertion_methods/SVI000_vertex_insertors.cpp vertex_insertion_methods/SVI000_vertex_insertors.h common/SCD001_common_types.h SCD002_common_methods.o
	g++ $(FLAGS) -c $< -ltbb -lpthread -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

DSS000_server_skeleton.o: DSS000_server_skeleton.cpp DSS000_server_skeleton.h SVI000_vertex_insertors.o TQH000_transaction_handler.o SEI000_edge_insertors.o AQH000_analysis_handler.o SCD002_common_methods.o common/SCD001_common_types.h AQH001_bfs_handler.o AQH002_pr_handler.o AQH003_wcc_handler.o AQH004_lcc_handler.o AQH005_cdlp_handler.o AQH006_sssp_handler.o
	g++ $(FLAGS) -c $< -lpthread -ltbb -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

main_server: DSS000_server_skeleton.o SVI000_vertex_insertors.o TQH000_transaction_handler.o SEI000_edge_insertors.o AQH000_analysis_handler.o SCD002_common_methods.o AQH001_bfs_handler.o AQH002_pr_handler.o AQH003_wcc_handler.o AQH004_lcc_handler.o AQH005_cdlp_handler.o AQH006_sssp_handler.o
	g++ $(FLAGS) $^ -o $@ -lpthread -ltbb -llivegraph -L/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -Wl,-rpath=/home/dhruv/LiveGraph/LiveGraph-SS/third_party/ -std=c++17

clean_o:
	rm -f *.o

clean:
	rm main_server && make clean_o

.PHONY: clean_o, clean
