# COMP4801_FYP_EVALUATION_LiveGraph_SS_DB

## FYP: 22013

### FYP Team

**Student:** SRIVASTAVA Dhruv (3035667792)

**Supervisor:** Dr. Heming Cui

## Description

Database Server Skeleton for the evaluation phase of the project edited to become
an in-memory purpose-built database system for the benchmark with the ```LiveGraph```
HTAP Graph Database Storage System.

## Usage Instructions

### Building the executable

- **Step 1:** Download the ```json.hpp``` file from ```https://github.com/nlohmann/json/releases/tag/v3.11.2``` and put it in the ```third_party``` folder.

- **Step 2:** Download the ```gapbs.hpp``` file from ```https://github.com/PerFuchs/gfe_driver/blob/master/third-party/gapbs/gapbs.hpp``` and put it in the ```third_party``` folder.

- **Step 3:** Download the ```livegraph.hpp``` file from ```https://github.com/thu-pacman/LiveGraph/blob/master/bind/livegraph.hpp``` and put it in the ```third_party``` folder.

- **Step 4:** Download the ``` liblivegraph.so``` file from ```https://github.com/thu-pacman/LiveGraph-Binary/releases/tag/20200829``` and put it in the ```third_party``` folder.

- **Step 5:** Update the ```Makefile``` with the *actual path of the directory*
containing the ``` liblivegraph.so``` file from Step 4 in the place holders
provided in the compilation flags in the Makefile. The first *make* command has
been left as an example of how this is supposed to be done.

- **Step 6:** Install ```libtbb-dev```.

- **Step 7:** Build the executable by using ```make main_server```.

The following libraries/compilers were used during development:

- nlohmann json (v3.11.2)
- g++ (version 9.3.0)

## Module Description

| Module Name | Description | Directory |
|-------------|-------------|-----------|
|DSS (Database Server Skeleton)| Brings all Server Skeleton components together to build a purpose-built server | ./ |
|SCD (Skeleton Common Definition)| Defines common data types and methods to be used across the server | common/ |
|SEI (Skeleton Edge Insertor)| Defines the functions that will insert the edges in the base data of the benchmark | edge_insertion_methods/ |
|SVI (Skeleton Vertex Insertor)| Defines the functions that will insert the vertices in the base data of the benchmark | vertex_insertion_methods/ |
|TQH (Transactional Query Handler)| Defines the functions that will serve the transactional queries of the benchmark | transactional_query_methods/ |
|AQH (Analytical Query Handler)| Defines the functions that will serve the analytical queries of the benchmark | analytical_query_methods/ |

## Important Notes

The implementation of all the analytical queries for this purpose-built database to
serve the queries from the benchmark during evaluation was taken from the
evaluation experiments conducted for the ```Sortledton``` paper. The original
code can be found at: ```https://github.com/PerFuchs/gfe_driver```.
