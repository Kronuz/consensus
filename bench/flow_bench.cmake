# Adds consensus_flow_bench to a standalone build without touching CMakeLists.txt:
#   cmake -S . -B build -DCMAKE_PROJECT_consensus_INCLUDE=$PWD/bench/flow_bench.cmake
# It runs at the end of project(consensus) and defers the target until consensus::transport exists.
cmake_minimum_required(VERSION 3.19)
function(consensus_flow_bench_add)
	add_executable(consensus_flow_bench "${CMAKE_SOURCE_DIR}/bench/flow_bench.cc")
	target_link_libraries(consensus_flow_bench PRIVATE consensus::transport)
endfunction()
cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL consensus_flow_bench_add)
