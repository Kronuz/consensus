# Adds consensus_netlab_node to a standalone build without touching CMakeLists.txt:
#   cmake -S . -B build -DCMAKE_PROJECT_consensus_INCLUDE=$PWD/bench/netlab/node.cmake
# It runs at the end of project(consensus) and defers the target until consensus::transport exists.
# On Linux it links libstdc++ statically, so a binary built in one guest runs in a fresh one.
cmake_minimum_required(VERSION 3.19)
function(consensus_netlab_node_add)
	add_executable(consensus_netlab_node "${CMAKE_SOURCE_DIR}/bench/netlab/node.cc")
	target_link_libraries(consensus_netlab_node PRIVATE consensus::transport)
	if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
		target_link_options(consensus_netlab_node PRIVATE -static-libstdc++ -static-libgcc)
	endif()
endfunction()
cmake_language(DEFER DIRECTORY "${CMAKE_SOURCE_DIR}" CALL consensus_netlab_node_add)
