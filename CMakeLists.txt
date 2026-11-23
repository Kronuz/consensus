cmake_minimum_required(VERSION 3.16)
project(consensus LANGUAGES CXX)

# Strict durable Raft: a deterministic Core, a Worker over journal storage, checkpoints,
# and snapshot transfer, plus an optional authenticated peer transport. Header-only.
include(FetchContent)
# Standalone, the transport, the completion test and the bench need Asio: task and completion
# at journal's pins, with their Asio targets, made available before journal turns them off.
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	FetchContent_Declare(task GIT_REPOSITORY https://github.com/Kronuz/task.git GIT_TAG 95798a02f68c86f070401329a05c78d326ac2579)
	FetchContent_Declare(completion GIT_REPOSITORY https://github.com/Kronuz/completion.git GIT_TAG d8b538c6778f0a1b969e42c8428995d1d29d4731)
	set(TASK_ASIO ON)
	set(COMPLETION_TASK ON)
	set(COMPLETION_ASIO ON)
	FetchContent_MakeAvailable(task completion)
endif()
FetchContent_Declare(journal GIT_REPOSITORY https://github.com/Kronuz/journal.git GIT_TAG 9e11713edbb6e31681e1ae7a3f8f18f15eb7eaac)
FetchContent_MakeAvailable(journal)

add_library(consensus INTERFACE)
target_include_directories(consensus INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(consensus INTERFACE cxx_std_20)
target_link_libraries(consensus INTERFACE journal::journal)
add_library(consensus::consensus ALIAS consensus)

# The authenticated peer transport (TLS over TCP, a control and a bulk lane per peer):
# optional, since it brings Asio, reactor, and OpenSSL 3.
option(CONSENSUS_TRANSPORT "Build the peer transport target consensus::transport" OFF)
if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	set(CONSENSUS_TRANSPORT ON)
endif()
if(CONSENSUS_TRANSPORT)
	FetchContent_Declare(reactor GIT_REPOSITORY https://github.com/Kronuz/reactor.git GIT_TAG ab69cf26e556f7108fb800d89c324648779e2549)
	FetchContent_MakeAvailable(reactor)
	if(NOT TARGET reactor::tls)
		message(FATAL_ERROR "consensus::transport needs OpenSSL 3 (reactor::tls)")
	endif()
	add_library(consensus_transport INTERFACE)
	target_link_libraries(consensus_transport INTERFACE consensus::consensus reactor::tls)
	if(TARGET task::asio)
		target_link_libraries(consensus_transport INTERFACE task::asio)  # the host connection (connection.h)
	endif()
	add_library(consensus::transport ALIAS consensus_transport)
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
	enable_testing()
	file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/.scratch")  # the tests' state
	# faultlab's faultfs, the crash model the journal tests run on; test-only. It reuses the
	# completion target declared above.
	FetchContent_Declare(faultlab GIT_REPOSITORY https://github.com/Kronuz/faultlab.git GIT_TAG a3b71d1b9bdcfa3664dbce8725f14c07bb14845f)
	FetchContent_MakeAvailable(faultlab)
	foreach(t consensus checkpoint_encoder retained_log retirement simulation membership pipeline retained_state)
		add_executable(consensus_${t}_test test/${t}_test.cc)
		target_link_libraries(consensus_${t}_test PRIVATE consensus::consensus)
		add_test(NAME consensus_${t} COMMAND consensus_${t}_test)
		set_tests_properties(consensus_${t} PROPERTIES TIMEOUT 30)
	endforeach()
	# Sanitized and beside the shared-journal tests on two CPUs, the simulation needs more.
	set_tests_properties(consensus_simulation PROPERTIES TIMEOUT 300)
	target_sources(consensus_simulation_test PRIVATE test/driver_simulation.cc)  # the same oracles through the Driver
	# A full rpc_bytes window over slow in-order links (docs/mechanisms/append-pacing.md).
	add_executable(consensus_full_window_stall_test test/full_window_stall_test.cc)
	target_link_libraries(consensus_full_window_stall_test PRIVATE consensus::consensus)
	add_test(NAME consensus_full_window_stall COMMAND consensus_full_window_stall_test)
	# T1: stores written at b7073bb recover unchanged.
	add_executable(consensus_compat_test test/compat_test.cc)
	target_link_libraries(consensus_compat_test PRIVATE consensus::consensus)
	add_test(NAME consensus_compat COMMAND consensus_compat_test ${CMAKE_CURRENT_SOURCE_DIR}/test/fixtures/b7073bb)
	# T3 in simulated time, under the default and per-platform profiles.
	add_executable(consensus_liveness_test test/liveness_test.cc)
	target_link_libraries(consensus_liveness_test PRIVATE consensus::consensus)
	add_test(NAME consensus_liveness COMMAND consensus_liveness_test 20)
	set_tests_properties(consensus_liveness PROPERTIES TIMEOUT 180)
	add_executable(consensus_shared_worker_test test/shared_worker_test.cc)
	target_link_libraries(consensus_shared_worker_test PRIVATE consensus::consensus)
	add_test(NAME consensus_shared_worker COMMAND consensus_shared_worker_test)
	set_tests_properties(consensus_shared_worker PROPERTIES TIMEOUT 600)
	add_executable(consensus_shared_cluster_test test/shared_cluster_test.cc)
	target_link_libraries(consensus_shared_cluster_test PRIVATE consensus::consensus)
	add_test(NAME consensus_shared_cluster COMMAND consensus_shared_cluster_test)
	set_tests_properties(consensus_shared_cluster PROPERTIES TIMEOUT 600)
	# 64 lone voters opened in one wave on one loop, as a host opens its groups.
	add_executable(consensus_wave_open_test test/wave_open_test.cc)
	target_link_libraries(consensus_wave_open_test PRIVATE consensus::consensus)
	add_test(NAME consensus_wave_open COMMAND consensus_wave_open_test 64)
	# More than a segment's turn at once, and a power cut that fences every store outside its worker.
	add_test(NAME consensus_wave_open_256 COMMAND consensus_wave_open_test 256)
	add_test(NAME consensus_wave_open_cut COMMAND consensus_wave_open_test 128 128 cut)
	set_tests_properties(consensus_wave_open consensus_wave_open_256 consensus_wave_open_cut PROPERTIES TIMEOUT 120)
	add_executable(consensus_waiter_test test/waiter_test.cc)
	target_link_libraries(consensus_waiter_test PRIVATE consensus::consensus)
	# One entry per case, so each stays well inside its timeout under ASan on a CI runner.
	foreach(c rows-propose rows-committed rows-read rows-propose-pair close close-read close-open close-late teardown teardown-close threads)
		add_test(NAME consensus_waiter_${c} COMMAND consensus_waiter_test ${c})
		set_tests_properties(consensus_waiter_${c} PROPERTIES TIMEOUT 600)
	endforeach()
	# A seeded fault (test/driver_mutants.py), for the waiter, driver, driver replacement and membership tests' mutant jobs.
	set(CONSENSUS_DRIVER_MUTANT "" CACHE STRING "a test/driver_mutants.py fault built into consensus_waiter_test, consensus_driver_test, consensus_driver_replace_test and consensus_membership_test")
	if(CONSENSUS_DRIVER_MUTANT)
		execute_process(COMMAND python3 "${CMAKE_CURRENT_SOURCE_DIR}/test/driver_mutants.py" "${CONSENSUS_DRIVER_MUTANT}" "${CMAKE_CURRENT_BINARY_DIR}/mutant" RESULT_VARIABLE mutant_result)
		if(NOT mutant_result EQUAL 0)
			message(FATAL_ERROR "driver mutant ${CONSENSUS_DRIVER_MUTANT} not applied")
		endif()
		target_include_directories(consensus_waiter_test BEFORE PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/mutant/include")
		target_include_directories(consensus_membership_test BEFORE PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/mutant/include")
	endif()
	add_executable(consensus_blocking_entry_test test/blocking_entry_test.cc)
	target_link_libraries(consensus_blocking_entry_test PRIVATE consensus::consensus)
	add_test(NAME consensus_blocking_entry COMMAND consensus_blocking_entry_test)
	set_tests_properties(consensus_blocking_entry PROPERTIES TIMEOUT 120)
	add_executable(consensus_driver_test test/driver_test.cc)
	target_link_libraries(consensus_driver_test PRIVATE consensus::consensus)
	if(CONSENSUS_DRIVER_MUTANT)
		target_include_directories(consensus_driver_test BEFORE PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/mutant/include")
	endif()
	add_test(NAME consensus_driver COMMAND consensus_driver_test)
	set_tests_properties(consensus_driver PROPERTIES TIMEOUT 120)
	# A Driver group's install (docs/DRIVER.md, Install), a ctest entry per case.
	add_executable(consensus_driver_install_test test/driver_install_test.cc)
	target_link_libraries(consensus_driver_install_test PRIVATE consensus::consensus)
	foreach(c lagging rejected stale restart orphan)
		add_test(NAME consensus_driver_install_${c} COMMAND consensus_driver_install_test ${c})
		set_tests_properties(consensus_driver_install_${c} PROPERTIES TIMEOUT 120)
	endforeach()
	# D3 through the Driver: a replacement, add, promote and the core's removal, over seeds.
	add_executable(consensus_driver_replace_test test/driver_replace_test.cc)
	target_link_libraries(consensus_driver_replace_test PRIVATE consensus::consensus)
	if(CONSENSUS_DRIVER_MUTANT)
		target_include_directories(consensus_driver_replace_test BEFORE PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/mutant/include")
	endif()
	# 1000 seeds in four parts, so each fits a sanitizer build's timeout and they run in parallel.
	foreach(first 1 251 501 751)
		add_test(NAME consensus_driver_replace_${first} COMMAND consensus_driver_replace_test 250 ${first})
		set_tests_properties(consensus_driver_replace_${first} PROPERTIES TIMEOUT 300)
	endforeach()
	add_executable(consensus_driver_close_test test/driver_close_test.cc)
	target_link_libraries(consensus_driver_close_test PRIVATE consensus::consensus)
	add_test(NAME consensus_driver_close COMMAND consensus_driver_close_test)
	add_test(NAME consensus_driver_close_bytes COMMAND consensus_driver_close_test closed_bytes)
	add_test(NAME consensus_driver_close_crash COMMAND consensus_driver_close_test close_crash)
	set_tests_properties(consensus_driver_close PROPERTIES TIMEOUT 120)
	add_executable(consensus_worker_test test/worker_test.cc)
	target_link_libraries(consensus_worker_test PRIVATE consensus::consensus)
	add_test(NAME consensus_worker COMMAND consensus_worker_test)
	# Each seeded fault in tools/mutants.sh must fail the simulation (it reads Ninja's commands and
	# needs bash). One test per mutant, so ctest -j runs them in parallel; the TIMEOUT is four
	# times the slowest of the macOS, Linux and FreeBSD gate hosts' time (floor 600 s), for sanitizers.
	find_program(CONSENSUS_BASH bash)
	if(CMAKE_GENERATOR MATCHES "Ninja" AND CONSENSUS_BASH)
		set(consensus_mutants
			recovered-delivery-byte-accounting-underflows=600
			sends-before-durability=600
			votes-and-pre-votes-without-the-log-check=700
			repair-with-a-delivery-outstanding=600
			membership-change-before-the-leader-commits-in-its-term=652
			no-membership-rollback-on-a-replaced-suffix=600
			a-removed-leader-counts-itself=600
			a-follower-checks-entries-by-its-node-limits-not-by-position=600
			a-leader-grants-pre-votes=640
			learners-refuse-votes=600
			repair-ignores-the-term-at-the-base-boundary=1712
			repair-above-the-commit-index=600
			repair-below-base-index=600
			a-leader-ignores-members-node-limits=1016
			commit-of-an-earlier-term-by-counting=844
			a-follower-ignores-leadergone=896
			a-read-counts-every-acknowledgement=900)
		set(consensus_mutant_names "")
		foreach(entry IN LISTS consensus_mutants)
			string(REPLACE "=" ";" entry "${entry}")
			list(GET entry 0 mutant)
			list(GET entry 1 seconds)
			list(APPEND consensus_mutant_names ${mutant})
			add_test(NAME consensus_mutant_${mutant} COMMAND ${CONSENSUS_BASH} ${CMAKE_CURRENT_SOURCE_DIR}/tools/mutants.sh ${CMAKE_BINARY_DIR} 400 10000 ${mutant})
			set_tests_properties(consensus_mutant_${mutant} PROPERTIES TIMEOUT ${seconds})
		endforeach()
		# A mutant added to the script without a test here would go unrun.
		execute_process(COMMAND ${CONSENSUS_BASH} ${CMAKE_CURRENT_SOURCE_DIR}/tools/mutants.sh --list OUTPUT_VARIABLE listed OUTPUT_STRIP_TRAILING_WHITESPACE)
		string(REPLACE "\n" ";" listed "${listed}")
		if(NOT "${listed}" STREQUAL "${consensus_mutant_names}")
			message(FATAL_ERROR "tools/mutants.sh --list differs from the consensus_mutant_ tests: ${listed}")
		endif()
	endif()
	# Its real-file cases are flush-bound on macOS (F_FULLFSYNC): 145-170 s alone, more on a busy disk.
	set_tests_properties(consensus_worker PROPERTIES TIMEOUT 600)
	# T8: the decoders' checked-in corpus replays as a test; with CONSENSUS_FUZZ (Clang) the
	# libFuzzer binary builds too.
	add_executable(consensus_fuzz_decoders_replay fuzz/fuzz_decoders.cc fuzz/replay.cc)
	target_link_libraries(consensus_fuzz_decoders_replay PRIVATE consensus::consensus)
	add_test(NAME consensus_fuzz_decoders_replay COMMAND consensus_fuzz_decoders_replay "${CMAKE_CURRENT_SOURCE_DIR}/fuzz/corpus/decoders")
	add_executable(consensus_fuzz_seed fuzz/seed.cc)
	target_link_libraries(consensus_fuzz_seed PRIVATE consensus::consensus)
	# The RPC lane's codec and call table (fuzz/rpc_fuzz.cc): its corpus replays as a test.
	add_executable(consensus_fuzz_rpc_replay fuzz/rpc_fuzz.cc fuzz/replay.cc)
	target_link_libraries(consensus_fuzz_rpc_replay PRIVATE consensus::consensus)
	add_test(NAME consensus_fuzz_rpc_replay COMMAND consensus_fuzz_rpc_replay "${CMAKE_CURRENT_SOURCE_DIR}/fuzz/corpus/rpc")
	# The multicast seed datagram (fuzz/seeds_fuzz.cc): its corpus replays as a test.
	add_executable(consensus_fuzz_seeds_replay fuzz/seeds_fuzz.cc fuzz/replay.cc)
	target_link_libraries(consensus_fuzz_seeds_replay PRIVATE consensus::consensus)
	add_test(NAME consensus_fuzz_seeds_replay COMMAND consensus_fuzz_seeds_replay "${CMAKE_CURRENT_SOURCE_DIR}/fuzz/seeds_corpus")
	option(CONSENSUS_FUZZ "Build the libFuzzer targets (Clang)" OFF)
	if(CONSENSUS_FUZZ)
		add_executable(consensus_fuzz_decoders fuzz/fuzz_decoders.cc)
		target_link_libraries(consensus_fuzz_decoders PRIVATE consensus::consensus)
		target_compile_options(consensus_fuzz_decoders PRIVATE -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all)
		target_link_options(consensus_fuzz_decoders PRIVATE -fsanitize=fuzzer,address,undefined)
		add_executable(consensus_fuzz_rpc fuzz/rpc_fuzz.cc)
		target_link_libraries(consensus_fuzz_rpc PRIVATE consensus::consensus)
		target_compile_options(consensus_fuzz_rpc PRIVATE -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all)
		target_link_options(consensus_fuzz_rpc PRIVATE -fsanitize=fuzzer,address,undefined)
		add_executable(consensus_fuzz_seeds fuzz/seeds_fuzz.cc)
		target_link_libraries(consensus_fuzz_seeds PRIVATE consensus::consensus)
		target_compile_options(consensus_fuzz_seeds PRIVATE -fsanitize=fuzzer,address,undefined -fno-sanitize-recover=all)
		target_link_options(consensus_fuzz_seeds PRIVATE -fsanitize=fuzzer,address,undefined)
	endif()
	add_test(NAME consensus_worker_terminal COMMAND consensus_worker_test --terminal-regressions)
	set_tests_properties(consensus_worker_terminal PROPERTIES TIMEOUT 30)
	foreach(b checkpoint_bench worker_checkpoint_bench)
		add_executable(consensus_${b} bench/${b}.cc)
		target_link_libraries(consensus_${b} PRIVATE consensus::consensus)
	endforeach()
	# Y6, Y9: an idle group's threads, descriptors, frames, timers and bytes on a Driver.
	add_executable(consensus_driver_idle bench/driver_idle.cc)
	target_link_libraries(consensus_driver_idle PRIVATE consensus::consensus)
	add_test(NAME consensus_driver_idle COMMAND consensus_driver_idle 100 WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
	# Each create syncs real files, as the Worker suite does: the two never share the disk.
	set_tests_properties(consensus_driver_idle PROPERTIES TIMEOUT 300 RESOURCE_LOCK file_sync)
	set_tests_properties(consensus_worker PROPERTIES RESOURCE_LOCK file_sync)
	add_executable(consensus_demo examples/demo.cc)
	target_link_libraries(consensus_demo PRIVATE consensus::consensus)

	if(APPLE OR CMAKE_SYSTEM_NAME STREQUAL "FreeBSD" OR CMAKE_SYSTEM_NAME STREQUAL "Linux")
		add_executable(consensus_completion_asio_test test/completion_asio_test.cc)
		target_link_libraries(consensus_completion_asio_test PRIVATE consensus::consensus completion::asio reactor::reactor)
		add_test(NAME consensus_completion_asio COMMAND consensus_completion_asio_test)
		set_tests_properties(consensus_completion_asio PROPERTIES TIMEOUT 30)
		add_executable(consensus_worker_completion_bench EXCLUDE_FROM_ALL bench/worker_completion_bench.cc)
		target_link_libraries(consensus_worker_completion_bench PRIVATE consensus::consensus completion::asio reactor::reactor)
	endif()

	# The tests that build a journal run it on faultfs.
	foreach(t simulation shared_worker shared_cluster wave_open waiter blocking_entry driver driver_close driver_replace worker)
		target_link_libraries(consensus_${t}_test PRIVATE faultlab::faultlab)
	endforeach()

	# Transport tests; they check with assert, so NDEBUG stays off.
	# The fixture's certificates come from the openssl beside the linked library: macOS's
	# /usr/bin/openssl is LibreSSL, whose certificates OpenSSL 3 rejects.
	get_filename_component(CONSENSUS_OPENSSL_PREFIX "${OPENSSL_INCLUDE_DIR}" DIRECTORY)
	find_program(CONSENSUS_TEST_OPENSSL openssl HINTS "${CONSENSUS_OPENSSL_PREFIX}/bin" NO_DEFAULT_PATH)
	find_program(CONSENSUS_TEST_OPENSSL openssl REQUIRED)
	add_test(NAME consensus_tls_certificates COMMAND ${CMAKE_COMMAND} -DOUTPUT=${CMAKE_CURRENT_BINARY_DIR}/tls-fixture
		-DOPENSSL=${CONSENSUS_TEST_OPENSSL} -P ${CMAKE_CURRENT_SOURCE_DIR}/test/transport/tls_certificates.cmake)
	set_tests_properties(consensus_tls_certificates PROPERTIES FIXTURES_SETUP consensus_tls TIMEOUT 30)
	foreach(t codec rpc_observation ownership tls authentication peers deadline node_hello connection links admission resolve)
		add_executable(consensus_transport_${t}_test test/transport/${t}_test.cc)
		target_link_libraries(consensus_transport_${t}_test PRIVATE consensus::transport)
		target_compile_options(consensus_transport_${t}_test PRIVATE -UNDEBUG)
	endforeach()
	foreach(t codec rpc_observation ownership peers deadline node_hello admission resolve)
		add_test(NAME consensus_transport_${t} COMMAND consensus_transport_${t}_test)
		set_tests_properties(consensus_transport_${t} PROPERTIES TIMEOUT 30)
	endforeach()
	target_link_libraries(consensus_transport_peers_test PRIVATE faultlab::faultlab)  # its Nodes' journal runs on faultfs
	set_tests_properties(consensus_transport_peers PROPERTIES TIMEOUT 50)  # under TSan a run takes up to 25 s
	# A scenario that throws with its voters running reports the failure instead of hanging.
	add_test(NAME consensus_transport_peers_throwing COMMAND consensus_transport_peers_test throwing)
	set_tests_properties(consensus_transport_peers_throwing PROPERTIES TIMEOUT 20
		PASS_REGULAR_EXPRESSION "throwing: threw: thrown with the voters running")
	# A contender on every port the system could hand out; the voters' own ports stay free.
	add_test(NAME consensus_transport_peers_taken_ports COMMAND consensus_transport_peers_test taken-ports)
	set_tests_properties(consensus_transport_peers_taken_ports PROPERTIES TIMEOUT 20)
	# PeerOptions::wake (docs/mechanisms/peers-wake.md), one ctest entry per case.
	add_executable(consensus_transport_peers_wake_test test/peers_wake_test.cc)
	target_link_libraries(consensus_transport_peers_wake_test PRIVATE consensus::transport faultlab::faultlab)
	target_compile_options(consensus_transport_peers_wake_test PRIVATE -UNDEBUG)
	foreach(c idle control bulk)
		add_test(NAME consensus_transport_peers_wake_${c} COMMAND consensus_transport_peers_wake_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture ${c})
		set_tests_properties(consensus_transport_peers_wake_${c} PROPERTIES FIXTURES_REQUIRED consensus_tls TIMEOUT 60)
	endforeach()
	add_test(NAME consensus_transport_tls COMMAND consensus_transport_tls_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture)
	add_test(NAME consensus_transport_pipe COMMAND consensus_transport_tls_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture --pipes)
	add_test(NAME consensus_transport_authentication COMMAND consensus_transport_authentication_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture)
	add_test(NAME consensus_transport_connection COMMAND consensus_transport_connection_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture)
	add_test(NAME consensus_transport_links COMMAND consensus_transport_links_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture)
	set_tests_properties(consensus_transport_tls consensus_transport_pipe consensus_transport_authentication consensus_transport_connection consensus_transport_links
		PROPERTIES FIXTURES_REQUIRED consensus_tls TIMEOUT 30)
	set_tests_properties(consensus_transport_connection PROPERTIES TIMEOUT 120)  # the slow readers run for seconds each
	# Nodes admitted and removed at runtime (docs/mechanisms/admission.md): its own certificates,
	# one ctest entry per case and trust policy.
	add_test(NAME consensus_admission_certificates COMMAND ${CMAKE_COMMAND} -DOUTPUT=${CMAKE_CURRENT_BINARY_DIR}/admission-fixture
		-DOPENSSL=${CONSENSUS_TEST_OPENSSL} -P ${CMAKE_CURRENT_SOURCE_DIR}/test/transport/admission_certificates.cmake)
	set_tests_properties(consensus_admission_certificates PROPERTIES FIXTURES_SETUP consensus_admission TIMEOUT 60)
	add_executable(consensus_transport_admission_nodes_test test/transport/admission_nodes_test.cc)
	target_link_libraries(consensus_transport_admission_nodes_test PRIVATE consensus::transport)
	target_compile_options(consensus_transport_admission_nodes_test PRIVATE -UNDEBUG)
	foreach(c policy issue save)
		add_test(NAME consensus_transport_admission_nodes_${c}
			COMMAND consensus_transport_admission_nodes_test ${CMAKE_CURRENT_BINARY_DIR}/admission-fixture ${c})
		set_tests_properties(consensus_transport_admission_nodes_${c} PROPERTIES FIXTURES_REQUIRED consensus_admission TIMEOUT 60)
	endforeach()
	foreach(c join refused remove epoch)
		foreach(p ca pins)
			add_test(NAME consensus_transport_admission_nodes_${c}_${p}
				COMMAND consensus_transport_admission_nodes_test ${CMAKE_CURRENT_BINARY_DIR}/admission-fixture ${c} ${p})
			set_tests_properties(consensus_transport_admission_nodes_${c}_${p} PROPERTIES FIXTURES_REQUIRED consensus_admission TIMEOUT 150)
		endforeach()
	endforeach()
	# The host transport (host_transport.h): one ctest entry per case.
	add_executable(consensus_transport_groups_test test/transport/groups_test.cc)
	target_link_libraries(consensus_transport_groups_test PRIVATE consensus::transport)
	target_compile_options(consensus_transport_groups_test PRIVATE -UNDEBUG)
	foreach(c threads credit reader upload driver refill identity joiner lagging leftover reconnect backlog owe-order receive-bound relayout scan order fetch-alongside-consensus install)
		add_test(NAME consensus_transport_groups_${c} COMMAND consensus_transport_groups_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture ${c})
		set_tests_properties(consensus_transport_groups_${c} PROPERTIES FIXTURES_REQUIRED consensus_tls TIMEOUT 120)
	endforeach()
	set_tests_properties(consensus_transport_groups_joiner consensus_transport_groups_lagging consensus_transport_groups_relayout
		consensus_transport_groups_install PROPERTIES TIMEOUT 240)
	# The RPC lane (rpc.h): codec, call table, pools, the model's mutants, two lanes over pipes.
	add_executable(consensus_transport_rpc_test test/transport/rpc_test.cc)
	target_link_libraries(consensus_transport_rpc_test PRIVATE consensus::transport)
	target_compile_options(consensus_transport_rpc_test PRIVATE -UNDEBUG)
	add_test(NAME consensus_transport_rpc COMMAND consensus_transport_rpc_test)
	# The RPC lane on the host transport's Bulk connection, one ctest entry per case.
	add_executable(consensus_transport_rpc_host_test test/transport/rpc_host_test.cc)
	target_link_libraries(consensus_transport_rpc_host_test PRIVATE consensus::transport)
	target_compile_options(consensus_transport_rpc_host_test PRIVATE -UNDEBUG)
	foreach(c calls group-full call-full lane-only stream local-throw stalled-peer hold idle-race shutdown fetch reader stop-drain)
		add_test(NAME consensus_transport_rpc_host_${c} COMMAND consensus_transport_rpc_host_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture ${c})
		set_tests_properties(consensus_transport_rpc_host_${c} PROPERTIES FIXTURES_REQUIRED consensus_tls TIMEOUT 150)
	endforeach()
	# Latency bounds a runner that stops the process can break (D100): `ctest -LE timing` skips them; CI runs them.
	set_tests_properties(consensus_transport_rpc_host_group-full consensus_transport_rpc_host_call-full
		consensus_transport_rpc_host_stream consensus_transport_rpc_host_reader PROPERTIES LABELS timing)
	# The node hello's limits and wire versions (docs/mechanisms/hello.md).
	add_executable(consensus_transport_hello_test test/transport/hello_test.cc)
	target_link_libraries(consensus_transport_hello_test PRIVATE consensus::transport)
	target_compile_options(consensus_transport_hello_test PRIVATE -UNDEBUG)
	foreach(c codec leader version)
		add_test(NAME consensus_transport_hello_${c} COMMAND consensus_transport_hello_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture ${c})
		set_tests_properties(consensus_transport_hello_${c} PROPERTIES FIXTURES_REQUIRED consensus_tls TIMEOUT 120)
	endforeach()
	set_tests_properties(consensus_transport_rpc PROPERTIES TIMEOUT 120)
	# Multicast seeds (docs/mechanisms/seeds.md), one ctest entry per case. A host whose loopback
	# cannot carry multicast skips the cases that need it (exit 77) and says why; where CI or
	# CONSENSUS_SEEDS_REQUIRE is set, that fails instead.
	add_executable(consensus_transport_seeds_test test/transport/seeds_test.cc)
	target_link_libraries(consensus_transport_seeds_test PRIVATE consensus::transport)
	target_compile_options(consensus_transport_seeds_test PRIVATE -UNDEBUG)
	foreach(c codec drops processes unavailable forged forged_rotating rate recovers receive_errors throws require)
		add_test(NAME consensus_transport_seeds_${c} COMMAND consensus_transport_seeds_test ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture ${c})
		set_tests_properties(consensus_transport_seeds_${c} PROPERTIES FIXTURES_REQUIRED consensus_tls TIMEOUT 60 SKIP_RETURN_CODE 77)
	endforeach()
	# The host transport's idle cost per group, connection end and host, and its hop's CPU
	# (docs/perf/transport-idle-cost.md); ctest checks the counts on every platform.
	add_executable(consensus_transport_idle bench/transport_idle.cc)
	target_link_libraries(consensus_transport_idle PRIVATE consensus::transport)
	add_test(NAME consensus_transport_idle COMMAND consensus_transport_idle ${CMAKE_CURRENT_BINARY_DIR}/tls-fixture cost --quick)
	set_tests_properties(consensus_transport_idle PROPERTIES FIXTURES_REQUIRED consensus_tls TIMEOUT 120)
endif()
