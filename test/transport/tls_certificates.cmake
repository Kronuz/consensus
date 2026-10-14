if(NOT DEFINED OUTPUT OR NOT DEFINED OPENSSL)
  message(FATAL_ERROR "TLS fixture arguments missing")
endif()
file(MAKE_DIRECTORY "${OUTPUT}")
function(run_openssl)
  execute_process(COMMAND "${OPENSSL}" ${ARGN} WORKING_DIRECTORY "${OUTPUT}"
    RESULT_VARIABLE result OUTPUT_QUIET ERROR_VARIABLE error)
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "TLS fixture generation failed: ${error}")
  endif()
endfunction()
run_openssl(req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=Consensus-Test-CA
  -keyout ca.key -out ca.pem -addext basicConstraints=critical,CA:TRUE
  -addext keyUsage=critical,keyCertSign,cRLSign -addext subjectKeyIdentifier=hash)
file(WRITE "${OUTPUT}/leaf.ext" "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:localhost\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n")
foreach(node IN ITEMS one two three)
  run_openssl(req -new -newkey rsa:2048 -nodes -subj "/CN=${node}"
    -keyout "${node}.key" -out "${node}.csr")
  run_openssl(x509 -req -in "${node}.csr" -CA ca.pem -CAkey ca.key -CAcreateserial
    -days 1 -extfile leaf.ext -out "${node}.pem")
endforeach()
run_openssl(req -x509 -newkey rsa:2048 -nodes -days 1 -subj /CN=Untrusted-CA
  -keyout rogue.key -out rogue.pem -addext basicConstraints=critical,CA:TRUE)
