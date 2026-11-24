# The admission tests' certificates: a cluster CA, and leaves whose URI names a node of cluster
# kronuz.test (n1 to n4, n3b a re-keyed n3, n3r a renewed n3: its key, a new certificate), another
# cluster's (other), none (bare), two nodes (twice), and node 3 signed by another CA (rogue3); a
# second CA (ca2) signs n1's and n2's keys again (n1c2, n2c2), and a new key for node 1 (n1k).
if(NOT DEFINED OUTPUT OR NOT DEFINED OPENSSL)
  message(FATAL_ERROR "admission fixture arguments missing")
endif()
file(MAKE_DIRECTORY "${OUTPUT}")
function(run_openssl)
  execute_process(COMMAND "${OPENSSL}" ${ARGN} WORKING_DIRECTORY "${OUTPUT}"
    RESULT_VARIABLE result OUTPUT_QUIET ERROR_VARIABLE error)
  if(NOT result EQUAL 0)
    message(FATAL_ERROR "admission fixture generation failed: ${error}")
  endif()
endfunction()
foreach(ca IN ITEMS ca rogue ca2)
  run_openssl(req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 -subj /CN=Admission-${ca}
    -keyout ${ca}.key -out ${ca}.pem -addext basicConstraints=critical,CA:TRUE
    -addext keyUsage=critical,keyCertSign,cRLSign -addext subjectKeyIdentifier=hash)
endforeach()
# Signs ${name}.csr with `ca` into ${name}.pem, its URIs `uris`.
function(sign name ca uris)
  set(san "DNS:localhost")
  foreach(uri IN LISTS uris)
    string(APPEND san ",URI:${uri}")
  endforeach()
  file(WRITE "${OUTPUT}/${name}.ext" "basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=${san}\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n")
  run_openssl(x509 -req -in "${name}.csr" -CA ${ca}.pem -CAkey ${ca}.key -CAcreateserial
    -days 1 -extfile "${name}.ext" -out "${name}.pem")
endfunction()
function(leaf name ca uris)
  run_openssl(req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj "/CN=${name}"
    -keyout "${name}.key" -out "${name}.csr")
  sign(${name} ${ca} "${uris}")
endfunction()
# A new certificate for the key of the leaf `key`.
function(renewed name key ca uris)
  configure_file("${OUTPUT}/${key}.key" "${OUTPUT}/${name}.key" COPYONLY)
  run_openssl(req -new -key "${name}.key" -subj "/CN=${name}" -out "${name}.csr")
  sign(${name} ${ca} "${uris}")
endfunction()
foreach(n IN ITEMS 1 2 3 4)
  leaf(n${n} ca "spiffe://kronuz.test/node/${n}")
endforeach()
leaf(n3b ca "spiffe://kronuz.test/node/3")
leaf(other ca "spiffe://elsewhere.test/node/4")
leaf(bare ca "")
leaf(twice ca "spiffe://kronuz.test/node/3;spiffe://kronuz.test/node/5")
leaf(padded ca "spiffe://kronuz.test/node/03")
leaf(rogue3 rogue "spiffe://kronuz.test/node/3")
renewed(n3r n3 ca "spiffe://kronuz.test/node/3")
renewed(n1c2 n1 ca2 "spiffe://kronuz.test/node/1")
renewed(n2c2 n2 ca2 "spiffe://kronuz.test/node/2")
leaf(n1k ca2 "spiffe://kronuz.test/node/1")
