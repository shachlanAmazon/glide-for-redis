for VARIABLE in 1 2 3 4 5 6 7 8 9 1 1 1 1
do
  echo *****BASELINE*****
#   git checkout csharp-babushka-client && git submodule update 
  ./install_and_test.sh -node -csharp -host $HOST -prefix baseline
  ./install_and_test.sh -node -csharp -no-tls -prefix baseline-local
  ./install_and_test.sh -node -is-cluster  -host $CLUSTER_HOST -prefix baseline
#   echo *****MULTIPLEX*****
#   git checkout csharp-test && git submodule update 
#   ./install_and_test.sh -csharp -tasks 10 100 1000 -data 100 -only-ffi -host $HOST -prefix multiplex
#   ./install_and_test.sh -csharp -tasks 10 100 1000 -data 100 -only-ffi -no-tls -prefix multiplex-local
#   ./install_and_test.sh -csharp -tasks 10 100 1000 -data 100 -only-socket -is-cluster  -host $CLUSTER_HOST -prefix multiplex
#   echo *****FLATBUFFERS*****
#   git checkout flat-buffers && git submodule update && ./install_and_test.sh -node -tasks 10 100 1000 -data 100 -only-socket -host $HOST -prefix flatbuffers
#   ./install_and_test.sh -node -tasks 10 100 1000 -data 100 -only-socket -is-cluster  -host $CLUSTER_HOST -prefix flatbuffers
done
