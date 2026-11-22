#!/bin/sh
# Regenera mpcomm_pb2.py e mpcomm_pb2_grpc.py a partir de protos/mpcomm.proto.
# Requer grpcio-tools (pip install -r requirements.txt).
set -e
cd "$(dirname "$0")"
python3 -m grpc_tools.protoc -Iprotos --python_out=. --grpc_python_out=. protos/mpcomm.proto
