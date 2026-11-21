"""Shared configuration for the MPComm experiment.

Every setting can be overridden with an environment variable. The defaults
preserve the ports used by the original project, while the addresses default
to localhost so the project does not depend on the professor's old VM.
"""

import os


def _env_int(name, default):
    return int(os.getenv(name, str(default)))


PEER_BIND_ADDR = os.getenv("PEER_BIND_ADDR", "0.0.0.0")
PEER_ADVERTISED_HOST = os.getenv("PEER_ADVERTISED_HOST", "127.0.0.1")
PEER_UDP_PORT = _env_int("PEER_UDP_PORT", 6789)
PEER_RPC_PORT = _env_int("PEER_RPC_PORT", 5679)

SERVER_BIND_ADDR = os.getenv("SERVER_BIND_ADDR", "0.0.0.0")
SERVER_ADDR = os.getenv("SERVER_ADDR", "127.0.0.1")
SERVER_RPC_PORT = _env_int("SERVER_RPC_PORT", 5678)

GROUPMNGR_BIND_ADDR = os.getenv("GROUPMNGR_BIND_ADDR", "0.0.0.0")
GROUPMNGR_ADDR = os.getenv("GROUPMNGR_ADDR", "127.0.0.1")
GROUPMNGR_RPC_PORT = _env_int("GROUPMNGR_RPC_PORT", 5680)

RPC_LOG_WAIT_TIMEOUT = _env_int("RPC_LOG_WAIT_TIMEOUT", 60)
# Deadline of each individual gRPC call, in seconds.
RPC_CALL_TIMEOUT = float(os.getenv("RPC_CALL_TIMEOUT", "10"))

