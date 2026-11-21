"""Helpers shared by the gRPC clients (peers and Comparison Server)."""

from contextlib import contextmanager

import grpc

import mpcomm_pb2_grpc
from constMP import (
    GROUPMNGR_ADDR,
    GROUPMNGR_RPC_PORT,
    SERVER_ADDR,
    SERVER_RPC_PORT,
)


@contextmanager
def connect(host, port, stub_class):
    """Open a channel to host:port and yield a stub of ``stub_class``.

    Every call made through the stub should pass ``timeout=RPC_CALL_TIMEOUT``,
    which gRPC turns into a deadline (StatusCode.DEADLINE_EXCEEDED).
    """
    with grpc.insecure_channel(f"{host}:{port}") as channel:
        yield stub_class(channel)


def group_manager():
    return connect(GROUPMNGR_ADDR, GROUPMNGR_RPC_PORT,
                   mpcomm_pb2_grpc.GroupManagerStub)


def comparison_server():
    return connect(SERVER_ADDR, SERVER_RPC_PORT,
                   mpcomm_pb2_grpc.ComparisonServerStub)


def peer(peer_info):
    return connect(peer_info["host"], peer_info["rpc_port"],
                   mpcomm_pb2_grpc.PeerStub)


def describe_error(error):
    """Short text for an RpcError, e.g. 'UNAVAILABLE: failed to connect'."""
    if isinstance(error, grpc.RpcError) and hasattr(error, "code"):
        return f"{error.code().name}: {error.details()}"
    return str(error)
