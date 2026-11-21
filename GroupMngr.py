"""gRPC service responsible for peer membership."""

import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

import mpcomm_pb2
import mpcomm_pb2_grpc
from constMP import GROUPMNGR_BIND_ADDR, GROUPMNGR_RPC_PORT


def peer_key(peer_info):
    return (peer_info.host, peer_info.udp_port, peer_info.rpc_port)


class GroupManagerServicer(mpcomm_pb2_grpc.GroupManagerServicer):
    def __init__(self, stop_event):
        # gRPC handles requests in parallel threads, so membership needs a lock.
        self._lock = threading.Lock()
        self._membership = []
        self._stop_event = stop_event

    def Register(self, request, context):
        peer = peer_key(request)
        with self._lock:
            if peer not in self._membership:
                self._membership.append(peer)
        print("Registered peer:", peer)
        return mpcomm_pb2.Reply(message="registered")

    def ListPeers(self, request, context):
        with self._lock:
            peers = list(self._membership)
        print("List of peers sent:", peers)
        return mpcomm_pb2.PeerList(peers=[
            mpcomm_pb2.PeerInfo(host=host, udp_port=udp_port, rpc_port=rpc_port)
            for host, udp_port, rpc_port in peers
        ])

    def Unregister(self, request, context):
        peer = peer_key(request)
        with self._lock:
            removed = peer in self._membership
            if removed:
                self._membership.remove(peer)
        if removed:
            print("Unregistered peer:", peer)
            return mpcomm_pb2.Reply(message="unregistered")
        return mpcomm_pb2.Reply(message="peer was not registered")

    def Stop(self, request, context):
        print("Stopping Group Manager.")
        self._stop_event.set()
        return mpcomm_pb2.Reply(message="stopping")


def main():
    stop_event = threading.Event()
    server = grpc.server(ThreadPoolExecutor(max_workers=10))
    mpcomm_pb2_grpc.add_GroupManagerServicer_to_server(
        GroupManagerServicer(stop_event), server
    )
    server.add_insecure_port(f"{GROUPMNGR_BIND_ADDR}:{GROUPMNGR_RPC_PORT}")
    server.start()
    print(
        "Group Manager gRPC listening on "
        f"{GROUPMNGR_BIND_ADDR}:{GROUPMNGR_RPC_PORT}"
    )

    try:
        while not stop_event.wait(0.5):
            pass
    except KeyboardInterrupt:
        print("Interrupted.")
    finally:
        # The grace period lets the reply to Stop (and other calls) finish.
        server.stop(grace=1).wait()


if __name__ == "__main__":
    main()
