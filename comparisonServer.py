"""Experiment coordinator and gRPC log collector."""

import threading
from concurrent.futures import ThreadPoolExecutor

import grpc

import mpcomm_pb2
import mpcomm_pb2_grpc
import rpcClient
from constMP import (
    RPC_CALL_TIMEOUT,
    RPC_LOG_WAIT_TIMEOUT,
    SERVER_BIND_ADDR,
    SERVER_RPC_PORT,
)


class PeerCallError(Exception):
    """A gRPC call to a specific peer failed."""

    def __init__(self, peer_id, peer, error):
        super().__init__(
            f"peer {peer_id} ({peer['host']}:{peer['rpc_port']}): "
            f"{rpcClient.describe_error(error)}"
        )


class LogCollector(mpcomm_pb2_grpc.ComparisonServerServicer):
    """Receives the peers' logs; SubmitLog runs in gRPC worker threads."""

    def __init__(self):
        self._condition = threading.Condition()
        self._logs = {}
        self._experiment_id = None

    def begin(self, experiment_id):
        with self._condition:
            self._experiment_id = experiment_id
            self._logs = {}

    def SubmitLog(self, request, context):
        with self._condition:
            if request.experiment_id != self._experiment_id:
                print(f"Ignoring log from peer {request.peer_id}: experiment "
                      f"{request.experiment_id} is not the current one "
                      f"({self._experiment_id})")
                return mpcomm_pb2.Reply(message="ignored: stale experiment")
            self._logs[request.peer_id] = [
                (entry.sender, entry.number) for entry in request.entries
            ]
            self._condition.notify_all()
        print(f"Received log from peer {request.peer_id} "
              f"({len(request.entries)} messages)")
        return mpcomm_pb2.Reply(message="log received")

    def wait_for_logs(self, expected_peers, timeout):
        with self._condition:
            complete = self._condition.wait_for(
                lambda: len(self._logs) >= expected_peers, timeout
            )
            if not complete:
                missing = sorted(set(range(expected_peers)) - set(self._logs))
                raise TimeoutError(f"Timed out waiting for peer logs: {missing}")
            return [self._logs[peer_id] for peer_id in range(expected_peers)]


def prompt_user():
    while True:
        try:
            text = input("Enter the number of messages for each peer to send "
                         "(0 to terminate)=> ")
        except EOFError:
            return 0
        try:
            n_msgs = int(text)
        except ValueError:
            print("Please enter an integer.")
            continue
        if n_msgs < 0:
            print("Please enter a number greater than or equal to zero.")
            continue
        return n_msgs


def get_peers():
    with rpcClient.group_manager() as group_manager:
        reply = group_manager.ListPeers(mpcomm_pb2.Empty(),
                                        timeout=RPC_CALL_TIMEOUT)
    return [
        {"host": p.host, "udp_port": p.udp_port, "rpc_port": p.rpc_port}
        for p in reply.peers
    ]


def call_peer(peer_id, peer, method_name, request):
    try:
        with rpcClient.peer(peer) as stub:
            reply = getattr(stub, method_name)(request, timeout=RPC_CALL_TIMEOUT)
    except grpc.RpcError as error:
        raise PeerCallError(peer_id, peer, error) from error
    print(reply.message)


def prepare_peers(peers, n_msgs, experiment_id):
    for peer_id, peer in enumerate(peers):
        request = mpcomm_pb2.PrepareRequest(
            peer_id=peer_id,
            n_messages=n_msgs,
            experiment_id=experiment_id,
        )
        call_peer(peer_id, peer, "PrepareExperiment", request)


def start_peers(peers):
    for peer_id, peer in enumerate(peers):
        call_peer(peer_id, peer, "StartExperiment", mpcomm_pb2.Empty())


def abort_peers(peers):
    """Best effort: release every peer after a failed round."""
    for peer_id, peer in enumerate(peers):
        try:
            call_peer(peer_id, peer, "AbortExperiment", mpcomm_pb2.Empty())
        except PeerCallError as error:
            print("Could not abort:", error)


def stop_peers(peers):
    """Ask every peer to terminate (PrepareExperiment with zero messages)."""
    for peer_id, peer in enumerate(peers):
        request = mpcomm_pb2.PrepareRequest(peer_id=peer_id, n_messages=0)
        try:
            call_peer(peer_id, peer, "PrepareExperiment", request)
        except PeerCallError as error:
            print("Could not stop:", error)


def compare_logs(logs):
    """Compare the observed global order while reporting missing messages."""
    if not logs:
        print("No logs received.")
        return

    lengths = [len(log) for log in logs]
    maximum_length = max(lengths)
    unordered = 0

    for index in range(maximum_length):
        values = [log[index] if index < len(log) else None for log in logs]
        if any(value != values[0] for value in values[1:]):
            unordered += 1

    print("Messages received by each peer:", lengths)
    print(f"Found {unordered} unordered message positions")


def stop_group_manager():
    try:
        with rpcClient.group_manager() as group_manager:
            group_manager.Stop(mpcomm_pb2.Empty(), timeout=RPC_CALL_TIMEOUT)
    except grpc.RpcError as error:
        print("Could not stop the Group Manager:",
              rpcClient.describe_error(error))


def run_experiment(collector, peers, n_msgs, experiment_id):
    collector.begin(experiment_id)
    try:
        prepare_peers(peers, n_msgs, experiment_id)
        start_peers(peers)
        print(f"Experiment {experiment_id}: waiting for the message logs "
              "from the communicating peers...")
        logs = collector.wait_for_logs(len(peers), RPC_LOG_WAIT_TIMEOUT)
    except (PeerCallError, TimeoutError) as error:
        print(f"Experiment {experiment_id} failed: {error}")
        print("Aborting the experiment on all peers.")
        abort_peers(peers)
        return
    compare_logs(logs)


def coordinate(collector):
    experiment_id = 0
    while True:
        n_msgs = prompt_user()
        try:
            peers = get_peers()
        except grpc.RpcError as error:
            print("Could not reach the Group Manager:",
                  rpcClient.describe_error(error))
            if n_msgs == 0:
                return
            continue
        print("List of peers:", peers)

        if n_msgs == 0:
            print("Stopping.")
            stop_peers(peers)
            stop_group_manager()
            return

        if not peers:
            print("No peers registered. Start the peers and try again.")
            continue

        experiment_id += 1
        run_experiment(collector, peers, n_msgs, experiment_id)


def main():
    collector = LogCollector()
    server = grpc.server(ThreadPoolExecutor(max_workers=16))
    mpcomm_pb2_grpc.add_ComparisonServerServicer_to_server(collector, server)
    server.add_insecure_port(f"{SERVER_BIND_ADDR}:{SERVER_RPC_PORT}")
    server.start()
    print(f"Comparison Server gRPC listening on {SERVER_BIND_ADDR}:{SERVER_RPC_PORT}")

    try:
        coordinate(collector)
    except KeyboardInterrupt:
        print("\nInterrupted.")
    finally:
        server.stop(grace=1).wait()


if __name__ == "__main__":
    main()
