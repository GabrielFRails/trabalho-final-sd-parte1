"""Experiment coordinator and XML-RPC log collector."""

import time
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

from constMP import (
    GROUPMNGR_ADDR,
    GROUPMNGR_RPC_PORT,
    RPC_LOG_WAIT_TIMEOUT,
    SERVER_BIND_ADDR,
    SERVER_RPC_PORT,
)


def rpc_url(host, port):
    return f"http://{host}:{port}"


class LogCollector:
    def __init__(self):
        self.logs = {}

    def submit_log(self, peer_id, messages):
        peer_id = int(peer_id)
        self.logs[peer_id] = messages
        print(f"Received log from peer {peer_id} ({len(messages)} messages)")
        return True

    def reset(self):
        self.logs.clear()


def prompt_user():
    return int(input("Enter the number of messages for each peer to send "
                     "(0 to terminate)=> "))


def get_peers():
    url = rpc_url(GROUPMNGR_ADDR, GROUPMNGR_RPC_PORT)
    with ServerProxy(url, allow_none=True) as group_manager:
        return group_manager.list_peers()


def prepare_peers(peers, n_msgs):
    for peer_id, peer in enumerate(peers):
        url = rpc_url(peer["host"], peer["rpc_port"])
        with ServerProxy(url, allow_none=True) as peer_rpc:
            reply = peer_rpc.prepare_experiment(peer_id, n_msgs)
        print(reply)


def start_peers(peers):
    for peer in peers:
        url = rpc_url(peer["host"], peer["rpc_port"])
        with ServerProxy(url, allow_none=True) as peer_rpc:
            reply = peer_rpc.start_experiment()
        print(reply)


def wait_for_logs(server, collector, expected_peers):
    deadline = time.monotonic() + RPC_LOG_WAIT_TIMEOUT
    while len(collector.logs) < expected_peers:
        if time.monotonic() >= deadline:
            missing = sorted(set(range(expected_peers)) - set(collector.logs))
            raise TimeoutError(f"Timed out waiting for peer logs: {missing}")
        server.handle_request()

    return [collector.logs[peer_id] for peer_id in range(expected_peers)]


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
    url = rpc_url(GROUPMNGR_ADDR, GROUPMNGR_RPC_PORT)
    with ServerProxy(url, allow_none=True) as group_manager:
        group_manager.stop()


def main():
    collector = LogCollector()
    with SimpleXMLRPCServer(
        (SERVER_BIND_ADDR, SERVER_RPC_PORT),
        allow_none=True,
        logRequests=False,
    ) as server:
        server.register_introspection_functions()
        server.register_instance(collector)
        server.timeout = 0.5
        print(
            "Comparison Server RPC listening on "
            f"{SERVER_BIND_ADDR}:{SERVER_RPC_PORT}"
        )

        while True:
            n_msgs = prompt_user()
            peers = get_peers()
            print("List of peers:", peers)
            collector.reset()
            prepare_peers(peers, n_msgs)

            if n_msgs == 0:
                print("Stopping.")
                stop_group_manager()
                break

            start_peers(peers)
            print("Now, wait for the message logs from the communicating peers...")
            logs = wait_for_logs(server, collector, len(peers))
            compare_logs(logs)


if __name__ == "__main__":
    main()
