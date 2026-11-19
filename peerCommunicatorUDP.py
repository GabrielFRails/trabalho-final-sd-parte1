"""Peer process: XML-RPC control plane and UDP message exchange."""

import pickle
import random
import threading
import time
from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

from constMP import (
    GROUPMNGR_ADDR,
    GROUPMNGR_RPC_PORT,
    PEER_ADVERTISED_HOST,
    PEER_BIND_ADDR,
    PEER_RPC_PORT,
    PEER_UDP_PORT,
    SERVER_ADDR,
    SERVER_RPC_PORT,
)


def rpc_url(host, port):
    return f"http://{host}:{port}"


def register_with_group_manager():
    url = rpc_url(GROUPMNGR_ADDR, GROUPMNGR_RPC_PORT)
    print("Connecting to Group Manager RPC:", url)
    with ServerProxy(url, allow_none=True) as group_manager:
        group_manager.register(
            PEER_ADVERTISED_HOST,
            PEER_UDP_PORT,
            PEER_RPC_PORT,
        )
    print(
        "Registered peer:",
        (PEER_ADVERTISED_HOST, PEER_UDP_PORT, PEER_RPC_PORT),
    )


def get_list_of_peers():
    url = rpc_url(GROUPMNGR_ADDR, GROUPMNGR_RPC_PORT)
    with ServerProxy(url, allow_none=True) as group_manager:
        peers = group_manager.list_peers()
    print("Got list of peers:", peers)
    return peers


def submit_log(peer_id, messages):
    url = rpc_url(SERVER_ADDR, SERVER_RPC_PORT)
    print("Sending the message log to the Comparison Server RPC...")
    with ServerProxy(url, allow_none=True) as comparison_server:
        comparison_server.submit_log(peer_id, messages)


class MessageHandler(threading.Thread):
    def __init__(self, sock, peer_id, expected_peers):
        super().__init__(name=f"message-handler-{peer_id}")
        self.sock = sock
        self.peer_id = peer_id
        self.expected_peers = expected_peers
        self.handshake_complete = threading.Event()
        self.log = []

    def run(self):
        print("Handler is ready. Waiting for the handshakes...")
        handshakes = set()
        early_messages = []

        while len(handshakes) < self.expected_peers:
            message = pickle.loads(self.sock.recv(65535))
            if message[0] == "READY":
                handshakes.add(int(message[1]))
                print("--- Handshake received:", message[1])
            else:
                early_messages.append(message)

        self.handshake_complete.set()
        print("Received all handshakes. Receiving messages.")

        finished_peers = set()
        for message in early_messages:
            self._process_message(message, finished_peers)

        while len(finished_peers) < self.expected_peers:
            message = pickle.loads(self.sock.recv(65535))
            self._process_message(message, finished_peers)

        with open(f"logfile{self.peer_id}.log", "w", encoding="utf-8") as log_file:
            log_file.write(str(self.log))

        submit_log(self.peer_id, self.log)

    def _process_message(self, message, finished_peers):
        if message[0] == -1:
            finished_peers.add(int(message[1]))
            return

        print(f"Message {message[1]} from process {message[0]}")
        self.log.append(message)


def send_udp(sock, peer, message):
    packet = pickle.dumps(message)
    sock.sendto(packet, (peer["host"], peer["udp_port"]))


class PeerExperiment:
    def __init__(self, peer_id, n_messages):
        self.peer_id = peer_id
        self.n_messages = n_messages
        self.peers = get_list_of_peers()
        if not self.peers:
            raise RuntimeError("No peers registered with the Group Manager")

        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.receive_socket = socket(AF_INET, SOCK_DGRAM)
        self.receive_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        self.receive_socket.bind((PEER_BIND_ADDR, PEER_UDP_PORT))
        self.handler = MessageHandler(
            self.receive_socket,
            self.peer_id,
            len(self.peers),
        )
        self.handler.start()
        self.sender_thread = None

    def start(self):
        self.sender_thread = threading.Thread(
            target=self._send_messages,
            name=f"message-sender-{self.peer_id}",
        )
        self.sender_thread.start()

    def is_running(self):
        return self.sender_thread is None or self.sender_thread.is_alive()

    def _send_messages(self):
        try:
            print("I am up, and my ID is:", self.peer_id)
            for peer in self.peers:
                print("Sending handshake to", peer)
                send_udp(self.send_socket, peer, ("READY", self.peer_id))

            self.handler.handshake_complete.wait()

            for message_number in range(self.n_messages):
                time.sleep(random.randrange(10, 100) / 1000)
                message = (self.peer_id, message_number)
                for peer in self.peers:
                    send_udp(self.send_socket, peer, message)
                print(f"Sent message {message_number}")

            for peer in self.peers:
                send_udp(self.send_socket, peer, (-1, self.peer_id))

            self.handler.join()
        finally:
            self.send_socket.close()
            self.receive_socket.close()


class PeerControl:
    def __init__(self):
        self.experiment = None
        self.stop_requested = False

    def prepare_experiment(self, peer_id, n_messages):
        peer_id = int(peer_id)
        n_messages = int(n_messages)
        if n_messages == 0:
            print("Terminating.")
            self.stop_requested = True
            return f"Peer process {peer_id} stopping."

        if self.experiment is not None and self.experiment.is_running():
            raise RuntimeError("Peer is already running an experiment")

        self.experiment = PeerExperiment(peer_id, n_messages)
        return f"Peer process {peer_id} prepared."

    def start_experiment(self):
        if self.experiment is None:
            raise RuntimeError("Peer has no prepared experiment")
        self.experiment.start()
        return f"Peer process {self.experiment.peer_id} started."


def main():
    register_with_group_manager()
    control = PeerControl()

    with SimpleXMLRPCServer(
        (PEER_BIND_ADDR, PEER_RPC_PORT),
        allow_none=True,
        logRequests=False,
    ) as server:
        server.register_introspection_functions()
        server.register_instance(control)
        server.timeout = 0.5
        print(f"Peer RPC listening on {PEER_BIND_ADDR}:{PEER_RPC_PORT}")
        print("Waiting for RPC signal to start...")

        while not control.stop_requested:
            server.handle_request()


if __name__ == "__main__":
    main()
