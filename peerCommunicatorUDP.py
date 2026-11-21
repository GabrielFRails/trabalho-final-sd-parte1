"""Peer process: gRPC control plane and UDP message exchange."""

import pickle
import random
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from socket import AF_INET, SOCK_DGRAM, SOL_SOCKET, SO_REUSEADDR, socket

import grpc

import mpcomm_pb2
import mpcomm_pb2_grpc
import rpcClient
from constMP import (
    PEER_ADVERTISED_HOST,
    PEER_BIND_ADDR,
    PEER_RPC_PORT,
    PEER_UDP_PORT,
    RPC_CALL_TIMEOUT,
)

MY_INFO = mpcomm_pb2.PeerInfo(
    host=PEER_ADVERTISED_HOST,
    udp_port=PEER_UDP_PORT,
    rpc_port=PEER_RPC_PORT,
)


def register_with_group_manager():
    print("Connecting to Group Manager gRPC...")
    with rpcClient.group_manager() as group_manager:
        group_manager.Register(MY_INFO, timeout=RPC_CALL_TIMEOUT)
    print("Registered peer:", (MY_INFO.host, MY_INFO.udp_port, MY_INFO.rpc_port))


def unregister_from_group_manager():
    try:
        with rpcClient.group_manager() as group_manager:
            group_manager.Unregister(MY_INFO, timeout=RPC_CALL_TIMEOUT)
        print("Unregistered from the Group Manager.")
    except grpc.RpcError as error:
        # Expected when the Group Manager has already been stopped.
        print("Could not unregister:", rpcClient.describe_error(error))


def get_list_of_peers():
    with rpcClient.group_manager() as group_manager:
        reply = group_manager.ListPeers(mpcomm_pb2.Empty(),
                                        timeout=RPC_CALL_TIMEOUT)
    peers = [
        {"host": p.host, "udp_port": p.udp_port, "rpc_port": p.rpc_port}
        for p in reply.peers
    ]
    print("Got list of peers:", peers)
    return peers


def submit_log(peer_id, experiment_id, messages):
    print("Sending the message log to the Comparison Server via gRPC...")
    request = mpcomm_pb2.LogRequest(
        peer_id=peer_id,
        experiment_id=experiment_id,
        entries=[
            mpcomm_pb2.LogEntry(sender=sender, number=number)
            for sender, number in messages
        ],
    )
    with rpcClient.comparison_server() as comparison_server:
        comparison_server.SubmitLog(request, timeout=RPC_CALL_TIMEOUT)


class MessageHandler(threading.Thread):
    def __init__(self, sock, peer_id, experiment_id, expected_peers, aborted):
        super().__init__(name=f"message-handler-{peer_id}")
        self.sock = sock
        self.peer_id = peer_id
        self.experiment_id = experiment_id
        self.expected_peers = expected_peers
        self.aborted = aborted
        self.handshake_complete = threading.Event()
        self.log = []

    def _receive(self):
        """Next UDP message, or None if the experiment was aborted."""
        while not self.aborted.is_set():
            try:
                return pickle.loads(self.sock.recv(65535))
            except TimeoutError:
                continue
            except OSError:
                return None  # socket closed by abort()
        return None

    def run(self):
        print("Handler is ready. Waiting for the handshakes...")
        handshakes = set()
        early_messages = []

        while len(handshakes) < self.expected_peers:
            message = self._receive()
            if message is None:
                return
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
            message = self._receive()
            if message is None:
                return
            self._process_message(message, finished_peers)

        with open(f"logfile{self.peer_id}.log", "w", encoding="utf-8") as log_file:
            log_file.write(str(self.log))

        try:
            submit_log(self.peer_id, self.experiment_id, self.log)
        except grpc.RpcError as error:
            print("Could not submit the log:", rpcClient.describe_error(error))

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
    def __init__(self, peer_id, n_messages, experiment_id):
        self.peer_id = peer_id
        self.n_messages = n_messages
        self.experiment_id = experiment_id
        self.peers = get_list_of_peers()
        if not self.peers:
            raise RuntimeError("No peers registered with the Group Manager")

        self.aborted = threading.Event()
        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.receive_socket = socket(AF_INET, SOCK_DGRAM)
        self.receive_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        # The timeout lets the handler notice an abort while waiting.
        self.receive_socket.settimeout(0.5)
        try:
            self.receive_socket.bind((PEER_BIND_ADDR, PEER_UDP_PORT))
        except OSError:
            self._close_sockets()
            raise
        self.handler = MessageHandler(
            self.receive_socket,
            self.peer_id,
            self.experiment_id,
            len(self.peers),
            self.aborted,
        )
        self.handler.start()
        self.sender_thread = None

    @property
    def started(self):
        return self.sender_thread is not None

    def is_running(self):
        """True while a started experiment still has active threads."""
        return self.started and (
            self.sender_thread.is_alive() or self.handler.is_alive()
        )

    def start(self):
        self.sender_thread = threading.Thread(
            target=self._send_messages,
            name=f"message-sender-{self.peer_id}",
        )
        self.sender_thread.start()

    def abort(self):
        """Stop the experiment, prepared or running, and release the sockets."""
        self.aborted.set()
        self.handler.handshake_complete.set()  # wake the sender, if waiting
        self.handler.join(timeout=2)
        if self.sender_thread is not None:
            self.sender_thread.join(timeout=2)
        self._close_sockets()

    def _close_sockets(self):
        self.send_socket.close()
        self.receive_socket.close()

    def _send_messages(self):
        try:
            print("I am up, and my ID is:", self.peer_id)
            for peer in self.peers:
                print("Sending handshake to", peer)
                send_udp(self.send_socket, peer, ("READY", self.peer_id))

            self.handler.handshake_complete.wait()

            for message_number in range(self.n_messages):
                if self.aborted.is_set():
                    return
                time.sleep(random.randrange(10, 100) / 1000)
                message = (self.peer_id, message_number)
                for peer in self.peers:
                    send_udp(self.send_socket, peer, message)
                print(f"Sent message {message_number}")

            if self.aborted.is_set():
                return
            for peer in self.peers:
                send_udp(self.send_socket, peer, (-1, self.peer_id))

            self.handler.join()
        except OSError:
            if not self.aborted.is_set():
                raise
        finally:
            self._close_sockets()


class PeerServicer(mpcomm_pb2_grpc.PeerServicer):
    def __init__(self, stop_event):
        self._lock = threading.Lock()
        self._stop_event = stop_event
        self.experiment = None

    def PrepareExperiment(self, request, context):
        with self._lock:
            if request.n_messages == 0:
                print("Terminating.")
                self._discard_experiment()
                self._stop_event.set()
                return mpcomm_pb2.Reply(
                    message=f"Peer process {request.peer_id} stopping."
                )

            current = self.experiment
            if (current is not None and current.is_running()
                    and current.experiment_id == request.experiment_id):
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              "Peer is already running this experiment")

            # There is a single coordinator, so a new experiment id means the
            # previous round is over. It may have finished, never started, or
            # be stuck because its abort was lost; in any case discard it.
            if current is not None and current.is_running():
                print(f"Discarding unfinished experiment {current.experiment_id}.")
            self._discard_experiment()
            try:
                self.experiment = PeerExperiment(
                    request.peer_id,
                    request.n_messages,
                    request.experiment_id,
                )
            except (OSError, RuntimeError, grpc.RpcError) as error:
                message = f"Could not prepare: {rpcClient.describe_error(error)}"
                print(message)
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, message)

            return mpcomm_pb2.Reply(
                message=f"Peer process {request.peer_id} prepared "
                        f"(experiment {request.experiment_id})."
            )

    def StartExperiment(self, request, context):
        with self._lock:
            if self.experiment is None or self.experiment.started:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                              "Peer has no prepared experiment")
            self.experiment.start()
            return mpcomm_pb2.Reply(
                message=f"Peer process {self.experiment.peer_id} started."
            )

    def AbortExperiment(self, request, context):
        with self._lock:
            had_experiment = self.experiment is not None
            self._discard_experiment()
        if had_experiment:
            print("Experiment aborted by the coordinator.")
            return mpcomm_pb2.Reply(message="experiment aborted")
        return mpcomm_pb2.Reply(message="no experiment to abort")

    def shutdown(self):
        with self._lock:
            self._discard_experiment()

    def _discard_experiment(self):
        # Caller must hold self._lock.
        if self.experiment is not None:
            self.experiment.abort()
            self.experiment = None


def main():
    stop_event = threading.Event()
    # Ctrl+C or `kill` end the peer cleanly, so it unregisters before exiting.
    for signal_number in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signal_number, lambda *_: stop_event.set())
    servicer = PeerServicer(stop_event)
    server = grpc.server(ThreadPoolExecutor(max_workers=4))
    mpcomm_pb2_grpc.add_PeerServicer_to_server(servicer, server)
    server.add_insecure_port(f"{PEER_BIND_ADDR}:{PEER_RPC_PORT}")
    # Start serving before registering, so the coordinator can always reach
    # a peer that appears in the membership list.
    server.start()
    print(f"Peer gRPC listening on {PEER_BIND_ADDR}:{PEER_RPC_PORT}")

    registered = False
    try:
        register_with_group_manager()
        registered = True
        print("Waiting for RPC signal to start...")
        while not stop_event.wait(0.5):
            pass
        print("Shutting down.")
    except grpc.RpcError as error:
        print("Could not register with the Group Manager:",
              rpcClient.describe_error(error))
    finally:
        servicer.shutdown()
        if registered:
            unregister_from_group_manager()
        server.stop(grace=1).wait()


if __name__ == "__main__":
    main()
