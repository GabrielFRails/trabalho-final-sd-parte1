"""Peer process: gRPC control plane and reliable UDP message exchange."""

import random
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
from socket import (
    AF_INET,
    SO_RCVBUF,
    SO_REUSEADDR,
    SOCK_DGRAM,
    SOL_SOCKET,
    socket,
)

import grpc

import mpcomm_pb2
import mpcomm_pb2_grpc
import reliableUDP
import rpcClient
from constMP import (
    PEER_ADVERTISED_HOST,
    PEER_BIND_ADDR,
    PEER_RPC_PORT,
    PEER_SEND_MAX_MS,
    PEER_SEND_MIN_MS,
    PEER_UDP_PORT,
    RPC_CALL_TIMEOUT,
    UDP_LOSS_RATE,
    UDP_MAX_RETRIES,
    UDP_RCVBUF,
    UDP_RETRANSMIT_TIMEOUT,
)
from reliableUDP import DATA, READY, STOP

MY_INFO = mpcomm_pb2.PeerInfo(
    host=PEER_ADVERTISED_HOST,
    udp_port=PEER_UDP_PORT,
    rpc_port=PEER_RPC_PORT,
)

# Intervalo com que as threads verificam se a rodada foi abortada.
_POLL = 0.1
# Tempo que o peer segue respondendo ACKs depois de entregar o log, para
# atender quem não recebeu o último ACK e ainda retransmite (como o TIME_WAIT
# do TCP).
_LINGER = 2.0


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


def submit_log(peer_id, experiment_id, messages, stats):
    print("Sending the message log to the Comparison Server via gRPC...")
    request = mpcomm_pb2.LogRequest(
        peer_id=peer_id,
        experiment_id=experiment_id,
        entries=[
            mpcomm_pb2.LogEntry(sender=sender, number=number)
            for sender, number in messages
        ],
        stats=mpcomm_pb2.PeerStats(**asdict(stats)),
    )
    with rpcClient.comparison_server() as comparison_server:
        comparison_server.SubmitLog(request, timeout=RPC_CALL_TIMEOUT)


class MessageHandler(threading.Thread):
    """Recebe os pacotes UDP de uma rodada e monta o log de entregas.

    ACKs, retransmissões e duplicatas são resolvidos pelo ReliableChannel;
    aqui só chegam pacotes novos. A thread continua rodando depois que tudo
    foi entregue, para seguir respondendo ACKs, até ``stop_receiving``.
    """

    def __init__(self, sock, channel, peer_id, experiment_id, expected_peers,
                 stop_receiving):
        super().__init__(name=f"message-handler-{peer_id}", daemon=True)
        self.sock = sock
        self.channel = channel
        self.peer_id = peer_id
        self.experiment_id = experiment_id
        self.expected_peers = expected_peers
        self.stop_receiving = stop_receiving
        self.handshake_complete = threading.Event()
        self.delivery_complete = threading.Event()
        self.log = []
        self._handshakes = set()
        self._announced = {}  # remetente -> quantidade de DATA (do STOP)
        self._delivered = {}  # remetente -> quantidade de DATA entregues

    def run(self):
        print("Handler is ready. Waiting for the handshakes...")
        while not self.stop_receiving.is_set():
            try:
                raw, addr = self.sock.recvfrom(65535)
            except (TimeoutError, ConnectionError):
                # ConnectionError: no Windows, um ICMP "porta inacessível"
                # de um envio anterior aparece no recvfrom; basta ignorar.
                continue
            except OSError:
                return  # socket fechado pelo fim ou abort da rodada

            packet = reliableUDP.decode(raw)
            if packet is None or packet["exp"] != self.experiment_id:
                continue  # datagrama inválido ou de outra rodada
            packet = self.channel.receive(packet, addr)
            if packet is not None:
                self._handle(packet)

    def _handle(self, packet):
        kind, src, seq = packet["t"], packet["src"], packet["seq"]
        if kind == READY:
            self._handshakes.add(src)
            print("--- Handshake received:", src)
            if len(self._handshakes) >= self.expected_peers:
                if not self.handshake_complete.is_set():
                    print("Received all handshakes. Receiving messages.")
                self.handshake_complete.set()
        elif kind == DATA:
            self._on_data(src, seq)
        elif kind == STOP:
            self._announced[src] = seq
        self._check_complete()

    def _on_data(self, src, seq):
        # ------------------------------------------------------------------
        # PARTE 1b, SEGUNDA METADE: ORDENAÇÃO FIFO (a implementar)
        #
        # Aqui chega cada DATA exatamente uma vez (o canal já descartou as
        # duplicatas), mas não necessariamente em ordem: se a mensagem 3 de
        # um remetente se perder, a 4 chega antes e a 3 só vem na
        # retransmissão. Hoje ela é entregue assim que chega, e o
        # comparador acusa isso como "FIFO violations".
        #
        # Para FIFO: manter, por remetente, o próximo número esperado e uma
        # fila de espera (hold-back queue). Entregar com _deliver somente
        # quando seq for o esperado, e em seguida liberar da fila as
        # mensagens consecutivas que já estavam esperando.
        # ------------------------------------------------------------------
        self._deliver(src, seq)

    def _deliver(self, src, seq):
        """Entrega a mensagem à aplicação, isto é, registra no log."""
        print(f"Message {seq} from process {src}")
        self.log.append((src, seq))
        self._delivered[src] = self._delivered.get(src, 0) + 1

    def _check_complete(self):
        """A rodada termina quando todos anunciaram o fim (STOP) e todas as
        mensagens anunciadas foram entregues."""
        if self.delivery_complete.is_set():
            return
        if len(self._announced) < self.expected_peers:
            return
        if all(self._delivered.get(src, 0) >= count
               for src, count in self._announced.items()):
            print("All messages delivered.")
            self.delivery_complete.set()


class PeerExperiment:
    def __init__(self, peer_id, n_messages, experiment_id):
        self.peer_id = peer_id
        self.n_messages = n_messages
        self.experiment_id = experiment_id
        self.peers = get_list_of_peers()
        if not self.peers:
            raise RuntimeError("No peers registered with the Group Manager")

        self.aborted = threading.Event()
        self.stop_receiving = threading.Event()
        self.finished = False  # log pronto; a rodada só responde ACKs

        # Um único socket envia e recebe, para que os ACKs voltem à porta
        # UDP divulgada (o endereço de origem dos datagramas).
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        if UDP_RCVBUF > 0:
            self.sock.setsockopt(SOL_SOCKET, SO_RCVBUF, UDP_RCVBUF)
        # O timeout deixa o receptor perceber o fim ou abort da rodada.
        self.sock.settimeout(_POLL)
        try:
            self.sock.bind((PEER_BIND_ADDR, PEER_UDP_PORT))
        except OSError:
            self.sock.close()
            raise

        self.channel = reliableUDP.ReliableChannel(
            self.sock,
            self.peer_id,
            self.experiment_id,
            self.peers,
            loss_rate=UDP_LOSS_RATE,
            retransmit_timeout=UDP_RETRANSMIT_TIMEOUT,
            max_retries=UDP_MAX_RETRIES,
        )
        self.handler = MessageHandler(
            self.sock,
            self.channel,
            self.peer_id,
            self.experiment_id,
            len(self.peers),
            self.stop_receiving,
        )
        self.channel.start()
        self.handler.start()
        self.sender_thread = None
        if UDP_LOSS_RATE > 0:
            print(f"Simulating UDP loss: {UDP_LOSS_RATE:.0%} of the datagrams")

    @property
    def started(self):
        return self.sender_thread is not None

    def is_running(self):
        """True enquanto uma rodada iniciada não terminou.

        Depois de entregar o log o peer ainda responde ACKs por _LINGER
        segundos, mas a rodada já é considerada concluída.
        """
        return (self.started and not self.finished
                and (self.sender_thread.is_alive() or self.handler.is_alive()))

    def start(self):
        self.sender_thread = threading.Thread(
            target=self._run,
            name=f"message-sender-{self.peer_id}",
            daemon=True,
        )
        self.sender_thread.start()

    def abort(self):
        """Stop the experiment, prepared or running, and release the socket."""
        self.aborted.set()
        self.stop_receiving.set()
        self.handler.join(timeout=2)
        if self.sender_thread is not None:
            self.sender_thread.join(timeout=2)
        self._close()

    def _close(self):
        self.stop_receiving.set()
        self.channel.close()
        self.sock.close()

    def _wait(self, wait_step):
        """Repete wait_step(_POLL) até ele dar True; False se abortar antes."""
        while not wait_step(_POLL):
            if self.aborted.is_set():
                return False
        return not self.aborted.is_set()

    def _run(self):
        try:
            print("I am up, and my ID is:", self.peer_id)
            print(f"Sending handshake to {len(self.peers)} peers")
            self.channel.multicast(READY, 0)
            if not self._wait(self.handler.handshake_complete.wait):
                return

            for message_number in range(self.n_messages):
                pause = random.uniform(PEER_SEND_MIN_MS, PEER_SEND_MAX_MS)
                if self.aborted.wait(pause / 1000):
                    return
                self.channel.multicast(DATA, message_number)
                print(f"Sent message {message_number}")

            # O STOP leva a quantidade de mensagens enviadas: assim quem
            # recebe sabe quantas esperar, mesmo que o STOP chegue antes de
            # alguma retransmissão.
            self.channel.multicast(STOP, self.n_messages)

            # Termina quando recebeu tudo de todos e todos confirmaram tudo
            # o que este peer enviou.
            if not self._wait(self.handler.delivery_complete.wait):
                return
            if not self._wait(self.channel.wait_until_acked):
                return
            self._finish()

            # Continua respondendo ACKs: outro peer pode não ter recebido o
            # último ACK deste e ainda estar retransmitindo.
            self.aborted.wait(_LINGER)
        except OSError:
            if not self.aborted.is_set():
                raise
        finally:
            self._close()
            self.handler.join(timeout=2)

    def _finish(self):
        # Marca antes do RPC: assim que o coordenador recebe o último log ele
        # pode iniciar a próxima rodada, e esta já deve contar como concluída.
        self.finished = True
        log = list(self.handler.log)
        stats = self.channel.stats()
        with open(f"logfile{self.peer_id}.log", "w", encoding="utf-8") as log_file:
            log_file.write(str(log))
        print("UDP stats:", ", ".join(f"{k}={v}" for k, v in asdict(stats).items()))
        if stats.gave_up:
            print(f"WARNING: gave up on {stats.gave_up} packets after "
                  f"{UDP_MAX_RETRIES} retransmissions")
        try:
            submit_log(self.peer_id, self.experiment_id, log, stats)
        except grpc.RpcError as error:
            print("Could not submit the log:", rpcClient.describe_error(error))


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
