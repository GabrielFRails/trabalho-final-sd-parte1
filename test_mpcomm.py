"""Testes automáticos da Parte 1b.  Rodar com:  python3 -m unittest -v

Usam sockets UDP reais na interface de loopback, sem gRPC nem outros
processos, e terminam em poucos segundos.
"""

import contextlib
import io
import pickle
import threading
import unittest
from socket import AF_INET, SOCK_DGRAM, socket

import reliableUDP
from comparisonServer import fifo_violations
from peerCommunicatorUDP import MessageHandler
from reliableUDP import DATA, STOP, ReliableChannel


class Endpoint:
    """Um socket UDP com um ReliableChannel e uma thread que recebe."""

    def __init__(self, my_id, peers, loss_rate=0.0, max_retries=30, seed=0):
        self.sock = socket(AF_INET, SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.settimeout(0.05)
        self.peers = peers
        self.loss_rate = loss_rate
        self.max_retries = max_retries
        self.my_id = my_id
        self.seed = seed
        self.delivered = []
        self._stop = threading.Event()

    @property
    def address(self):
        host, port = self.sock.getsockname()
        return {"host": host, "udp_port": port}

    def start(self):
        self.channel = ReliableChannel(
            self.sock, self.my_id, 1, self.peers,
            loss_rate=self.loss_rate, retransmit_timeout=0.02,
            max_retries=self.max_retries,
        )
        self.channel._random.seed(self.seed)  # perdas reproduzíveis
        self.channel.start()
        self._thread = threading.Thread(target=self._receive, daemon=True)
        self._thread.start()

    def _receive(self):
        while not self._stop.is_set():
            try:
                raw, addr = self.sock.recvfrom(65535)
            except TimeoutError:
                continue
            except OSError:
                return
            packet = reliableUDP.decode(raw)
            if packet is not None:
                packet = self.channel.receive(packet, addr)
                if packet is not None:
                    self.delivered.append((packet["src"], packet["seq"]))

    def close(self):
        self._stop.set()
        self._thread.join()
        self.channel.close()
        self.sock.close()


def run_transfer(n_messages, loss_rate, max_retries=30):
    """Envia n_messages DATA de A para B e devolve (A, B)."""
    receiver = Endpoint(1, [], loss_rate, max_retries, seed=1)
    sender = Endpoint(0, [receiver.address], loss_rate, max_retries, seed=2)
    receiver.start()
    sender.start()
    for seq in range(n_messages):
        sender.channel.multicast(DATA, seq)
    sender.channel.wait_until_acked(timeout=20)
    sender.close()
    receiver.close()
    return sender, receiver


class DecodeTest(unittest.TestCase):
    def test_rejects_invalid_datagrams(self):
        invalid = [
            b"lixo",
            b"\xff\xfe",
            b"{}",
            pickle.dumps(("READY", 1)),  # formato antigo
            b'{"t":"DATA","exp":1,"src":"x","seq":0,"dst":0}',  # src não é int
            b'{"t":"ACK","exp":1,"src":0,"seq":0,"dst":0}',  # ACK sem ack_t
            b'{"t":"OUTRO","exp":1,"src":0,"seq":0,"dst":0}',
        ]
        for raw in invalid:
            with self.subTest(raw=raw):
                self.assertIsNone(reliableUDP.decode(raw))

    def test_round_trip(self):
        packet = {"t": DATA, "exp": 1, "src": 0, "seq": 3, "dst": 2}
        self.assertEqual(reliableUDP.decode(reliableUDP.encode(packet)), packet)


class ReliableChannelTest(unittest.TestCase):
    def test_no_loss(self):
        sender, receiver = run_transfer(100, loss_rate=0.0)
        stats = sender.channel.stats()
        self.assertEqual(sorted(receiver.delivered), [(0, n) for n in range(100)])
        self.assertEqual(stats.retransmissions, 0)
        self.assertEqual(stats.acks_received, 100)

    def test_loss_is_recovered_exactly_once(self):
        sender, receiver = run_transfer(200, loss_rate=0.3)
        sender_stats = sender.channel.stats()
        receiver_stats = receiver.channel.stats()
        # Todas chegaram, cada uma uma única vez...
        self.assertEqual(sorted(receiver.delivered), [(0, n) for n in range(200)])
        # ...porque houve retransmissões e duplicatas descartadas.
        self.assertGreater(sender_stats.retransmissions, 0)
        self.assertGreater(receiver_stats.duplicates, 0)
        self.assertEqual(sender_stats.gave_up, 0)

    def test_without_retransmission_messages_are_lost(self):
        sender, receiver = run_transfer(200, loss_rate=0.3, max_retries=0)
        self.assertLess(len(receiver.delivered), 200)
        self.assertEqual(sender.channel.stats().retransmissions, 0)

    def test_duplicate_is_acked_but_not_delivered(self):
        receiver = Endpoint(1, [])
        receiver.start()
        packet = {"t": DATA, "exp": 1, "src": 0, "seq": 0, "dst": 0}
        address = ("127.0.0.1", receiver.address["udp_port"])
        self.assertIsNotNone(receiver.channel.receive(dict(packet), address))
        self.assertIsNone(receiver.channel.receive(dict(packet), address))
        stats = receiver.channel.stats()
        receiver.close()
        self.assertEqual(stats.acks_sent, 2)  # o ACK repetido é necessário
        self.assertEqual(stats.duplicates, 1)


class FifoMetricTest(unittest.TestCase):
    def test_fifo_violations(self):
        self.assertEqual(fifo_violations([(0, 0), (1, 0), (0, 1), (1, 1)]), 0)
        self.assertEqual(fifo_violations([(0, 1), (0, 0), (0, 3), (0, 2)]), 2)
        self.assertEqual(fifo_violations([(0, 2), (0, 0), (0, 1)]), 2)


def new_handler(expected_peers=1):
    return MessageHandler(None, None, peer_id=0, experiment_id=1,
                          expected_peers=expected_peers,
                          stop_receiving=threading.Event())


def feed(handler, packets):
    """Entrega pacotes (tipo, remetente, número) ao handler, sem imprimir."""
    with contextlib.redirect_stdout(io.StringIO()):
        for kind, src, seq in packets:
            handler._handle({"t": kind, "src": src, "seq": seq})


class FifoDeliveryTest(unittest.TestCase):
    """Ordenação FIFO: a fila de espera do MessageHandler."""

    def test_out_of_order_arrivals_are_delivered_in_fifo_order(self):
        handler = new_handler()
        feed(handler, [(DATA, 0, seq) for seq in [2, 0, 3, 1, 4]]
             + [(STOP, 0, 5)])
        self.assertEqual(handler.log, [(0, n) for n in range(5)])
        self.assertEqual(handler.held_count, 2)  # o 2 e o 3 esperaram
        self.assertTrue(handler.delivery_complete.is_set())

    def test_senders_are_independent(self):
        # O 0 do remetente 0 está atrasado, mas isso não segura o remetente 1:
        # FIFO só ordena mensagens de um mesmo remetente.
        handler = new_handler(expected_peers=2)
        feed(handler, [(DATA, 0, 1), (DATA, 1, 0), (DATA, 1, 1), (DATA, 0, 0)])
        self.assertEqual(handler.log, [(1, 0), (1, 1), (0, 0), (0, 1)])

    def test_round_ends_only_after_the_gap_is_filled(self):
        handler = new_handler()
        feed(handler, [(DATA, 0, 1), (DATA, 0, 2), (STOP, 0, 3)])
        self.assertEqual(handler.log, [])  # tudo esperando pelo 0
        self.assertFalse(handler.delivery_complete.is_set())
        feed(handler, [(DATA, 0, 0)])  # chega a retransmissão
        self.assertEqual(handler.log, [(0, 0), (0, 1), (0, 2)])
        self.assertTrue(handler.delivery_complete.is_set())

    def test_fifo_over_real_lossy_transfer(self):
        # Ordem de chegada real produzida pelo canal com 30% de perda.
        _, receiver = run_transfer(200, loss_rate=0.3)
        arrivals = receiver.delivered
        self.assertGreater(fifo_violations(arrivals), 0)  # chegou fora de ordem
        handler = new_handler()
        feed(handler, [(DATA, src, seq) for src, seq in arrivals]
             + [(STOP, 0, 200)])
        self.assertEqual(handler.log, [(0, n) for n in range(200)])
        self.assertEqual(fifo_violations(handler.log), 0)
        self.assertTrue(handler.delivery_complete.is_set())


if __name__ == "__main__":
    unittest.main()
