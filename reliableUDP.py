"""Multicast confiável sobre UDP: ACK, retransmissão e descarte de duplicatas.

Formato dos pacotes (JSON, um por datagrama):

    {"t": "DATA",  "exp": 3, "src": 1, "seq": 7, "dst": 2}
    {"t": "ACK",   "exp": 3, "src": 2, "seq": 7, "dst": 2, "ack_t": "DATA"}

- ``t``: tipo. READY (handshake), DATA (mensagem do experimento) e STOP
  (fim dos envios; ``seq`` = quantidade de DATA enviadas) são confiáveis.
  ACK confirma um deles e não é confirmado (senão haveria ACK de ACK).
- ``exp``: rodada (experiment_id). Pacotes de rodadas antigas são ignorados.
- ``src``: id de quem enviou o datagrama.
- ``seq``: número de sequência do remetente dentro do tipo.
- ``dst``: índice do destinatário na lista de peers *de quem enviou*. O ACK
  devolve o mesmo valor, então o remetente sabe qual envio foi confirmado sem
  depender de o destinatário conhecer a própria posição nessa lista.

Quem envia guarda cada pacote confiável em ``_pending`` até receber o ACK.
Uma thread reenvia o que passou do prazo, dobrando o prazo a cada tentativa
(backoff exponencial), e desiste depois de ``max_retries`` reenvios.

Quem recebe sempre responde com ACK, inclusive para pacotes repetidos: a
repetição indica que o ACK anterior se perdeu. Mas só entrega o pacote à
aplicação na primeira vez. Assim, a garantia "pelo menos uma vez" da
retransmissão, somada ao descarte de duplicatas, resulta em cada mensagem
entregue exatamente uma vez.

Este módulo não ordena as mensagens: um DATA retransmitido pode ser entregue
depois de DATAs posteriores do mesmo remetente. A ordenação FIFO fica na
aplicação (``MessageHandler`` em peerCommunicatorUDP.py).
"""

import json
import random
import threading
import time
from dataclasses import dataclass, replace

DATA = "DATA"
READY = "READY"
STOP = "STOP"
ACK = "ACK"
RELIABLE_TYPES = (DATA, READY, STOP)

# Intervalo com que a thread de retransmissão verifica os prazos, em segundos.
_TICK = 0.01
# Limite do backoff exponencial: o prazo dobra a cada reenvio até este valor.
MAX_RETRANSMIT_TIMEOUT = 0.5


def encode(packet):
    return json.dumps(packet, separators=(",", ":")).encode("utf-8")


def decode(raw):
    """Pacote decodificado, ou None se o datagrama for inválido."""
    try:
        packet = json.loads(raw.decode("utf-8"))
        if packet["t"] not in RELIABLE_TYPES + (ACK,):
            return None
        for field in ("exp", "src", "seq", "dst"):
            if not isinstance(packet[field], int):
                return None
        if packet["t"] == ACK and packet.get("ack_t") not in RELIABLE_TYPES:
            return None
        return packet
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


@dataclass
class ChannelStats:
    data_sent: int = 0
    retransmissions: int = 0
    simulated_drops: int = 0
    acks_sent: int = 0
    acks_received: int = 0
    duplicates: int = 0
    gave_up: int = 0


class _Pending:
    """Pacote enviado que ainda espera ACK."""

    __slots__ = ("raw", "addr", "deadline", "timeout", "retries")

    def __init__(self, raw, addr, timeout):
        self.raw = raw
        self.addr = addr
        self.timeout = timeout
        self.deadline = time.monotonic() + timeout
        self.retries = 0


class ReliableChannel:
    """Camada confiável sobre um socket UDP já vinculado (bind).

    O mesmo socket envia e recebe. Por isso o ACK pode ser devolvido para o
    endereço de origem do datagrama, que é a porta UDP do remetente.
    """

    def __init__(self, sock, my_id, experiment_id, peers, *, loss_rate,
                 retransmit_timeout, max_retries):
        self.sock = sock
        self.my_id = my_id
        self.experiment_id = experiment_id
        self.addresses = [(p["host"], p["udp_port"]) for p in peers]
        self.loss_rate = loss_rate
        self.retransmit_timeout = retransmit_timeout
        self.max_retries = max_retries

        self._lock = threading.Lock()
        self._idle = threading.Condition(self._lock)  # sinaliza _pending vazio
        self._pending = {}  # (dst, tipo, seq) -> _Pending
        self._seen = set()  # (tipo, src, seq) já entregues à aplicação
        self._stats = ChannelStats()
        self._random = random.Random()
        self._closed = threading.Event()
        self._thread = threading.Thread(
            target=self._retransmit_loop,
            name=f"retransmitter-{my_id}",
            daemon=True,
        )

    # --- ciclo de vida -----------------------------------------------------

    def start(self):
        self._thread.start()

    def close(self):
        self._closed.set()
        with self._idle:
            self._idle.notify_all()
        if self._thread.is_alive():
            self._thread.join(timeout=1)

    def stats(self):
        with self._lock:
            return replace(self._stats)

    # --- envio -------------------------------------------------------------

    def multicast(self, packet_type, seq):
        """Envia um pacote confiável a todos os peers (inclusive a si)."""
        for dst in range(len(self.addresses)):
            self._send_reliable(dst, packet_type, seq)

    def _send_reliable(self, dst, packet_type, seq):
        packet = {"t": packet_type, "exp": self.experiment_id,
                  "src": self.my_id, "seq": seq, "dst": dst}
        pending = _Pending(encode(packet), self.addresses[dst],
                           self.retransmit_timeout)
        with self._lock:
            self._pending[(dst, packet_type, seq)] = pending
            if packet_type == DATA:
                self._stats.data_sent += 1
        self._transmit(pending.raw, pending.addr)

    def _transmit(self, raw, addr):
        if self.loss_rate > 0 and self._random.random() < self.loss_rate:
            with self._lock:
                self._stats.simulated_drops += 1
            return
        try:
            self.sock.sendto(raw, addr)
        except OSError:
            pass  # socket fechado por um abort, ou rede indisponível

    def wait_until_acked(self, timeout):
        """True quando não resta pacote sem ACK (confirmado ou abandonado)."""
        with self._idle:
            return self._idle.wait_for(
                lambda: not self._pending or self._closed.is_set(), timeout
            )

    def _retransmit_loop(self):
        while not self._closed.wait(_TICK):
            now = time.monotonic()
            due = []
            with self._lock:
                for key, pending in list(self._pending.items()):
                    if pending.deadline > now:
                        continue
                    if pending.retries >= self.max_retries:
                        del self._pending[key]
                        self._stats.gave_up += 1
                        continue
                    pending.retries += 1
                    pending.timeout = min(pending.timeout * 2,
                                          MAX_RETRANSMIT_TIMEOUT)
                    pending.deadline = now + pending.timeout
                    self._stats.retransmissions += 1
                    due.append(pending)
                if not self._pending:
                    self._idle.notify_all()
            for pending in due:
                self._transmit(pending.raw, pending.addr)

    # --- recepção ----------------------------------------------------------

    def receive(self, packet, addr):
        """Trata um pacote recebido desta rodada.

        Devolve o pacote se ele deve ser entregue à aplicação, ou None se era
        um ACK ou uma duplicata.
        """
        if packet["t"] == ACK:
            self._handle_ack(packet)
            return None

        self._send_ack(packet, addr)
        key = (packet["t"], packet["src"], packet["seq"])
        with self._lock:
            if key in self._seen:
                self._stats.duplicates += 1
                return None
            self._seen.add(key)
        return packet

    def _send_ack(self, packet, addr):
        ack = {"t": ACK, "exp": self.experiment_id, "src": self.my_id,
               "seq": packet["seq"], "dst": packet["dst"],
               "ack_t": packet["t"]}
        with self._lock:
            self._stats.acks_sent += 1
        self._transmit(encode(ack), addr)

    def _handle_ack(self, packet):
        key = (packet["dst"], packet["ack_t"], packet["seq"])
        with self._lock:
            self._stats.acks_received += 1
            self._pending.pop(key, None)
            if not self._pending:
                self._idle.notify_all()
