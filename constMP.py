"""Shared configuration for the MPComm experiment.

Every setting can be overridden with an environment variable. The defaults
preserve the ports used by the original project, while the addresses default
to localhost so the project does not depend on the professor's old VM.
"""

import os


def _env_int(name, default):
    return int(os.getenv(name, str(default)))


def _env_float(name, default):
    return float(os.getenv(name, str(default)))


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

# --- Multicast UDP confiável (Parte 1b) -----------------------------------
# Estes valores são lidos por cada peer, então podem ser diferentes em cada
# terminal (por exemplo, só um peer com enlace ruim).

# Pausa aleatória entre duas mensagens enviadas, em milissegundos.
# Com 0 e 0 o peer envia em rajada, o que pode estourar o buffer do receptor.
PEER_SEND_MIN_MS = _env_float("PEER_SEND_MIN_MS", 10)
PEER_SEND_MAX_MS = _env_float("PEER_SEND_MAX_MS", 100)
# Probabilidade (0 a 1) de descartar cada datagrama enviado, inclusive ACKs e
# retransmissões. Simula um canal com perda; 0 desativa.
UDP_LOSS_RATE = _env_float("UDP_LOSS_RATE", 0.0)
# Tempo de espera pelo ACK antes da primeira retransmissão, em segundos. A
# cada nova tentativa ele dobra (backoff exponencial), até 0,5 s. Entre
# regiões distantes (RTT de dezenas de ms), 0.3 evita reenvios desnecessários.
UDP_RETRANSMIT_TIMEOUT = _env_float("UDP_RETRANSMIT_TIMEOUT", 0.1)
# Retransmissões de um mesmo pacote antes de desistir dele. 0 desativa a
# retransmissão (comportamento do UDP puro).
UDP_MAX_RETRIES = _env_int("UDP_MAX_RETRIES", 30)
# Tamanho do buffer de recepção do socket UDP, em bytes; 0 usa o padrão do SO.
UDP_RCVBUF = _env_int("UDP_RCVBUF", 0)

