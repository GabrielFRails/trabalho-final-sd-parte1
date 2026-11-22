#!/bin/sh
# Roda um experimento completo localmente, sem precisar abrir 5 terminais.
#
# Uso:  ./run_local.sh [peers] [mensagens] [rodadas]
#       (padrão: 3 peers, 20 mensagens por peer, 1 rodada)
#
# Sobe o Group Manager e os peers em segundo plano, executa as rodadas no
# Comparison Server e encerra tudo. A saída de cada processo fica em OUT_DIR
# (padrão: um diretório temporário, mostrado no fim).
#
# As variáveis de ambiente valem para todos os peers. Exemplos:
#   UDP_LOSS_RATE=0.2 ./run_local.sh                        # 20% de perda
#   UDP_LOSS_RATE=0.2 UDP_MAX_RETRIES=0 ./run_local.sh      # sem retransmissão
#   PEER_SEND_MIN_MS=0 PEER_SEND_MAX_MS=0 UDP_RCVBUF=4096 ./run_local.sh 3 300
#                                    # rajada + buffer pequeno: perda real
set -eu
ROOT=$(cd "$(dirname "$0")" && pwd)
N_PEERS=${1:-3}
N_MSGS=${2:-20}
ROUNDS=${3:-1}
OUT_DIR=${OUT_DIR:-$(mktemp -d)}
mkdir -p "$OUT_DIR"

if [ -x "$ROOT/.venv/bin/python" ]; then PY="$ROOT/.venv/bin/python"; else PY=python3; fi

PIDS=""
cleanup() { [ -n "$PIDS" ] && kill $PIDS 2>/dev/null || true; }
trap cleanup EXIT INT TERM

cd "$OUT_DIR"   # os peers gravam logfile<ID>.log no diretório atual
"$PY" -u "$ROOT/GroupMngr.py" > groupmngr.out 2>&1 &
PIDS="$PIDS $!"
sleep 1

i=0
while [ "$i" -lt "$N_PEERS" ]; do
    PEER_UDP_PORT=$((6789 + i)) PEER_RPC_PORT=$((5700 + i)) \
        "$PY" -u "$ROOT/peerCommunicatorUDP.py" > "peer$i.out" 2>&1 &
    PIDS="$PIDS $!"
    i=$((i + 1))
done
sleep 2

# Uma linha com N_MSGS por rodada e, no fim, 0 para encerrar tudo.
r=0
{
    while [ "$r" -lt "$ROUNDS" ]; do echo "$N_MSGS"; r=$((r + 1)); done
    echo 0
} | "$PY" -u "$ROOT/comparisonServer.py" 2>&1 | tee comparison.out \
    | sed 's/^Enter the number of messages.*=> //' \
    | grep -v "^Peer process\|^List of peers\|^experiment aborted\|^$"

# Dá alguns segundos para os processos encerrarem sozinhos (comando 0); o que
# sobrar (por exemplo, um peer travado) é finalizado pelo trap de saída.
for _ in 1 2 3 4 5 6 7 8 9 10; do
    alive=""
    for pid in $PIDS; do kill -0 "$pid" 2>/dev/null && alive=1; done
    [ -z "$alive" ] && break
    sleep 0.5
done
echo "Saída de cada processo em: $OUT_DIR"
