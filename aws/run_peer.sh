#!/bin/bash
# Sobe um peer numa instância EC2.
#
# Uso:  ./aws/run_peer.sh <ip-publico-do-servidor>
#
# - cria o ambiente virtual .venv e instala requirements.txt, se necessário;
# - descobre o IP público da instância (metadados da EC2, IMDSv2) e o divulga
#   ao Group Manager como PEER_ADVERTISED_HOST;
# - aponta GROUPMNGR_ADDR e SERVER_ADDR para o servidor informado.
#
# Variáveis já exportadas têm prioridade (ex.: PEER_ADVERTISED_HOST,
# PEER_UDP_PORT, PEER_RPC_PORT, RPC_CALL_TIMEOUT).
set -euo pipefail
cd "$(dirname "$0")/.."

SERVER_IP="${1:-}"
if [ -z "$SERVER_IP" ]; then
    echo "Uso: $0 <ip-publico-do-servidor>" >&2
    exit 1
fi

if [ ! -x .venv/bin/python ]; then
    echo "Criando o ambiente virtual..."
    if ! python3 -m venv .venv; then
        echo "Falhou. Instale o venv com: sudo apt install -y python3-venv" >&2
        exit 1
    fi
    .venv/bin/pip install -q -r requirements.txt
fi

public_ip_from_metadata() {
    local token
    token=$(curl -sf -m 2 -X PUT "http://169.254.169.254/latest/api/token" \
        -H "X-aws-ec2-metadata-token-ttl-seconds: 60") || return 1
    curl -sf -m 2 -H "X-aws-ec2-metadata-token: $token" \
        "http://169.254.169.254/latest/meta-data/public-ipv4"
}

if [ -z "${PEER_ADVERTISED_HOST:-}" ]; then
    PEER_ADVERTISED_HOST=$(public_ip_from_metadata || true)
fi
if [ -z "${PEER_ADVERTISED_HOST:-}" ]; then
    echo "Não foi possível descobrir o IP público da instância." >&2
    echo "Ela tem IP público? Defina manualmente: PEER_ADVERTISED_HOST=<ip> $0 $SERVER_IP" >&2
    exit 1
fi

export PEER_ADVERTISED_HOST
export GROUPMNGR_ADDR="$SERVER_IP"
export SERVER_ADDR="$SERVER_IP"

echo "Peer divulgado como: $PEER_ADVERTISED_HOST" \
     "(UDP ${PEER_UDP_PORT:-6789}, gRPC ${PEER_RPC_PORT:-5679})"
echo "Servidor (Group Manager e Comparison Server): $SERVER_IP"
exec .venv/bin/python -u peerCommunicatorUDP.py
