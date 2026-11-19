# Trabalho Final de Sistemas Distribuídos — Parte 1

Implementação baseada no projeto MPComm fornecido pelo professor. O sistema
executa um experimento no qual vários peers enviam mensagens UDP entre si e um
coordenador compara a ordem observada por cada processo.

## Integrantes

- Gabriel Freitas — 202003564

## Arquitetura

O sistema possui três tipos de processo:

- **Group Manager:** mantém o cadastro dos peers;
- **Comparison Server:** inicia o experimento, recebe os logs e compara a ordem;
- **Peer:** envia e recebe mensagens UDP e registra a ordem de recebimento.

As interações cliente-servidor usam XML-RPC:

| Servidor RPC | Métodos principais | Clientes |
|---|---|---|
| Group Manager | `register`, `list_peers`, `unregister`, `stop` | peers e Comparison Server |
| Peer | `prepare_experiment`, `start_experiment` | Comparison Server |
| Comparison Server | `submit_log` | peers |

A comunicação de dados entre peers continua usando sockets UDP, conforme a
especificação da Parte 1. O tratamento de perdas e a ordenação FIFO serão
implementados separadamente.

## Requisitos

- Python 3.10 ou superior;
- nenhuma dependência externa.

## Execução local com três peers

Abra cinco terminais na raiz do repositório.

1. Inicie o Group Manager:

   ```bash
   python3 GroupMngr.py
   ```

2. Inicie o primeiro peer:

   ```bash
   PEER_UDP_PORT=6789 PEER_RPC_PORT=5679 python3 peerCommunicatorUDP.py
   ```

3. Inicie o segundo peer:

   ```bash
   PEER_UDP_PORT=6790 PEER_RPC_PORT=5681 python3 peerCommunicatorUDP.py
   ```

4. Inicie o terceiro peer:

   ```bash
   PEER_UDP_PORT=6791 PEER_RPC_PORT=5682 python3 peerCommunicatorUDP.py
   ```

5. Inicie o Comparison Server:

   ```bash
   python3 comparisonServer.py
   ```

Informe um número maior que zero para executar o experimento. Depois da
comparação, informe `0` para encerrar os peers e o Group Manager.

Cada peer grava também um arquivo `logfile<ID>.log` com as mensagens recebidas.

O início ocorre em duas fases RPC. Primeiro, `prepare_experiment` abre o
receptor UDP de todos os peers; depois, `start_experiment` libera os envios.
Isso evita que os primeiros handshakes sejam enviados antes de todos os
destinatários estarem prontos.

## Configuração

Os valores de `constMP.py` podem ser sobrescritos por variáveis de ambiente:

| Variável | Padrão | Finalidade |
|---|---:|---|
| `PEER_ADVERTISED_HOST` | `127.0.0.1` | endereço divulgado pelo peer |
| `PEER_BIND_ADDR` | `0.0.0.0` | interface local do peer |
| `PEER_UDP_PORT` | `6789` | porta das mensagens UDP |
| `PEER_RPC_PORT` | `5679` | porta RPC do peer |
| `SERVER_ADDR` | `127.0.0.1` | endereço do Comparison Server |
| `SERVER_RPC_PORT` | `5678` | porta RPC do Comparison Server |
| `GROUPMNGR_ADDR` | `127.0.0.1` | endereço do Group Manager |
| `GROUPMNGR_RPC_PORT` | `5680` | porta RPC do Group Manager |
| `RPC_LOG_WAIT_TIMEOUT` | `60` | espera máxima pelos logs, em segundos |

Em máquinas diferentes, configure `PEER_ADVERTISED_HOST`, `SERVER_ADDR` e
`GROUPMNGR_ADDR` com endereços alcançáveis pelos demais processos.

## RPC em comparação com sockets TCP manuais

### Vantagens

- as operações são expressas como métodos (`register`, `start_experiment` e
  `submit_log`) em vez de mensagens sem contrato explícito;
- serialização, requisição e resposta são tratadas pela biblioteca;
- elimina o uso de `pickle` nas interações cliente-servidor;
- reduz código repetido de criação, conexão, envio e fechamento de sockets;
- erros remotos são apresentados ao cliente como falhas da chamada RPC.

### Desvantagens

- XML-RPC adiciona overhead de HTTP e XML;
- oferece um conjunto limitado de tipos de dados;
- uma chamada remota pode falhar ou demorar, apesar de parecer uma chamada
  local;
- a implementação padrão não fornece autenticação nem criptografia;
- cliente e servidor continuam precisando concordar sobre nomes, parâmetros e
  resultados dos métodos.

## Limitações atuais

O UDP ainda não possui confirmação, retransmissão ou ordenação FIFO. Portanto,
mensagens e handshakes podem ser perdidos. Essa é a próxima etapa da Parte 1.

## Validação realizada

A implementação foi executada localmente com três peers em duas rodadas:

- cinco mensagens por peer: cada log recebeu as 15 mensagens esperadas;
- três mensagens por peer: cada log recebeu as 9 mensagens esperadas;
- os logs das duas rodadas chegaram ao Comparison Server via RPC;
- o comando `0` encerrou os três peers e o Group Manager via RPC.

No teste local, os peers observaram a mesma ordem. Isso não substitui um teste
com atraso ou perda de rede, que será necessário na etapa UDP/FIFO.

O relatório da tentativa de executar o código original está em
[`docs/resultado_execucao_experimento.md`](docs/resultado_execucao_experimento.md).
