# Trabalho Final de Sistemas Distribuídos — Parte 1

Implementação baseada no projeto MPComm fornecido pelo professor. O sistema
executa um experimento no qual vários peers enviam mensagens UDP entre si e um
coordenador compara a ordem observada por cada processo.

## Integrantes

- Gabriel Freitas — 202003564
- Wevton Santana - 202103785

## Arquitetura

O sistema possui três tipos de processo:

- **Group Manager:** mantém o cadastro dos peers;
- **Comparison Server:** inicia o experimento, recebe os logs e compara a ordem;
- **Peer:** envia e recebe mensagens UDP e registra a ordem de recebimento.

As interações cliente-servidor, que no código original usavam sockets TCP com
`pickle`, agora usam **gRPC**. O contrato está em
[`protos/mpcomm.proto`](protos/mpcomm.proto):

| Serviço gRPC | Métodos | Clientes |
|---|---|---|
| `GroupManager` | `Register`, `ListPeers`, `Unregister`, `Stop` | peers e Comparison Server |
| `Peer` | `PrepareExperiment`, `StartExperiment`, `AbortExperiment` | Comparison Server |
| `ComparisonServer` | `SubmitLog` | peers |

A comunicação de dados entre peers continua usando sockets UDP, conforme a
especificação da Parte 1. O tratamento de perdas e a ordenação FIFO serão
implementados separadamente.

### Arquivos

| Arquivo | Conteúdo |
|---|---|
| `protos/mpcomm.proto` | mensagens e serviços gRPC |
| `mpcomm_pb2.py`, `mpcomm_pb2_grpc.py` | código gerado a partir do `.proto` |
| `gen_proto.sh` | regenera o código acima |
| `rpcClient.py` | abertura de canais gRPC e formatação de erros |
| `GroupMngr.py` | servidor `GroupManager` |
| `peerCommunicatorUDP.py` | servidor `Peer` e troca de mensagens UDP |
| `comparisonServer.py` | coordenador e servidor `ComparisonServer` |
| `constMP.py` | configuração (endereços, portas e timeouts) |

## Requisitos

- Python 3.10 ou superior;
- dependências em `requirements.txt` (`grpcio`, `protobuf` e, para regenerar
  os stubs, `grpcio-tools`).

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

O código gerado já está no repositório. Depois de alterar o `.proto`, rode
`./gen_proto.sh` com o ambiente virtual ativado.

## Execução local com três peers

Abra cinco terminais na raiz do repositório e ative o ambiente virtual em cada
um (`source .venv/bin/activate`).

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

O início ocorre em duas fases. Primeiro, `PrepareExperiment` abre o receptor
UDP de todos os peers; depois, `StartExperiment` libera os envios. Isso evita
que os primeiros handshakes sejam enviados antes de todos os destinatários
estarem prontos.

## Configuração

Os valores de `constMP.py` podem ser sobrescritos por variáveis de ambiente:

| Variável | Padrão | Finalidade |
|---|---:|---|
| `PEER_ADVERTISED_HOST` | `127.0.0.1` | endereço divulgado pelo peer |
| `PEER_BIND_ADDR` | `0.0.0.0` | interface local do peer |
| `PEER_UDP_PORT` | `6789` | porta das mensagens UDP |
| `PEER_RPC_PORT` | `5679` | porta gRPC do peer |
| `SERVER_ADDR` | `127.0.0.1` | endereço do Comparison Server |
| `SERVER_RPC_PORT` | `5678` | porta gRPC do Comparison Server |
| `GROUPMNGR_ADDR` | `127.0.0.1` | endereço do Group Manager |
| `GROUPMNGR_RPC_PORT` | `5680` | porta gRPC do Group Manager |
| `RPC_CALL_TIMEOUT` | `10` | prazo (deadline) de cada chamada gRPC, em segundos |
| `RPC_LOG_WAIT_TIMEOUT` | `60` | espera máxima pelos logs, em segundos |

Em máquinas diferentes, configure `PEER_ADVERTISED_HOST`, `SERVER_ADDR` e
`GROUPMNGR_ADDR` com endereços alcançáveis pelos demais processos.

## Tratamento de falhas

- **Deadlines:** toda chamada gRPC tem prazo de `RPC_CALL_TIMEOUT` segundos.
  Um processo travado gera `DEADLINE_EXCEEDED` em vez de bloquear o chamador.
- **Rodada com falha:** se um peer não responde ou um log não chega dentro de
  `RPC_LOG_WAIT_TIMEOUT`, o Comparison Server informa o erro, chama
  `AbortExperiment` em todos os peers e volta ao prompt. Os peers fecham os
  sockets UDP e aceitam uma nova rodada.
- **Identificador de rodada:** cada experimento recebe um `experiment_id`,
  enviado em `PrepareExperiment` e devolvido em `SubmitLog`. Logs atrasados de
  rodadas anteriores são ignorados. Como há um único coordenador, um
  `PrepareExperiment` com id novo descarta a rodada anterior do peer, mesmo que
  ela tenha ficado presa porque o `AbortExperiment` se perdeu.
- **Saída de peers:** ao encerrar (comando `0`, Ctrl+C ou `kill`), o peer chama
  `Unregister` e deixa de aparecer em `ListPeers`. Um peer que morre sem
  encerrar (`kill -9`, queda da máquina) continua cadastrado: a rodada falha e é
  abortada até ele ser reiniciado com as mesmas portas. Não há verificação
  periódica de que os peers estão vivos (heartbeat).
- **Entrada inválida:** o prompt do Comparison Server repete a pergunta para
  valores não numéricos ou negativos.

## gRPC em comparação com sockets TCP manuais

### Vantagens

- **Contrato explícito:** serviços, métodos e tipos ficam definidos no
  `.proto`. Com sockets, o formato das mensagens (`{"op": "register", ...}`)
  existia só implicitamente no código de cada lado.
- **Código gerado:** stubs de cliente e servidor são gerados automaticamente.
  Conexão, envio, recepção, delimitação das mensagens e fechamento deixam de
  ser escritos à mão.
- **Serialização eficiente e segura:** Protocol Buffers é binário, compacto e
  tipado. Substitui o `pickle`, que executa código ao desserializar dados vindos
  da rede.
- **Erros e prazos nativos:** cada chamada tem deadline e devolve um status
  (`UNAVAILABLE`, `DEADLINE_EXCEEDED`, `FAILED_PRECONDITION`...). Os sockets do
  original não tinham timeout, e um `recv(2048)` podia truncar mensagens grandes.
- **Concorrência:** o servidor gRPC atende requisições em paralelo, num pool de
  threads. O servidor original atendia uma conexão por vez.
- **HTTP/2:** multiplexa várias chamadas na mesma conexão e permite streaming,
  se necessário nas próximas partes.

### Desvantagens

- **Dependência externa:** exige `grpcio` e `protobuf`; o original usava só a
  biblioteca padrão.
- **Etapa de geração de código:** cada mudança no `.proto` exige regenerar os
  stubs e manter as versões de `protobuf` compatíveis.
- **Depuração mais difícil:** o tráfego é binário sobre HTTP/2, ilegível sem
  ferramentas específicas.
- **Overhead para mensagens pequenas:** HTTP/2 e os cabeçalhos do gRPC custam
  mais do que um socket TCP simples numa troca curta.
- **Transparência ilusória:** a chamada parece local, mas pode falhar ou
  demorar, e o código precisa tratar `RpcError`.
- **Sem segurança no modo usado:** `insecure_channel` não autentica nem
  criptografa. Para isso seria preciso configurar TLS.

## Limitações atuais

O UDP ainda não possui confirmação, retransmissão ou ordenação FIFO. Portanto,
mensagens e handshakes podem ser perdidos. Essa é a próxima etapa da Parte 1.

## Validação realizada

A versão gRPC foi executada localmente com três peers:

- entradas inválidas (`abc`, `-1`) fazem o prompt repetir a pergunta;
- rodadas de 5 e 3 mensagens por peer: cada log recebeu as 15 e as 9
  mensagens esperadas;
- o comando `0` encerrou os peers (cada um chamou `Unregister`) e o Group
  Manager.

Cenários de falha, com `RPC_CALL_TIMEOUT=3` e `RPC_LOG_WAIT_TIMEOUT=3`:

| Cenário | Resultado |
|---|---|
| peer encerrado com Ctrl+C | sai de `ListPeers`; a rodada seguinte usa os peers restantes |
| peer morto com `kill -9` | `UNAVAILABLE` no `PrepareExperiment`; rodada abortada e o coordenador continua |
| mesmo peer reiniciado | registro sem duplicata; a rodada seguinte funciona |
| peer congelado (`kill -STOP`) no meio da rodada | timeout dos logs; rodada abortada (o abort ao peer congelado expira com `DEADLINE_EXCEEDED`) |
| rodada após o peer voltar (`kill -CONT`) | o peer descarta a rodada presa e a nova rodada funciona |

O relatório da tentativa de executar o código original está em
[`docs/resultado_execucao_experimento.md`](docs/resultado_execucao_experimento.md).
