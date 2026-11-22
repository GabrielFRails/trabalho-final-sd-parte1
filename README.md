# Trabalho Final de Sistemas Distribuídos — Parte 1

Implementação baseada no projeto MPComm fornecido pelo professor. O sistema
executa um experimento no qual vários peers enviam mensagens UDP entre si e um
coordenador compara a ordem observada por cada processo.

## Integrantes

- Gabriel Freitas — 202003564
- Wevton Santana - 202103785
- Gabriel Soares - 202301444

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
especificação da Parte 1, agora com uma camada de confiabilidade própria (ACK,
retransmissão e descarte de duplicatas), descrita em
[Tratamento de perda de mensagens](#tratamento-de-perda-de-mensagens-parte-1b).
A ordenação FIFO é a segunda metade da Parte 1b e ainda está pendente.

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
| `reliableUDP.py` | multicast UDP confiável: ACK, retransmissão e descarte de duplicatas |
| `run_local.sh` | roda um experimento completo localmente com um só comando |
| `test_mpcomm.py` | testes automáticos da Parte 1b (`python3 -m unittest -v`) |
| `aws/run_peer.sh` | sobe um peer numa instância EC2 (descobre o IP público) |

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

### Execução com um comando

`run_local.sh` sobe o Group Manager e os peers em segundo plano, executa as
rodadas no Comparison Server e encerra tudo. A saída de cada processo fica num
diretório temporário, informado no fim.

```bash
./run_local.sh                           # 3 peers, 20 mensagens, 1 rodada
./run_local.sh 5 50 3                    # 5 peers, 50 mensagens, 3 rodadas
UDP_LOSS_RATE=0.2 ./run_local.sh 3 50    # 20% de perda simulada
```

As variáveis de ambiente valem para todos os peers. Para dar configurações
diferentes a cada peer (por exemplo, só um peer com perda), use os cinco
terminais e defina a variável apenas no terminal desse peer.

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
| `PEER_SEND_MIN_MS` / `PEER_SEND_MAX_MS` | `10` / `100` | pausa aleatória entre mensagens enviadas; `0`/`0` envia em rajada |
| `UDP_LOSS_RATE` | `0` | probabilidade (0 a 1) de descartar cada datagrama enviado |
| `UDP_RETRANSMIT_TIMEOUT` | `0.1` | espera pelo ACK antes da 1ª retransmissão, em segundos (entre regiões da AWS, use `0.3`) |
| `UDP_MAX_RETRIES` | `30` | retransmissões antes de desistir de um pacote; `0` = UDP puro |
| `UDP_RCVBUF` | `0` | buffer de recepção UDP em bytes; `0` = padrão do sistema |

As variáveis `PEER_*` e `UDP_*` são lidas por cada peer.

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

## Tratamento de perda de mensagens (Parte 1b)

O UDP não garante entrega: um datagrama pode se perder, chegar repetido ou
fora de ordem. No código original, qualquer perda era fatal: sem um `READY`,
o peer nunca terminava o handshake; sem um aviso de fim, nunca terminava a
rodada. Mensagens comuns perdidas simplesmente sumiam do log.

A camada em [`reliableUDP.py`](reliableUDP.py) resolve a perda mantendo o uso
de sockets UDP.

### Formato dos pacotes

Cada datagrama carrega um objeto JSON (o `pickle` foi removido também do UDP,
pois ele executa código ao desserializar dados vindos da rede):

```json
{"t": "DATA", "exp": 3, "src": 1, "seq": 7, "dst": 2}
{"t": "ACK",  "exp": 3, "src": 2, "seq": 7, "dst": 2, "ack_t": "DATA"}
```

| Campo | Significado |
|---|---|
| `t` | tipo: `READY` (handshake), `DATA` (mensagem), `STOP` (fim dos envios) ou `ACK` |
| `exp` | rodada (`experiment_id`); pacotes de outra rodada são descartados |
| `src` | id de quem enviou o datagrama |
| `seq` | número de sequência; no `STOP`, a quantidade de mensagens enviadas |
| `dst` | posição do destinatário na lista de quem enviou; o ACK devolve o mesmo valor |

### Protocolo

1. **ACK:** quem recebe um `READY`, `DATA` ou `STOP` responde com um `ACK`
   para o endereço de origem. Cada peer usa um único socket para enviar e
   receber, então esse endereço é a porta UDP do remetente.
2. **Retransmissão com backoff:** quem envia guarda o pacote até receber o
   ACK. Uma thread reenvia o que passou do prazo (`UDP_RETRANSMIT_TIMEOUT`),
   dobrando o prazo a cada tentativa, até 0,5 s. O
   backoff evita que, num buffer cheio, as retransmissões piorem o
   congestionamento. Depois de `UDP_MAX_RETRIES` tentativas o pacote é
   abandonado e contado em `gave_up`.
3. **Descarte de duplicatas:** se o ACK se perde, o remetente reenvia um
   pacote que já chegou. O receptor responde o ACK de novo, mas não entrega a
   mensagem outra vez. Retransmitir garante entrega **pelo menos uma vez**; com
   o descarte de duplicatas, cada mensagem é entregue **exatamente uma vez**.
4. **Handshake e fim confiáveis:** `READY` e `STOP` usam o mesmo mecanismo. O
   `STOP` informa quantas mensagens o remetente enviou. Assim o receptor sabe
   quantas esperar, mesmo que o `STOP` chegue antes de uma retransmissão.
5. **Término:** um peer entrega o log ao Comparison Server quando recebeu o
   `STOP` e todas as mensagens anunciadas de cada peer, e quando todos
   confirmaram tudo o que ele enviou. Em seguida ele ainda responde ACKs por
   2 segundos, como o `TIME_WAIT` do TCP: outro peer pode não ter recebido o
   último ACK e estar retransmitindo.

### Decisões de projeto

- **ACK positivo, e não NACK.** Cada destinatário confirma cada pacote. É o
  esquema básico de multicast confiável (livro-texto, Cap. 8, comunicação de
  grupo confiável). O custo é a **implosão de ACKs**: com *N* peers, cada
  multicast gera *N* ACKs, ou *N²* por rodada de mensagens. A alternativa
  escalável é o NACK: o receptor só se manifesta quando percebe uma lacuna na
  sequência. Mas ela exige detectar a lacuna, guardar histórico e tratar a
  perda da última mensagem. Para um grupo pequeno, o ACK positivo é mais
  simples e suficiente.
- **Retransmissão só para quem não confirmou.** Cada destinatário tem seu
  próprio pacote pendente, então o reenvio é unicast. Os outros peers não
  recebem a mesma mensagem de novo.
- **Confiabilidade separada da ordenação.** `reliableUDP.py` garante que cada
  mensagem chegue exatamente uma vez, mas entrega na ordem de chegada. A
  ordenação fica na camada acima (`MessageHandler`), como no modelo de
  *hold-back queue*. Assim a FIFO pode ser feita sem mexer no transporte, e as
  Partes 3 e 4 podem trocar a política de entrega pela ordem total.
- **Simulação de perda no envio.** O descarte vale para todos os datagramas,
  inclusive ACKs e retransmissões. Perder um ACK é o caso que gera duplicatas,
  e precisa ser testado.

### Por que é difícil perder mensagens no teste local

Pela interface de loopback o datagrama não passa por uma rede real: só se
perde quando o **buffer de recepção** do socket enche. No experimento, cada
peer espera de 10 a 100 ms entre mensagens, enquanto o receptor processa cada
uma em bem menos de 1 ms. O buffer nunca enche, e por isso nenhuma mensagem
se perdia (nem na AWS, onde a rede entre regiões é estável e o tráfego é
pequeno).

Há três formas de provocar perda:

| Forma | Como | Observação |
|---|---|---|
| Simulada | `UDP_LOSS_RATE=0.2` | portátil e controlável; descarta também ACKs e retransmissões |
| Real, por estouro de buffer | `PEER_SEND_MIN_MS=0 PEER_SEND_MAX_MS=0 UDP_RCVBUF=4096` | rajada de envios num buffer pequeno; o kernel descarta o excesso |
| Real, pela rede (Linux) | `sudo tc qdisc add dev lo root netem loss 10%` | afeta tudo em `lo`; remover com `sudo tc qdisc del dev lo root` |

### Saída do Comparison Server

Para cada peer, o Comparison Server mostra quantas mensagens recebeu, quantas
faltaram, quantas vieram repetidas e quantas violaram a ordem FIFO (mensagem
entregue depois de outra posterior do mesmo remetente). Em seguida vem a
métrica original de ordem global e a soma dos contadores UDP dos peers:

```
peer received missing duplicates FIFO violations
   0      150       0          0              28
   1      150       0          0              27
   2      150       0          0              20
Expected 150 messages per peer. Totals: 0 missing, 0 duplicates, 75 FIFO violations
Found 144 unordered message positions (total order)
UDP totals: data_sent=450, retransmissions=257, simulated_drops=257, acks_sent=586, acks_received=468, duplicates=118, gave_up=0
```

Os contadores se conferem entre si. Cada pacote confiável recebe exatamente
um ACK (`acks_received` = DATA + READY + STOP = 450 + 9 + 9), cada datagrama
descartado gera uma retransmissão, e cada ACK perdido gera uma duplicata no
receptor (`acks_sent` = 468 + `duplicates`).

## Limitações atuais

- **Ordenação FIFO (segunda metade da Parte 1b):** ainda não implementada.
  Uma mensagem retransmitida é entregue quando chega, depois de mensagens
  posteriores do mesmo remetente. Isso aparece como `FIFO violations` na
  saída. O ponto de implementação está marcado em `MessageHandler._on_data`,
  em [`peerCommunicatorUDP.py`](peerCommunicatorUDP.py).
- **Ordem total:** mesmo com FIFO, peers diferentes podem intercalar de forma
  diferente mensagens de remetentes diferentes. Isso é esperado (FIFO só
  ordena mensagens de um mesmo remetente) e é o assunto da Parte 3.
- Um peer que deixa de responder por completo faz os outros desistirem de seus
  pacotes após `UDP_MAX_RETRIES` e a rodada é abortada pelo coordenador.

## Validação realizada

### Testes automáticos

```bash
python3 -m unittest -v
```

Os 8 testes usam sockets UDP reais no loopback, sem gRPC nem outros
processos, e levam cerca de 3 segundos:

| Teste | Verifica |
|---|---|
| `DecodeTest` | datagramas inválidos (lixo, `pickle`, campos faltando) são descartados |
| `test_no_loss` | sem perda: tudo chega, nenhuma retransmissão, um ACK por mensagem |
| `test_loss_is_recovered_exactly_once` | 30% de perda: as 200 mensagens chegam uma única vez, com retransmissões e duplicatas descartadas |
| `test_without_retransmission_messages_are_lost` | sem retransmissão, a mesma perda faz mensagens sumirem |
| `test_duplicate_is_acked_but_not_delivered` | a duplicata recebe ACK de novo, mas não é entregue |
| `FifoMetricTest` | a contagem de violações FIFO do Comparison Server |
| `FifoDeliveryTest` | **meta da ordenação FIFO**, marcado como falha esperada até ela ser implementada |

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

### Perda de mensagens (Parte 1b)

Executado localmente com `run_local.sh`, Linux, Python 3.14:

| Cenário | Peers × msgs | Recebidas por peer | Faltando | Duplicadas no log | Retransmissões | Violações FIFO |
|---|---:|---:|---:|---:|---:|---:|
| sem perda | 3 × 20 (2 rodadas) | 60 de 60 | 0 | 0 | 0 | 0 |
| 5% de perda simulada | 3 × 50 | 150 de 150 | 0 | 0 | 45 | 19 |
| 20% de perda simulada | 3 × 50 | 150 de 150 | 0 | 0 | 257 | 75 |
| 40% de perda simulada | 3 × 50 | 150 de 150 | 0 | 0 | 740 | 136 |
| 60% de perda simulada | 5 × 20 | 100 de 100 | 0 | 0 | 2792 | 225 |
| 30% de perda, 4 rodadas seguidas | 3 × 20 | 60 de 60 em todas | 0 | 0 | 195 a 239 | 40 a 44 |
| perda real (rajada, buffer de 4 KB) | 3 × 300 | 900 de 900 | 0 | 0 | 1551 | 868 |

Para comparação, sem retransmissão (`UDP_MAX_RETRIES=0`):

| Cenário | Resultado |
|---|---|
| 20% de perda simulada, 3 × 50 | handshakes perdidos; dois peers nem começaram a enviar; cerca de 40 de 150 mensagens entregues; rodada abortada por timeout |
| perda real (rajada, buffer de 4 KB), 3 × 300 | 487 a 544 de 900 mensagens entregues; rodada abortada por timeout |

Outros cenários verificados:

- peer congelado (`kill -STOP`) no meio de uma rodada com 20% de perda: a
  rodada é abortada; depois do `kill -CONT`, a rodada seguinte entrega 60 de 60
  mensagens e o comando `0` encerra todos os processos;
- com 60% de perda, 2 pacotes foram abandonados (`gave_up`), mas nenhuma
  mensagem faltou: eram ACKs para um peer que já tinha recebido tudo e
  encerrado.

As violações FIFO confirmam o que a segunda metade da Parte 1b precisa
resolver: a mensagem perdida é recuperada, mas chega depois das seguintes.

### Execução na AWS (duas regiões)

Configuração:

- 7 peers: 5 em N. Virginia (`us-east-1`) e 2 em Oregon (`us-west-2`), um por
  instância EC2 (Ubuntu 24.04), iniciados com `aws/run_peer.sh`;
- Group Manager e Comparison Server numa instância separada em N. Virginia;
- comunicação pelos IPs públicos, com TCP 5678-5680 e UDP 6789-6790 liberados
  nos Security Groups das duas regiões.

| Mensagens por peer | Mensagens recebidas por peer | Posições fora de ordem |
|---:|---:|---:|
| 5 | 35 de 35 | 33 de 35 (94%) |
| 10 | 70 de 70 | 64 de 70 (91%) |
| 30 | 210 de 210 | 208 de 210 (99%) |
| 50 | 350 de 350 | 339 de 350 (97%) |

Nenhuma mensagem foi perdida nessas rodadas, mas quase todas as posições
diferem entre os peers. A latência entre regiões (cerca de 70 ms, contra menos
de 1 ms dentro de uma região) faz cada peer observar uma intercalação diferente
das mensagens. É o problema de escalabilidade geográfica que o MPComm
demonstra.

A métrica compara os logs posição por posição: uma única mensagem adiantada
desloca todas as posições seguintes daquele peer. Por isso ela indica que as
ordens divergem, e não quantas mensagens trocaram de lugar.

O relatório da tentativa de executar o código original está em
[`docs/resultado_execucao_experimento.md`](docs/resultado_execucao_experimento.md).
