# Resultado da execução do experimento original

Data da execução: 18 de novembro de 2026.

## Objetivo

Verificar se o código fornecido pelo professor pode ser executado no ambiente
local e registrar o comportamento observado antes das alterações da Parte 1 do
trabalho.

## Ambiente utilizado

- macOS
- Python 3.13.1
- Execução dos três processos em uma única máquina
- Código do commit `8cccdef` (`add content from professor repo`)

## Tentativa com a configuração original

A primeira tentativa utilizou os arquivos sem alterações.

O comando do peer falhou inicialmente porque o módulo `requests` não estava
instalado:

```text
ModuleNotFoundError: No module named 'requests'
```

O projeto não possui um arquivo `requirements.txt` que declare essa
dependência. Para prosseguir com o teste, `requests` foi instalado somente em
uma pasta temporária, sem alteração do ambiente Python do projeto.

O `GroupMngr.py` pôde ser iniciado localmente e ficou aguardando conexões na
porta TCP 5680. Entretanto, o peer e o Comparison Server não tentaram acessar
esse processo local. Os dois utilizaram o endereço fixo definido em
`constMP.py`:

```text
GROUPMNGR_ADDR = '34.203.80.210'
```

O peer permaneceu bloqueado na conexão:

```text
Connecting to group manager: ('34.203.80.210', 5680)
TimeoutError: [Errno 60] Operation timed out
```

O `comparisonServer.py`, depois de receber o valor `1`, também permaneceu
bloqueado em `connect()` para o mesmo endereço. A execução precisou ser
interrompida manualmente. Os sockets não possuem timeout próprio nem tratamento
de indisponibilidade do servidor.

Portanto, o experimento original não é reproduzível diretamente porque depende
da antiga infraestrutura configurada no endereço `34.203.80.210`.

## Execução local controlada

Para separar problemas de configuração de problemas no fluxo do programa, foi
criada uma cópia temporária fora do repositório. Nessa cópia foram realizadas
somente estas adaptações:

- `SERVER_ADDR` e `GROUPMNGR_ADDR` definidos como `127.0.0.1`;
- `N` definido como `1`;
- retorno de `get_public_ip()` definido como `127.0.0.1`.

Nenhuma dessas adaptações foi aplicada aos arquivos versionados.

Os processos foram iniciados nesta ordem:

1. `GroupMngr.py`;
2. `peerCommunicatorUDP.py`;
3. `comparisonServer.py`.

No Comparison Server foi solicitado o envio de três mensagens. O fluxo
completo foi concluído:

```text
List of Peers: ['127.0.0.1']
Peer process 0 started.
Now, wait for the message logs from the communicating peers...
Received log from peer
Found 0 unordered message rounds
```

O peer enviou e recebeu as mensagens `0`, `1` e `2`. O log produzido foi:

```text
[(0, 0), (0, 1), (0, 2)]
```

Em seguida, o valor `0` foi informado ao Comparison Server. O peer, o
Comparison Server e o Group Manager encerraram normalmente.

## Interpretação do resultado

O teste local confirma que o fluxo básico funciona quando todos os endereços
apontam para processos acessíveis: registro, descoberta, comando de início,
handshake, envio UDP, coleta do log e encerramento.

O resultado `0 unordered message rounds` não comprova ordenação ou
confiabilidade. Com apenas um peer, não existem ordens concorrentes para
comparar, e a comunicação UDP ocorreu pela interface de loopback da própria
máquina.

Não foi possível executar vários peers localmente sem modificar a arquitetura.
Cada instância tenta abrir as mesmas portas em `0.0.0.0` (`5679` para o comando
de início e `6789` para UDP). O código original pressupõe que cada peer execute
em uma máquina ou VM com endereço próprio.

## Problemas observados

1. Dependência `requests` não declarada.
2. Endereços da antiga infraestrutura AWS fixos em `constMP.py`.
3. Operações de rede sem timeout ou tratamento de falha.
4. Configuração não permite iniciar vários peers na mesma máquina sem usar
   endereços/portas distintos ou contêineres.
5. Um teste com um único peer não demonstra diferenças de ordenação.
6. O protocolo UDP ainda não possui ACK, retransmissão ou descarte controlado
   de duplicatas.

## Conclusão

O código-base não executa diretamente com a configuração presente no
repositório. Após adaptações mínimas e temporárias de endereço, o cenário de um
peer conclui, indicando que o fluxo principal está funcional. Para reproduzir o
experimento pretendido pelo professor, será necessário configurar várias VMs
ou tornar endereços e portas configuráveis para permitir vários peers locais.
