O ponto de partida para este trabalho é o sistema de comunicação multicast apresentado no início do curso para demonstrar o problema da escalabilidade geográfica com respeito à (falta de) ordenação de mensagens. O código pode ser encontrado neste repositório: https://github.com/professorfabio/MPComm.git (use como template ou faça uma cópia integral em um repositório seu). 

O trabalho será dividido em partes, cada uma entregue como uma tarefa separada:

Parte 1: Realizar melhorias nos protocolos de comunicação.

a) Substituir sockets por RPC nas interações cliente-servidor e comparar em termos de vantagens e desvantagens.

b) Implementar tratamento para perda de mensagens e ordenação FIFO na comunicação multicast (mantendo o uso de sockets).

Parte 2: Criar uma aplicação para o sistema. Atualmente, ele só faz a troca de mensagens sem um significado específico. Nesta parte do trabalho, deve ser dado significado às mensagens, bem como aos processos envolvidos, como parte de uma aplicação com finalidade específica. Por exemplo, os processos podem manter uma base de dados replicada e as mensagens conduzem comandos que fazem alterações e consultas aos dados da base de dados replicada. Pense em um exemplo específico de aplicação que dependa de comunicação multicast e para a qual a ordem correta das mensagens seja um critério crítico. Obs.: como parte desta tarefa, você também deve fazer melhorias no código-base fornecido (por exemplo, para melhorar a arquitetura do sistema).

Parte 3: Projetar e implementar um mecanismo de coordenação baseado em ordenação total de mensagens (ver Cap. 5) e verificar a eficácia para garantir a consistência entre as réplicas da base dados.

Parte 4: Projetar e implementar um mecanismo de coordenação mais leve (que envolva menos trocas de mensagens) e que atinja o objetivo de garantir a consistência entre as réplicas. Ver Cap. 7.

OBSERVAÇÕES:

O trabalho deverá ser realizado em grupo de 3 alunos
Cada parte deverá ser entregue separadamente.
Entrega via GitHub.
Entregar: código; readme explicativo da aplicação, incluindo a descrição das funcionalidades, arquitetura e detalhes de implementação; e vídeo com demonstração (com a participação de todos os integrantes do grupo).
Última atualização: segunda-feira, 28 set. 2026, 11:36