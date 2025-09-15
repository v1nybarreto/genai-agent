## Requisitos Funcionais do Agente

O objetivo é construir um agente que funcione como um analista de dados. Ele deve ser capaz de entender uma pergunta, decidir se precisa consultar o banco de dados, gerar e executar a consulta, e por fim, sintetizar o resultado em uma resposta clara.

Sua implementação em **LangGraph** deve conter um fluxo lógico com, no mínimo, os seguintes passos (nós):

1.  **Roteador de Intenção (Router):**
    -   Ao receber a pergunta do usuário, este nó deve decidir o próximo passo.
    -   **Caminho 1: Pergunta requer dados.** Se a pergunta for sobre os chamados (ex: "quantos...", "qual o bairro com mais..."), o fluxo deve seguir para a geração de SQL.
    -   **Caminho 2: Pergunta conversacional.** Se for uma saudação ou uma pergunta genérica (ex: "olá", "obrigado"), o agente deve responder diretamente sem consultar o banco.

2.  **Gerador de SQL (SQL Generator):**
    -   Recebe a pergunta e deve gerar uma consulta SQL válida e eficiente para o BigQuery.
    -   A consulta deve ser otimizada para performance, evitando `SELECT *` e utilizando agregações sempre que possível.
    -   Deve ser capaz de gerar consultas que envolvam `JOIN` com a tabela de bairros quando a pergunta mencionar nomes de bairros.

3.  **Executor de SQL (SQL Executor):**
    -   Este nó executa a consulta gerada no BigQuery.
    -   Deve tratar possíveis erros de execução (ex: sintaxe SQL inválida, falha na conexão) e retornar o resultado de forma estruturada (ex: DataFrame do Pandas ou lista de dicionários).

4.  **Sintetizador de Resposta (Response Synthesizer):**
    -   Recebe os dados do executor de SQL.
    -   Utiliza um LLM para traduzir os dados brutos em uma resposta final em linguagem natural, clara e objetiva para o usuário.

### Perguntas para Teste

Seu agente final deve ser capaz de responder corretamente às seguintes perguntas. Utilize-as para validar sua implementação.

#### Perguntas de Análise Simples
1.  Quantos chamados foram abertos no dia 28/11/2024?
2.  Qual o subtipo de chamado (`subtipo`) mais comum relacionado a "Iluminação Pública"?

#### Perguntas com JOIN e Agregação
3.  Quais os 3 bairros que mais tiveram chamados abertos sobre "reparo de buraco" em 2023?
4.  Qual o nome da unidade organizacional (`nome_unidade_organizacional`) que mais atendeu chamados de "Fiscalização de estacionamento irregular"?

#### Perguntas Conversacionais
5.  Olá, tudo bem?
6.  Me dê sugestões de brincadeiras para fazer com meu cachorro!

##### Importante: a tabela de Chamados do 1746 possui milhões de linhas. Garanta que seu agente gere consultas que utilizem filtros (especialmente de data) para evitar custos e processamento excessivos no BigQuery.
