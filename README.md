# Desafio Técnico - Cientista de Dados Pleno (Especialista em GenAI)

## Descrição

Bem-vindo(a) ao desafio técnico para a vaga de Pessoa Cientista de Dados Pleno no nosso time de transformação digital, focado em criar soluções inovadoras para a cidade do Rio de Janeiro!

### Objetivo

O objetivo deste desafio é avaliar suas habilidades no desenho e desenvolvimento de soluções baseadas em IA Generativa. Você irá projetar e construir um agente autônomo capaz de interagir com uma base de dados da prefeitura, transformando perguntas em linguagem natural em insights acionáveis.

Avaliaremos sua capacidade de:
- Projetar uma arquitetura de agente de IA.
- Orquestrar tarefas complexas utilizando o framework **LangGraph**.
- Gerar e executar consultas SQL de forma segura e eficiente.
- Integrar LLMs (Large Language Models) com fontes de dados estruturadas (BigQuery).
- Escrever código limpo, bem documentado e robusto.

#### Observação

É esperado que você possa não ter tido contato prévio com todas as tecnologias solicitadas (como LangGraph, por exemplo), e isso é intencional. Parte da avaliação consiste em verificar sua capacidade de aprender rapidamente e aplicar novos conceitos. Por essa razão, o desafio tem uma duração de 10 dias, permitindo que você tenha tempo para estudar e desenvolver sua solução.

### Conjunto de Dados

O agente deverá consultar dados públicos do projeto `datario` no BigQuery. As tabelas principais para este desafio são:

- **Chamados do 1746:** Dados de chamados de serviços públicos.
  - Caminho: `datario.adm_central_atendimento_1746.chamado`
- **Bairros do Rio de Janeiro:** Catálogo de bairros para enriquecimento dos dados.
  - Caminho: `datario.dados_mestres.bairro`

### Ferramentas e Recursos

- **Linguagem e Framework:** Python e LangGraph.
- **Banco de Dados:** Google BigQuery. Você precisará de uma conta no GCP para consultar os dados.
- **LLM:** Fique à vontade para escolher o modelo de sua preferência (OpenAI, Google, Anthropic, etc.).
- **Bibliotecas Python:** `langchain`, `langgraph`, `google-cloud-bigquery`, `pandas`.

**Recursos Úteis:**
- **Tutorial de Acesso ao BigQuery:** [Como acessar dados no datario.rio](https://docs.dados.rio/tutoriais/como-acessar-dados/)
- **Documentação do LangGraph:** [LangChain Python Documentation](https://python.langchain.com/docs/langgraph)

### Etapas do Desafio

1.  **Configuração:** Siga o tutorial para criar sua conta no GCP e configurar a autenticação para o BigQuery.
2.  **Fork:** Faça um fork deste repositório.
3.  **Desenvolvimento do Agente:** Crie um agente em Python utilizando LangGraph que atenda aos critérios definidos no arquivo `requisitos_do_agente.md`. O agente deve ser capaz de receber uma pergunta em linguagem natural e orquestrar os passos para respondê-la.
4.  **Estrutura do Projeto:** Organize seu código de forma clara. Sugerimos uma estrutura que inclua:
    - Um arquivo principal para a lógica do agente (ex: `agent.py`).
    - Um arquivo `requirements.txt` com as dependências.
    - Um `README.md` detalhado para o seu projeto.
5.  **Documentação:** Atualize o `README.md` do seu repositório explicando a arquitetura da sua solução, como configurá-la (chaves de API, etc.) e como executá-la.
6.  **Entrega:** Faça commits incrementais à medida que avança. Ao finalizar, envie o link do seu repositório no GitHub.

## Avaliação

Sua solução será avaliada com base nos seguintes critérios e pesos:

- **Qualidade do Código e Arquitetura do Agente (peso 3):** Clareza, modularidade, eficiência e a lógica do grafo construído em LangGraph.
- **Robustez e Tratamento de Erros (peso 2):** Como o agente lida com perguntas ambíguas, consultas que falham ou resultados inesperados.
- **Qualidade da Resposta e Eficiência da Consulta (peso 2):** A precisão da resposta final em linguagem natural e a qualidade do SQL gerado (evitar consultas desnecessariamente custosas).
- **Documentação (peso 1):** A clareza das instruções para rodar seu projeto e a explicação da sua solução.

**Dica:** Vá além do básico! Soluções que demonstrarem um raciocínio mais sofisticado, como validar o SQL gerado antes da execução, lidar com perguntas ambíguas pedindo esclarecimentos, ou implementar alguma forma de memória, serão vistas com grande diferencial.

## Dúvidas

Se tiver alguma dúvida, entre em contato pelo email `brunoalmeida@prefeitura.rio`.

Boa sorte! Estamos ansiosos para ver sua solução.

---

**Prefeitura da Cidade do Rio de Janeiro**
