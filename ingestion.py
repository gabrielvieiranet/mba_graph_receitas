import os

import pandas as pd
from py2neo import Graph, Node

# NEO4J_HOST será fornecido pelo Docker, caso contrário, localhost

HOST = os.environ.get("NEO4J_HOST", "localhost")
PORTA = 7687
USUARIO = "neo4j"
SENHA = "neo4j"  # padrão

grafo = Graph("bolt://" + HOST + ":7687", auth=(USUARIO, SENHA))


def main():

    # CRIACAO DE NOS

    criarNosReceita()
    criarNosAvaliacao()
    criarNosUsuario()
    criarNosIngrediente()

    # CRIACAO DE INDICES (PERFORMANCE)

    criarIndices()

    # CRIACAO DE RELACIONAMENTOS

    criarRelReceitaIngrediente()
    criarRelReceitaAvaliacao()
    criarRelAvaliacaoUsuario()
    criarRelReceitaReceita()


def criarIndices():
    # Criar índices para os nós
    indices = {
        "Receita": "receitaId",
        "Ingrediente": "ingredienteId",
        "Avaliacao": "avaliacaoId",
        "Usuario": "usuarioId"
    }

    for no, propriedade in indices.items():
        query = f"CREATE INDEX {no.lower()}_{propriedade}_index IF NOT EXISTS FOR (n:{no}) ON (n.{propriedade})"
        grafo.run(query)


def criarNosReceita():
    # Lendo o arquivo CSV
    df = pd.read_csv('dataset/sot_receitas.csv')

    # Dividindo os dados em lotes
    BATCH_SIZE = 1000  # Ajuste este número conforme necessário
    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]

        # Criando uma lista de dicionários para cada linha no lote
        receitas = [{'receitaId': row['receitaId'], 'nome': row['nome']}
                    for index, row in batch.iterrows()]

        # Inserindo o lote no Neo4j
        grafo.run("UNWIND $batch AS receita "
                  "CREATE (r:Receita {receitaId: receita.receitaId, nome: receita.nome})", batch=receitas)


def criarNosAvaliacao():
    df = pd.read_csv('dataset/sot_avaliacoes.csv')
    BATCH_SIZE = 1000

    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]
        avaliacoes = [{'avaliacaoId': row['avaliacaoId'], 'data': row['data'],
                       'nota': row['nota'], 'comentario': row['comentario']}
                      for index, row in batch.iterrows()]
        grafo.run("UNWIND $batch AS avaliacao "
                  "CREATE (a:Avaliacao {avaliacaoId: avaliacao.avaliacaoId, data: avaliacao.data, "
                  "nota: avaliacao.nota, comentario: avaliacao.comentario})", batch=avaliacoes)


def criarNosUsuario():
    # Substitua com o caminho correto para o seu arquivo de usuários
    df = pd.read_csv('dataset/sot_usuarios.csv')
    BATCH_SIZE = 1000

    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]
        usuarios = [{'usuarioId': row['usuarioId'], 'nome': row['nome']}
                    for index, row in batch.iterrows()]
        grafo.run("UNWIND $batch AS usuario "
                  "CREATE (u:Usuario {usuarioId: usuario.usuarioId, nome: usuario.nome})", batch=usuarios)


def criarNosIngrediente():
    df = pd.read_csv('dataset/sot_ingredientes.csv')
    BATCH_SIZE = 1000

    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]
        ingredientes = [{'ingredienteId': row['ingredienteId'], 'nome': row['nome']}
                        for index, row in batch.iterrows()]
        grafo.run("UNWIND $batch AS ingrediente "
                  "CREATE (i:Ingrediente {ingredienteId: ingrediente.ingredienteId, nome: ingrediente.nome})", batch=ingredientes)


def criarRelReceitaIngrediente():
    df = pd.read_csv('dataset/rel_receita_ingrediente.csv')
    BATCH_SIZE = 1000  # Ajuste o tamanho do lote conforme necessário

    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]
        relacoes = [{'receitaId': int(row['receitaId']), 'ingredienteId': int(row['ingredienteId'])}
                    for index, row in batch.iterrows()]

        grafo.run("UNWIND $batch AS rel "
                  "MATCH (r:Receita {receitaId: rel.receitaId}), "
                  "(i:Ingrediente {ingredienteId: rel.ingredienteId}) "
                  "CREATE (r)-[:CONTEM]->(i)", batch=relacoes)


def criarRelReceitaAvaliacao():
    df = pd.read_csv('dataset/rel_receita_avaliacao.csv')
    BATCH_SIZE = 1000

    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]
        relacoes = [{'receitaId': int(row['receitaId']), 'avaliacaoId': row['avaliacaoId']}
                    for index, row in batch.iterrows()]

        grafo.run("UNWIND $batch AS rel "
                  "MATCH (r:Receita {receitaId: rel.receitaId}), "
                  "(a:Avaliacao {avaliacaoId: rel.avaliacaoId}) "
                  "CREATE (r)-[:TEM_AVALIACAO]->(a)", batch=relacoes)


def criarRelAvaliacaoUsuario():
    df = pd.read_csv('dataset/rel_avaliacao_usuario.csv')
    BATCH_SIZE = 1000

    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]
        relacoes = [{'avaliacaoId': row['avaliacaoId'], 'usuarioId': int(row['usuarioId'])}
                    for index, row in batch.iterrows()]

        grafo.run("UNWIND $batch AS rel "
                  "MATCH (a:Avaliacao {avaliacaoId: rel.avaliacaoId}), "
                  "(u:Usuario {usuarioId: rel.usuarioId}) "
                  "CREATE (a)-[:FEITA_POR]->(u)", batch=relacoes)


def criarRelReceitaReceita():
    # Ajuste o nome do arquivo conforme necessário
    df = pd.read_csv('dataset/rel_avaliada_junto_com.csv')
    BATCH_SIZE = 1000

    for start in range(0, len(df), BATCH_SIZE):
        end = start + BATCH_SIZE
        batch = df.iloc[start:end]
        relacoes = [{'receitaId1': int(row['receitaId1']), 'receitaId2': int(row['receitaId2'])}
                    for index, row in batch.iterrows()]

        grafo.run("UNWIND $batch AS rel "
                  "MATCH (r1:Receita {receitaId: rel.receitaId1}), "
                  "(r2:Receita {receitaId: rel.receitaId2}) "
                  "CREATE (r1)-[:AVALIADA_JUNTO_COM]->(r2)", batch=relacoes)


if __name__ == '__main__':
    main()
