// 1 Ingredientes mais comuns em receitas com avaliações mais altas:

MATCH (r:Receita)-[:TEM_AVALIACAO]->(a:Avaliacao)
WHERE a.nota > 4
WITH r, count(a) as AvaliacoesAltas
MATCH (r)-[:CONTEM]->(i:Ingrediente)
RETURN i.nome, count(AvaliacoesAltas) as Frequencia
ORDER BY Frequencia DESC
LIMIT 10

// 2 Receitas frequentemente revisadas juntas pelos usuários:

MATCH (r1:Receita)-[rel:AVALIADA_JUNTO_COM]-(r2:Receita)
WITH r1, r2, count(rel) AS Frequencia
ORDER BY Frequencia DESC
LIMIT 10
RETURN r1, r2, Frequencia

// 4 Receitas com maior número de ingredientes únicos:

MATCH (r:Receita)-[:CONTEM]->(i:Ingrediente)
RETURN r.nome, count(i) AS NumeroDeIngredientes
ORDER BY NumeroDeIngredientes DESC
LIMIT 10

// 6 Receita com avaliações extremamente variadas:

MATCH (r:Receita)-[:TEM_AVALIACAO]->(a:Avaliacao)
WITH r, avg(a.nota) AS Media, stDev(a.nota) AS Desvio
RETURN r.nome, Media, Desvio
ORDER BY Desvio DESC
LIMIT 10

// 7 A receita mais central na rede, baseada na frequência de ingredientes compartilhados com outras receitas:

CALL gds.graph.drop('receitasGraph') // Se precisar deletar

CALL gds.graph.project(
    'receitasGraph',   // Nome do gráfico projetado
    ['Receita'], // Tipos de nós
    {
        CONTEM: {       // Tipo do relacionamento
            type: 'CONTEM',
            orientation: 'UNDIRECTED' // Relacionamento não direcionado
        }
    }
)

CALL gds.betweenness.stream('receitasGraph')
YIELD nodeId, score
RETURN gds.util.asNode(nodeId) AS node, score
ORDER BY score DESC
LIMIT 5


// 9 Influência de um ingrediente específico na avaliação das receitas

MATCH (r:Receita)-[:CONTEM]->(i:Ingrediente {nome: 'olive oil'})
MATCH (r)-[:TEM_AVALIACAO]->(a:Avaliacao)
WITH r, avg(a.nota) AS MediaNota
ORDER BY MediaNota DESC
LIMIT 10
RETURN r.nome AS Receita, MediaNota


// 10 Ingredientes usados predominantemente em receitas com avaliações extremamente positivas ou negativas:

MATCH (i:Ingrediente)<-[:CONTEM]-(r:Receita)-[:TEM_AVALIACAO]->(a:Avaliacao)
WITH i.nome AS Ingrediente, avg(a.nota) AS MediaNota
WHERE MediaNota > 4 OR MediaNota < 2
RETURN Ingrediente, MediaNota
ORDER BY MediaNota DESC
