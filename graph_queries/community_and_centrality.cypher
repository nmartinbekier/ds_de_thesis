// Simple projection. Using it for community detection and centrality
CALL gds.graph.project(
  'simpleFarmSlaughterouses',
  ['Farm', 'Slaughterhouse'],
  'SENDS_TO',
  {
    relationshipProperties: ['total_sent', 'total_2020', 'total_2019', 'total_2018' ]
  }
)

// Run Louvain community detection algorithm and writes the community id's into the nodes
CALL gds.louvain.write('simpleFarmSlaughterouses', { 
    writeProperty: 'community_louvain', relationshipWeightProperty: 'total_sent' })
YIELD communityCount, modularity, modularities

// Run PageRank centrality algorithm and show first 100 results. Only showing slaughterhouses
CALL gds.pageRank.stream('simpleFarmSlaughterouses', {
  relationshipWeightProperty: 'total_sent'
})
YIELD nodeId, score
WITH nodeId, score, labels(gds.util.asNode(nodeId))[0] AS node_type
WHERE node_type = 'Slaughterhouse'
RETURN gds.util.asNode(nodeId).name AS name, gds.util.asNode(nodeId).tax_number AS tax_number, score 
ORDER BY score DESC, name ASC
LIMIT 100

// Run PageRank centrality algorithm and show first 100 results.
CALL gds.pageRank.stream('simpleFarmSlaughterouses', {
  relationshipWeightProperty: 'total_sent',
  scaler: "L1Norm"
})
YIELD nodeId, score
WITH nodeId, score, labels(gds.util.asNode(nodeId))[0] AS node_type
RETURN gds.util.asNode(nodeId).name AS name, gds.util.asNode(nodeId).tax_number AS tax_number, score 
ORDER BY score DESC, name ASC
LIMIT 100

----

// Sample queries

// Returns the community id and amount of nodes of communities with more than 20 nodes: 45 communities
// lowest of this set has 21 nodes, largest has 38.735 nodes
MATCH (n:Farm|Slaughterhouse)
WITH DISTINCT(n.community_louvain) AS louvain_id, COUNT(n) as node_amount
WHERE node_amount > 20
RETURN louvain_id, node_amount
ORDER BY node_amount DESC

// Number of farms and slaughterhouses in the community JBS Maraba is part of: 14.537 nodes
MATCH (n:Farm|Slaughterhouse)
WHERE n.community_louvain = 5849
RETURN COUNT(DISTINCT(n))

// Run PageRank centrality algorithm and show first 5 results of Farms
CALL gds.pageRank.stream('simpleFarmSlaughterouses', {
  relationshipWeightProperty: 'total_sent',
  scaler: "L1Norm"
})
YIELD nodeId, score
WITH nodeId, score, labels(gds.util.asNode(nodeId))[0] AS node_type
WHERE node_type = "Farm"
RETURN gds.util.asNode(nodeId).name AS name, gds.util.asNode(nodeId).tax_number AS tax_number, score 
ORDER BY score DESC, name ASC
LIMIT 5

// Run PageRank centrality algorithm and show first 100 results of Farms
CALL gds.pageRank.stream('simpleFarmSlaughterouses', {
  relationshipWeightProperty: 'total_sent',
  scaler: "L1Norm"
})
YIELD nodeId, score
WITH nodeId, score, labels(gds.util.asNode(nodeId))[0] AS node_type
WHERE node_type = "Farm"
RETURN gds.util.asNode(nodeId).name AS name, gds.util.asNode(nodeId).tax_number AS tax_number, score 
ORDER BY score DESC, name ASC
LIMIT 100

// Return 95th percentile of Farms
CALL gds.pageRank.stream('simpleFarmSlaughterouses', {
  relationshipWeightProperty: 'total_sent',
  scaler: "L1Norm"
})
YIELD nodeId, score
WITH nodeId, score, labels(gds.util.asNode(nodeId))[0] AS node_type
WHERE node_type = "Farm"
WITH score as farm_score
RETURN percentileCont(farm_score, 0.95)



