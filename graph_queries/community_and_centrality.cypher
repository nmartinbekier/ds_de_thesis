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

// Run PageRank centrality algorithm and show first 100 results. Only showing Farms
CALL gds.pageRank.stream('simpleFarmSlaughterouses', {
  relationshipWeightProperty: 'total_sent'
})
YIELD nodeId, score
WITH nodeId, score, labels(gds.util.asNode(nodeId))[0] AS node_type
WHERE node_type = 'Farm'
RETURN gds.util.asNode(nodeId).name AS name, gds.util.asNode(nodeId).tax_number AS tax_number, score 
ORDER BY score DESC, name ASC
LIMIT 100