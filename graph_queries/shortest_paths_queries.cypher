// Review farms that have a 2 or more degrees away from the nearest slaughterhouse
// Note that the list of nodeIds (and costs) starts by the source's id
// consequently, the path length is 1 below its list size
MATCH (f:Farm)
WITH f
CALL {
    WITH f
    MATCH (s:Slaughterhouse)-[sp:SHORTEST_PATH]->(f)
    RETURN sp
    ORDER BY SIZE(sp.nodeIds) ASC
    LIMIT 1
}
WITH sp, f
WHERE SIZE(sp.nodeIds) >= 3
RETURN ID(f) AS NodeId, f.name AS farm_name,
    f.tax_number AS tax_number, SIZE(sp.nodeIds) - 1 AS path_length,
    sp.totalCost
LIMIT 10

// Find number of reachable slaughterhouses for a specific farm: 728 slaughterhouses
MATCH p=(f:Farm)<-[sh:SHORTEST_PATH]-(s:Slaughterhouse)
WHERE ID(f) = 14521
RETURN COUNT(DISTINCT(s)) AS reachable_slaughterhouses

// Explore the 5 slaughterhouses with the most transited path to farm ID:14521
// The shortest path, with cost 0.01286 and 3 degrees away, is JBS Maraba
// The next one has cost 0.01329 and 4 degrees away, is MERCURIO ALIMENTOS S.A
MATCH (s:Slaughterhouse)-[sh:SHORTEST_PATH]->(f:Farm)
WHERE ID(f) = 14521
RETURN ID(s) AS nodeId, s.name AS slaughterhouse_name,
    sh.totalCost AS total_cost, SIZE(sh.nodeIds) - 1 AS path_length
ORDER BY sh.totalCost ASC
LIMIT 5

// Show graph of a the path described by a shortest path
MATCH (s:Slaughterhouse)-[sh:SHORTEST_PATH]->(f:Farm)
WHERE ID(s) = 20369
AND ID(f) = 14521 
WITH sh.nodeIds AS node_list, s, sh
MATCH (s)-[cf1:COMES_FROM_2018]->(f1), (f2)-[cf2:COMES_FROM_2018_COMPATIBLE]->(f3)
WHERE ID(f1) IN node_list
AND ID(f2) IN node_list
AND ID(f3) IN node_list
RETURN s, cf1, f1, cf2, f2, f3, sh

// Show graph of a the path described by a shortest path
MATCH (s:Slaughterhouse)-[sh:SHORTEST_PATH]->(f:Farm)
WHERE ID(s) = 20369
AND ID(f) = 14560 
WITH sh.nodeIds AS node_list, s, sh
MATCH (s)-[cf1:COMES_FROM_2018]->(f1), (f2)-[cf2:COMES_FROM_2018_COMPATIBLE]->(f3)
WHERE ID(f1) IN node_list
AND ID(f2) IN node_list
AND ID(f3) IN node_list
RETURN s, cf1, f1, cf2, f2, f3, sh

MATCH (s:Slaughterhouse)-[sh:SHORTEST_PATH]->(f:Farm)
WHERE ID(f) = 14560
RETURN ID(s) AS nodeId, s.name AS slaughterhouse_name,
    sh.totalCost AS total_cost, SIZE(sh.nodeIds) - 1 AS path_length
ORDER BY sh.totalCost ASC
LIMIT 5