// Throughout these scripts, we will focus on animals arriving to slaughterhouses in 2018
// as this is the year with most records, and with less variability within months
----
// Identify which movements arrived to a slaughterhouse in 2018
// and which animal movements could have ended in a slaughterhouse in 2018, even if they
// happened in previous years

//Create a relation farm -> slaughterhouse based on animals sent from 2018
MATCH (f:Farm)-[st:SENDS_TO]->(s:Slaughterhouse)
WHERE st.total_2018 > 0
MERGE (s)-[cf:COMES_FROM_2018 {total: st.total_2018}]->(f)

// Mark animal groups that have ages and dates compatible for having been sent to a slaughterhouse in 2018
// assuming that animals should be younger than 42 months old in 2018
CALL apoc.periodic.iterate("
    MATCH (ag:Animal_Group)-[:GOES_TO]->(:Farm)
    WHERE ag.transport_year = 2018
    OR duration.inMonths(date(ag.transport_date), date('2018-12-31')).months + ag.lower_age < 42
    RETURN ag
","SET ag.possible_2018_source = True", {batchSize:50000, parallel:true})

//Create a relation farm1 -> farm2 based on animals compatible for having been sent to a slaughterhouse in 2018
MATCH (f1:Farm)<-[:COMES_FROM]-(ag:Animal_Group)-[:GOES_TO]->(f2:Farm)
WHERE ag.possible_2018_source = True
WITH f1, SUM(ag.amount) AS total_animals, f2
MERGE (f2)-[cf:COMES_FROM_2018_COMPATIBLE {total: total_animals}]->(f1)


// Creation of (s)-[:COMES_FROM_2018]->(f) weights
// Add the path_weight to existing :COMES_FROM_2018 relationships
// Using as weight:  1/(toFloat(cf.total)+1)
CALL apoc.periodic.iterate("
MATCH (:Slaughterhouse)-[cf:COMES_FROM_2018]->(:Farm)
RETURN cf",
"SET cf.path_weight = 1/(toFloat(cf.total)+1)", {batchSize:30000, parallel:true, concurrency:4})

// Creation of (f1)-[:COMES_FROM_2018_COMPATIBLE]->(f2) weights
// Add the path_weight to existing :COMES_FROM_2018_COMPATIBLE relationships
// Using as weight:  1/(toFloat(cf.total)+1)
CALL apoc.periodic.iterate("
MATCH (:Farm)-[cf:COMES_FROM_2018_COMPATIBLE]->(:Farm)
RETURN cf",
"SET cf.path_weight = 1/(toFloat(cf.total)+1)", {batchSize:30000, parallel:true, concurrency:4})


// Creation of base projection
CALL gds.graph.project(
  'Slaughterouses_Farms_totals_2018',
  ['Farm', 'Slaughterhouse'],
  ['COMES_FROM_2018', 'COMES_FROM_2018_COMPATIBLE'],
  {
    relationshipProperties: ['total', 'path_weight']
  }
)

-------
// Creation of the base projection using the relationships just created
CALL gds.graph.project(
  'Slaughterouses_Farms_totals_2018',
  ['Farm', 'Slaughterhouse'],
  ['COMES_FROM_2018', 'COMES_FROM_2018_COMPATIBLE'],
  {
    relationshipProperties: ['total', 'path_weight']
  }
)


// Creation for shortest paths between slaughterhouses and farms
// Using stream so to filter for paths < 10 of length (~40M relationships)
// (if its not filtered it will try to create 500M relationships and can take >100GB disk space)
// Note this script takes approx 4 hours to run in a 4cpu 16GB RAM ubuntu
CALL apoc.periodic.iterate("
  MATCH (s:Slaughterhouse)
  RETURN ID(s) AS sourceNodeId",
  "CALL gds.allShortestPaths.delta.stream('Slaughterouses_Farms_totals_2018', {
    sourceNode: sourceNodeId,
    relationshipWeightProperty: 'path_weight'
    })
    YIELD sourceNode, targetNode, totalCost, nodeIds, costs
    WITH gds.util.asNode(sourceNode) AS source, gds.util.asNode(targetNode) AS target, totalCost, nodeIds, costs
    WHERE size(nodeIds) <= 10
    MERGE (source)-[:SHORTEST_PATH {totalCost: totalCost, nodeIds: nodeIds, costs: costs}]->(target)"
,
  {
    batchSize: 1, parallel: false
  }
);
