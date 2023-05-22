// Creation of 'farm aggregations' to facilitate calculations.
// Do these first set of queries per year wanted to be analyzed.

// First degree, 2020
MATCH (s:Slaughterhouse)<-[:GOES_TO]-(ag1:Animal_Group)-[:COMES_FROM]->(f1:Farm)
WHERE s.tax_number='02916265014110'
AND ag1.transport_year = 2020
WITH elementId(f1) AS farm_node_id, SUM(ag1.amount) AS Animals_sent
MERGE (fa:Farm_aggregation {node_id: farm_node_id})
SET fa.first_degree_2020 = Animals_sent

//  Create a Farm_aggregation node per existing farm, to avoid lock errors while merging when running
// iterate in parallel
CALL apoc.periodic.iterate("MATCH (f:Farm) RETURN f",
"MERGE (:Farm_aggregation {node_id: elementId(f)})", {})

// Second degree, 2020
// For each ag1 that is connected to ag2’s of a f2, get the max possible from those ag2’s, and save it 
// to a :Sub_Group node with that value, which will later be aggregated (added). Doing this mainly to be
// able to run the following query in parallel, without using MERGE on Farm_aggregation (it would run into read locks)
CALL apoc.periodic.iterate("
MATCH (s:Slaughterhouse)<-[:GOES_TO]-(ag1:Animal_Group)-[:COULD_COME_FROM]->(:Animal_Group)-[:COMES_FROM]->(:Farm)
WHERE s.tax_number='02916265014110'
AND ag1.transport_year = 2020
WITH COLLECT(DISTINCT(ag1)) AS ag1s
UNWIND ag1s AS ag1_i
RETURN ag1_i",
"CALL {
  WITH ag1_i
  MATCH (ag1_i)-[ccf_i:COULD_COME_FROM]->(ag2_i:Animal_Group)-[:COMES_FROM]->(f2_i:Farm)
  RETURN MAX(ccf_i.possible_animals) AS max_animals_from_ag2, elementId(f2_i) AS farm_node_id
}
MATCH (fa:Farm_aggregation {node_id: farm_node_id})
CREATE (fa)-[:SECOND_DEGREE_AGG {year: 2020}]->(:Sub_Group {max_possible: max_animals_from_ag2})
", {batchSize:200, parallel:true})

// After the previous query, set 2nd degree farm aggregation of the year
// (summing all the max ag2’s)
MATCH (fa:Farm_aggregation)-[:SECOND_DEGREE_AGG {year:2020}]->(:Sub_Group)
WITH COLLECT(DISTINCT(fa)) AS unique_fas
UNWIND unique_fas as fa_i
CALL {
  WITH fa_i
  MATCH (fa_i)-[:SECOND_DEGREE_AGG {year:2020}]->(sg:Sub_Group)
  RETURN fa_i AS fa_ii, SUM(sg.max_possible) AS group_max_possible
}
SET fa_ii.second_degree_2020 = COALESCE(fa_ii.second_degree_2020, 0) + group_max_possible

// Create support graph per year 3rd degree
// For each ag1, take the max :COULD_COME_FROM ag2, and the max :COULD_COME_FROM between ag2 and ag3, coming from a farm f3
// Then, find the minimum between the ag1-ag2 and the ag2-ag3 segments, as that would be an upper bound of the path.
CALL apoc.periodic.iterate("
MATCH
(s:Slaughterhouse)<-[:GOES_TO]-(ag1:Animal_Group)-[:COULD_COME_FROM]->(:Animal_Group)-[COULD_COME_FROM]->(ag3:Animal_Group)-[:COMES_FROM]->(f3:Farm)
WHERE s.tax_number='02916265014110'
AND ag1.transport_year = 2020
WITH COLLECT(DISTINCT(ag1)) AS ag1s
UNWIND ag1s AS ag1_i
RETURN ag1_i",
"CALL {
  WITH ag1_i
  MATCH (ag1_i)-[ccf1_i:COULD_COME_FROM]->(ag2_i:Animal_Group)-[ccf2_i:COULD_COME_FROM]->(ag3_i:Animal_Group)-[:COMES_FROM]->(f3_i:Farm)
WITH [MAX(ccf1_i.possible_animals), MAX(ccf2_i.possible_animals)] AS max_values, f3_i
  UNWIND max_values AS max_values_rows
  RETURN MIN(max_values_rows) AS max_possible_animals, elementId(f3_i) AS farm_node_id
}
MATCH (fa:Farm_aggregation {node_id: farm_node_id})
CREATE (fa)-[:THIRD_DEGREE_AGG {year: 2020}]->(:Sub_Group {max_possible: max_possible_animals})
", {batchSize:200, parallel:true})

// Per farm, add the third degree aggregations
MATCH (fa:Farm_aggregation)-[:THIRD_DEGREE_AGG {year:2020}]->(:Sub_Group)
WITH COLLECT(DISTINCT(fa)) AS unique_fas
UNWIND unique_fas as fa_i
CALL {
  WITH fa_i
  MATCH (fa_i)-[:THIRD_DEGREE_AGG {year:2020}]->(sg:Sub_Group)
  RETURN fa_i AS fa_ii, SUM(sg.max_possible) AS group_max_possible
}
SET fa_ii.third_degree_2020 = COALESCE(fa_ii.third_degree_2020, 0) + group_max_possible

--------
// Create the consolidated CSV (without cities)
WITH "MATCH (fa:Farm_aggregation) WHERE SIZE(keys(fa)) > 1
WITH fa
MATCH (f:Farm)
WHERE elementId(f) = fa.node_id
WITH f, fa
RETURN f.name AS Farm_name, toString(f.tax_number) AS Tax_number,
COALESCE(fa.first_degree_2020, 0) AS first_degree_2020, fa.second_degree_2020 AS second_degree_2020, fa.third_degree_2020 as third_degree_2020,
fa.first_degree_2019 AS first_degree_2019, fa.second_degree_2019 AS second_degree_2019, fa.third_degree_2019 as third_degree_2019
ORDER BY first_degree_2020 DESC
" AS query
CALL apoc.export.csv.query(query, "supply_shed_2019-2020_wout_city.csv", {})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data;

// Create the consolidated CSV (with cities)
WITH "MATCH (fa:Farm_aggregation) WHERE SIZE(keys(fa)) > 1
WITH fa
MATCH (f:Farm)-[:IS_LOCATED_IN]->(c:City)
WHERE elementId(f) = fa.node_id
WITH c, f, fa
RETURN c.name AS City_name, c.geocode AS City_geocode,
f.name AS Farm_name, toString(f.tax_number) AS Tax_number,
COALESCE(fa.first_degree_2020, 0) AS first_degree_2020, fa.second_degree_2020 AS second_degree_2020, fa.third_degree_2020 as third_degree_2020,
fa.first_degree_2019 AS first_degree_2019, fa.second_degree_2019 AS second_degree_2019, fa.third_degree_2019 as third_degree_2019
ORDER BY first_degree_2020 DESC
" AS query
CALL apoc.export.csv.query(query, "supply_shed_2019-2020.csv", {})
YIELD file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data
RETURN file, source, format, nodes, relationships, properties, time, rows, batchSize, batches, done, data;

