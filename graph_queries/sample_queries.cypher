// Identify top 5 receiving slaughterhouses
MATCH (f:Farm)-[st:SENDS_TO]->(s:Slaughterhouse)-[:IS_LOCATED_IN]->(c:City)
RETURN c.name AS city, s.name AS slaughterhouse_name, s.tax_number AS slaughterhouse_taxnumber, 
	SUM(st.total_sent) as animals_received 
ORDER BY animals_received DESC
LIMIT 5

// Number of farms sending to JBS Maraba (top receiving farm): 1.394
MATCH (s:Slaughterhouse)<-[:SENDS_TO]-(f:Farm) 
WHERE s.tax_number = "02916265014110"
RETURN COUNT(DISTINCT(f)) AS first_degree_farms

// Number of farms up to the fourth degree from JBS Maraba: 133.230
MATCH (s:Slaughterhouse)<-[:SENDS_TO*1..4]-(f:Farm) 
WHERE s.tax_number = "02916265014110"
RETURN COUNT(DISTICT(f))

// Number of farms in Maraba: 7.636
MATCH (f:Farm)-[:IS_LOCATED_IN]->(c:City)
WHERE c.name = 'MARABA'
RETURN COUNT(DISTINCT(f))

// Number of farms up to the 4th degree, located in Maraba: 6.360
MATCH (s:Slaughterhouse)<-[:SENDS_TO*1..4]-(f:Farm) 
WHERE s.tax_number = "02916265014110"
WITH DISTINCT(f) AS d_f
WHERE (d_f)-[:IS_LOCATED_IN]->(:City {name: 'MARABA'})
RETURN COUNT(d_f)

------

// Sample 2nd degree animal groups (and associated farms) going to JBS Maraba, through 
// a group of animals from Fazenda Piedade
MATCH p=(s:Slaughterhouse)<-[:GOES_TO]-(ag1:Animal_Group)-[:COMES_FROM]->(f1:Farm)<-[:GOES_TO]-(ag2:Animal_Group)-[:COMES_FROM]->(f2:Farm)
WHERE s.tax_number = '02916265014110'
    AND f1.tax_number <>''
    AND NOT (f2)-[:SENDS_TO]->(s)
    AND f1.tax_number = '94362530100'
RETURN p
LIMIT 3

// Number of animals, groups of animals and farms sending to Fazenda Piedade: 12.866, 305, 127
MATCH (f:Farm)<-[:GOES_TO]-(ag:Animal_Group)-[:COMES_FROM]->(f2:Farm)
WHERE f.tax_number = '94362530100'
RETURN SUM(ag.amount) AS animals_sent, COUNT(ag) AS animal_groups, COUNT(DISTINCT(f2)) AS farms

