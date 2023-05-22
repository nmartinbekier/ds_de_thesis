CALL apoc.periodic.iterate("
MATCH(ag1:Animal_Group)-[:COMES_FROM]->(f1:Farm)<-[:GOES_TO]-(ag2:Animal_Group)-[:COMES_FROM]->(f2:Farm)
WHERE ag1.sex = ag2.sex
AND ag1.transport_date >= ag2.transport_date
AND ag2.lower_age + duration.inMonths(date(ag2.transport_date), date(ag1.transport_date)).months <= ag1.upper_age
AND ag2.upper_age + duration.inMonths(date(ag2.transport_date), date(ag1.transport_date)).months >= ag1.lower_age
RETURN ag1, ag2,
apoc.coll.min([ag1.amount, ag2.amount]) AS possible_animals_sent,
apoc.coll.min([ag1.upper_age, ag2.upper_age + duration.inMonths(date(ag2.transport_date), date(ag1.transport_date)).months]) - apoc.coll.max([ag1.lower_age, ag2.lower_age + duration.inMonths(date(ag2.transport_date), date(ag1.transport_date)).months]) + 1 AS 
time_intersection",
"MERGE (ag1)-[ccf:COULD_COME_FROM {possible_animals: possible_animals_sent,
month_intersection: time_intersection}]->(ag2)", {})