// Initial creation of Animal Groups
CALL apoc.periodic.iterate("
    MATCH (o:Farm)-->(g:GTA)-[ass:ANIMAL_TYPE_SENT]->(at:Animal_Type), (g:GTA)-->(dest:Farm|Slaughterhouse) 
        RETURN o, g, ass, at, dest",
    "CREATE (o)<-[:COMES_FROM]-(ag:Animal_Group 
        {amount: ass.amount,
        sex: apoc.text.regexGroups(at.animal_type, '(?<=,)[^,]+(?=,)')[0][0],
        lower_age: toInteger(apoc.text.regexGroups(at.animal_type, '^[^\d]*(\d+)')[0][1]),
        upper_age: toInteger(apoc.text.regexGroups(at.animal_type, '(\d+)(?!.*\d)')[0][1]),
        description: at.animal_type,
        transport_date: g.date,
        transport_year: g.year,
        related_GTA: g.gta_id
        })-[:GOES_TO]->(dest)
    CREATE (ag)-[:FROM_GTA]->(g)
    ",
    {})

// Set upper age in 42 on animals that have 'ACIMA DE'
// They can be identified because they have the same lower and upper age
CALL apoc.periodic.iterate("MATCH (ag:Animal_Group) 
WHERE ag.lower_age = ag.upper_age RETURN ag",
"SET ag.upper_age = 42", {})

