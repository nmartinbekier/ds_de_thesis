// Load JSON file
CALL apoc.periodic.iterate("CALL apoc.load.json('file:///sample.json') YIELD value AS v",
"MERGE (o_city:City {geocode:COALESCE(v.ORIGIN_GEOCODE, '')})
  ON CREATE SET o_city.name = v.ORIGIN_CITY
MERGE (d_city:City {geocode:COALESCE(v.DESTINATION_GEOCODE, '')})
  ON CREATE SET d_city.name = v.DESTINATION_CITY
MERGE (o_state:State {name:COALESCE(v.ORIGIN_STATE, '')})
MERGE (d_state:State {name:COALESCE(v.DESTINATION_STATE, '')})
MERGE (o_city)-[:IS_LOCATED_IN]->(o_state)
MERGE (d_city)-[:IS_LOCATED_IN]->(d_state)
MERGE (transp:Transport {transport:COALESCE(v.TRANSPOT, '')})
MERGE (purp:Purpose {purpose:COALESCE(v.SPECIES_PURPOSE, '')})
MERGE (gta_src:GTA_Source {gta_source:COALESCE(v.GTA_SOURCE, '')})
MERGE (sp:Species {species:COALESCE(v.SPECIES, '')})
MERGE (o_code:Code {code:COALESCE(v.ORIGIN_CODE, '')})
MERGE (d_code:Code {code:COALESCE(v.DESTINATION_CODE, '')})
CREATE (gta:GTA {gta_id:COALESCE(v.ID, ''), 
        info_status : v.INFO_STATUS,
        timeline_obs : v.TIMELINE_OBS,
        missing_info : v.MISSING_INFO,
        date : v.TRANSPORT_DATE,
        year : v.TRANSPORT_YEAR,
        num_animals_assumed : v.NUM_ANIMALS_ASSUMED,
        slaughter_gta : v.SLAUGHTER_GTA,
        total_animals : 0})
FOREACH (an_sent IN v.ANIMALS |
    MERGE (an_type:Animal_Type {animal_type:COALESCE(an_sent.DESCRIPTION, '')})
    CREATE (gta)-[:ANIMAL_TYPE_SENT {amount:COALESCE(an_sent.AMOUNT_SENT, '')}]->(an_type)
    SET gta.total_animals = gta.total_animals + an_sent.AMOUNT_SENT
)
CREATE (gta)-[:IS_TRANSPORTED_BY]->(transp)
CREATE (gta)-[:HAS_PURPOSE]->(purp)
CREATE (gta)-[:FROM_GTA_SOURCE]->(gta_src)
CREATE (gta)-[:SPECIES_TRANSPORTED]->(sp)
MERGE (o_farm:Farm {tax_number:COALESCE(v.ORIGIN_TAX_NUMBER, ''), name:COALESCE(v.ORIGIN_NAME, '')})
MERGE (o_farm)-[:HAS_CODE]->(o_code)
CREATE (o_farm)-[:IS_ORIGIN_OF_GTA]->(gta)
MERGE (o_farm)-[:IS_LOCATED_IN]->(o_city)
FOREACH (i IN CASE WHEN NOT gta.slaughter_gta THEN [1] ELSE [] END |
    MERGE (d_farm:Farm {tax_number:COALESCE(v.DESTINATION_TAX_NUMBER,''), name:COALESCE(v.DESTINATION_NAME,'')})
    MERGE (gta)-[:GTA_HAS_DESTINATION]->(d_farm)
    MERGE (d_farm)-[:IS_LOCATED_IN]->(d_city)
    MERGE (d_farm)-[:HAS_CODE]->(d_code)
    MERGE (o_farm)-[sends:SENDS_TO]->(d_farm)
        ON CREATE SET sends.total_sent = gta.total_animals,
                      sends.total_2020 = 0,
                      sends.total_2019 = 0,
                      sends.total_2018 = 0,
                      sends.total_2017 = 0,
                      sends.total_2016 = 0,
                      sends.total_2015 = 0,
                      sends.total_2014 = 0,
                      sends.total_2013 = 0
        ON MATCH SET sends.total_sent = sends.total_sent + gta.total_animals
)
FOREACH( i IN CASE WHEN gta.slaughter_gta THEN [1] ELSE [] END |
    MERGE (slaughter:Slaughterhouse {tax_number:COALESCE(v.DESTINATION_TAX_NUMBER,''), name:COALESCE(v.DESTINATION_NAME,'')})
    MERGE (gta)-[:GTA_HAS_DESTINATION]->(slaughter)
    MERGE (slaughter)-[:IS_LOCATED_IN]->(d_city)
    MERGE (slaughter)-[:HAS_CODE]->(d_code)
    MERGE (o_farm)-[sends:SENDS_TO]->(slaughter)
        ON CREATE SET sends.total_sent = gta.total_animals,
                      sends.total_2020 = 0,
                      sends.total_2019 = 0,
                      sends.total_2018 = 0,
                      sends.total_2017 = 0,
                      sends.total_2016 = 0,
                      sends.total_2015 = 0,
                      sends.total_2014 = 0,
                      sends.total_2013 = 0
        ON MATCH SET sends.total_sent = sends.total_sent + gta.total_animals
)
WITH o_farm, gta, v
MATCH (o_farm)-[sends:SENDS_TO]->(:Farm|Slaughterhouse {tax_number:v.DESTINATION_TAX_NUMBER})
CALL apoc.do.case([
    gta.year = 2020, 'SET sends.total_2020 = sends.total_2020 + gta.total_animals RETURN sends',
    gta.year = 2019, 'SET sends.total_2019 = sends.total_2019 + gta.total_animals RETURN sends',
    gta.year = 2018, 'SET sends.total_2018 = sends.total_2018 + gta.total_animals RETURN sends',
    gta.year = 2017, 'SET sends.total_2017 = sends.total_2017 + gta.total_animals RETURN sends',
    gta.year = 2016, 'SET sends.total_2016 = sends.total_2016 + gta.total_animals RETURN sends',
    gta.year = 2015, 'SET sends.total_2015 = sends.total_2015 + gta.total_animals RETURN sends',
    gta.year = 2014, 'SET sends.total_2014 = sends.total_2014 + gta.total_animals RETURN sends',
    gta.year = 2013, 'SET sends.total_2013 = sends.total_2013 + gta.total_animals RETURN sends'],
    'RETURN []',
    {gta:gta, sends:sends}
) YIELD value
RETURN value
", {})
