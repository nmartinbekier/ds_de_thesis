// Creation of indexes
CREATE INDEX animal_group_lower_age_index IF NOT EXISTS FOR (n:Animal_Group) ON (n.lower_age);
CREATE INDEX animal_group_upper_age_index IF NOT EXISTS FOR (n:Animal_Group) ON (n.upper_age);
CREATE INDEX animal_group_sex_index IF NOT EXISTS FOR (n:Animal_Group) ON (n.sex);
CREATE INDEX animal_group_date_index IF NOT EXISTS FOR (n:Animal_Group) ON (n.transport_date);
CREATE INDEX gta_id_index IF NOT EXISTS FOR (n:GTA) ON (n.gta_id);
CREATE INDEX gta_date_index IF NOT EXISTS FOR (n:GTA) ON (n.transport_date);
CREATE INDEX gta_year_index IF NOT EXISTS FOR (n:GTA) ON (n.transport_year);
CREATE INDEX farm_name_index IF NOT EXISTS FOR (n:Farm) ON (n.name);
CREATE INDEX farm_tax_index IF NOT EXISTS FOR (n:Farm) ON (n.tax_number);
CREATE INDEX slaughterhouse_name_index IF NOT EXISTS FOR (n:Slaughterhouse) ON (n.name);
CREATE INDEX slaughterhouse_tax_index IF NOT EXISTS FOR (n:Slaughterhouse) ON (n.tax_number);
CREATE INDEX farm_name_and_tax_index IF NOT EXISTS FOR (n:Farm) ON (n.tax_number, n.name);
CREATE INDEX slaughterhouse_name_and_tax_index IF NOT EXISTS FOR (n:Farm) ON (n.tax_number, n.name);