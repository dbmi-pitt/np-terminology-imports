--Example JSON object to be added as row in table: 
--{'lnhpd_id': 3911193, 
--'ingredient_name': 'Beta-carotene', 
--'ingredient_Text': None, 
--'potency_amount': 0.0, 
--'potency_constituent': '', 
--'potency_unit_of_measure': '', 
--'quantity': 750.0, 
--'quantity_minimum': 0.0, 
--'quantity_maximum': 0.0, 
--'quantity_unit_of_measure': 'mcg EAR', 
--'ratio_numerator': '', 
--'ratio_denominator': '', 
--'dried_herb_equivalent': '', 
--'dhe_unit_of_measure': '', 
--'extract_type_desc': '', 
--'source_material': 'Vitamin A'}

DROP TABLE IF EXISTS ${LNHPD_schema}.lnhpd_staging;

CREATE TABLE ${LNHPD_schema}.lnhpd_staging (
	lnhpd_id INTEGER NOT NULL PRIMARY KEY,
	ingredient_name VARCHAR(255),
	ingredient_Text VARCHAR(255),
	potency_amount FLOAT,
	potency_constituent VARCHAR(255),
	potency_unit_of_measure VARCHAR(20),
	quantity FLOAT,
	quantity_minimum FLOAT,
	quantity_maximum FLOAT,
	quantity_unit_of_measure VARCHAR(20),
	ratio_numerator VARCHAR(20),
	ratio_denominator VARCHAR(20),
	dried_herb_equivalent VARCHAR(20),
	dhe_unit_of_measure VARCHAR(20),
	extract_type_desc VARCHAR(20),
	source_material VARCHAR(255)
)

create unlogged table ${LNHPD_schema}.lnhpd_json (doc json);
--psql: \copy lnhpd_json from '/Users/sanya/npdi-workspace/LNHPD/LNHPD_output/lnhpd_all_unique.json'
insert into ${LNHPD_schema}.lnhpd_staging (lnhpd_id, ingredient_name, ingredient_Text, potency_amount, potency_constituent, potency_unit_of_measure,
quantity, quantity_minimum, quantity_maximum, quantity_unit_of_measure, ratio_numerator, ratio_denominator, 
dried_herb_equivalent, dhe_unit_of_measure, extract_type_desc, source_material)
select p.*
from ${LNHPD_schema}.lnhpd_json l
  cross join lateral json_populate_recordset(null::${LNHPD_schema},lnhpd_staging, doc) as p
on conflict (lnhpd_id) do nothing;