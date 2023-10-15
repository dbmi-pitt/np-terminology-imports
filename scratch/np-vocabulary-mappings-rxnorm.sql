/*add mappings for NP names and constituents to RxNorm concepts (if any)*/
drop table scratch_sanya.np_const_to_rxnorm;
 
drop table scratch_sanya.np_rxnorm_substring_temp 

create table if not exists scratch_sanya.np_to_rxnorm (
concept_id_rxnorm int4 not null,
concept_id_napdi int4 not null,
concept_name_rxnorm varchar,
concept_name_napdi varchar,
concept_class_id_rxnorm varchar,
concept_code varchar,
valid_start_date date,
valid_end_date date
);

create table if not exists scratch_sanya.np_const_to_rxnorm (
concept_id_rxnorm int4 not null,
concept_id_napdi int4 not null,
concept_name_rxnorm varchar,
concept_name_napdi varchar,
concept_class_id_rxnorm varchar,
concept_code varchar,
valid_start_date date,
valid_end_date date
);

create table if not exists scratch_sanya.np_rxnorm_substring_temp (
concept_id_rxnorm int4 not null,
concept_id_napdi int4 not null,
concept_name_rxnorm varchar,
concept_name_napdi varchar,
concept_class_id_rxnorm varchar,
concept_class_id_napdi varchar,
concept_code varchar,
valid_start_date date,
valid_end_date date
);

--test query to find RxNorm concepts - exact matches constituents (N=123)
with nps as ( 
 select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
 concept_class_id np_concept_class_id
 from staging_vocabulary.concept c 
 where c.vocabulary_id ='NAPDI'
) 
insert into scratch_sanya.np_const_to_rxnorm
(concept_id_rxnorm, concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
concept_code, valid_start_date, valid_end_date)
select c2.concept_id concept_id_rxnorm, c2.concept_name concept_name_rxnorm, nps.concept_id_napdi, 
nps.concept_name_napdi, c2.concept_class_id concept_class_id_rxnorm, nps.concept_code, 
c2.valid_start_date, c2.valid_end_date
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = nps.concept_name_napdi
where vocabulary_id ='RxNorm' and nps.np_concept_class_id = 'NaPDI NP Constituent'
;

--rxnorm np name matches (N=7)
with nps as ( 
 select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
 concept_class_id np_concept_class_id
 from staging_vocabulary.concept c 
 where c.vocabulary_id ='NAPDI'
) 
insert into scratch_sanya.np_to_rxnorm 
(concept_id_rxnorm, concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
concept_code, valid_start_date, valid_end_date)
select c2.concept_id concept_id_rxnorm, c2.concept_name concept_name_rxnorm, nps.concept_id_napdi, 
nps.concept_name_napdi, c2.concept_class_id concept_class_id_rxnorm, nps.concept_code, 
c2.valid_start_date, c2.valid_end_date
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = nps.concept_name_napdi
where vocabulary_id ='RxNorm' and nps.np_concept_class_id != 'NaPDI NP Constituent'
;

--for all NPs - substring match
with nps as (
select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
   concept_class_id concept_class_id_napdi
from staging_vocabulary.concept c 
where c.vocabulary_id ='NAPDI'
and c.concept_class_id != 'NaPDI NP Spelling Variation'
), rxns as (
 select distinct c2.concept_id concept_id_rxnorm, c2.vocabulary_id, c2.standard_concept,
    upper(c2.concept_name) concept_name_rxnorm, c2.concept_class_id concept_class_id_rxnorm,
    c2.valid_start_date, c2.valid_end_date
 from staging_vocabulary.concept c2 
 where c2.vocabulary_id = 'RxNorm'
)
insert into scratch_oct2022_np_vocab.np_rxnorm_substring_temp (concept_id_rxnorm,
 concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
 concept_class_id_napdi, concept_code, valid_start_date, valid_end_date)
select distinct rxns.concept_id_rxnorm, rxns.concept_name_rxnorm, nps.concept_id_napdi,
  nps.concept_name_napdi, rxns.concept_class_id_rxnorm, nps.concept_class_id_napdi, nps.concept_code,
  rxns.valid_start_date, rxns.valid_end_date
from rxns inner join nps on (
                               rxns.concept_name_rxnorm like concat('% ',nps.concept_name_napdi) or
                               rxns.concept_name_rxnorm like concat(nps.concept_name_napdi,' %') or
                               rxns.concept_name_rxnorm like concat('% ',nps.concept_name_napdi,' %')
                             )
;

--N=20,216 (includes non-exact matches, filtering done below)
insert into scratch_sanya.np_to_rxnorm (concept_id_rxnorm, concept_id_napdi, concept_name_rxnorm,
concept_name_napdi, concept_class_id_rxnorm, concept_code, valid_start_date, valid_end_date)
select npx.concept_id_rxnorm, npx.concept_id_napdi, npx.concept_name_rxnorm, npx.concept_name_napdi,
npx.concept_class_id_rxnorm, npx.concept_code, npx.valid_start_date, npx.valid_end_date 
from scratch_sanya.np_rxnorm_substring_temp npx
where npx.concept_class_id_napdi != 'NaPDI NP Constituent'

--N=11,308
insert into scratch_sanya.np_const_to_rxnorm (concept_id_rxnorm, concept_id_napdi, concept_name_rxnorm,
concept_name_napdi, concept_class_id_rxnorm, concept_code, valid_start_date, valid_end_date)
select npx.concept_id_rxnorm, npx.concept_id_napdi, npx.concept_name_rxnorm, npx.concept_name_napdi,
npx.concept_class_id_rxnorm, npx.concept_code, npx.valid_start_date, npx.valid_end_date 
from scratch_sanya.np_rxnorm_substring_temp npx
where npx.concept_class_id_napdi = 'NaPDI NP Constituent'

--add relationship in vocabulary - constituent_maps_to, np_maps_to
--add to vocabulary.concept
create temporary sequence if not exists napdi_concept_sequence as integer increment by -1 maxvalue -7005067;

INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
VALUES (nextval('napdi_concept_sequence'), 'napdi_np_maps_to', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');
									
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
VALUES (nextval('napdi_concept_sequence'), 'napdi_const_maps_to', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '')
;

--add to vocabulary.relationship
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
with rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_np_maps_to')
select 'napdi_np_maps_to', 'NaPDI NP maps to', 0, 0, 0, rel_id 
from rel
;  

insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
with rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_np_maps_to')
select 'napdi_const_maps_to', 'NaPDI constituent maps to', 0, 0, 0, rel_id 
from rel
; 

--Filtering mappings before adding to vocabulary

--remove BALM, JU, MATE, RAPE, RAPE SEED after review
delete from scratch_sanya.np_to_rxnorm 
where concept_id_napdi in (-7000171, -7000259, -7000470, -7000562, -7000172)

--how to include filtering done in rxnorm-mappings-string-match.ipynb?? filters out non exact matches (N=)
--also include rxnorm-faers-match queries?

--Add to concept_relationship: Exact+substring match of NP names
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
valid_start_date, valid_end_date, invalid_reason)
select concept_id_napdi, concept_id_rxnorm,  'napdi_np_maps_to', valid_start_date, valid_end_date, ''
from scratch_sanya.np_to_rxnorm

--Add to concept_relationship: Exact+substring match of constituents
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
valid_start_date, valid_end_date, invalid_reason)
select concept_id_napdi, concept_id_rxnorm,  'napdi_const_maps_to', valid_start_date, valid_end_date, ''
from scratch_sanya.np_const_to_rxnorm


/**BELOW QUERIES NOT FOR VOCAB - ASSESS MAPPINGS AFTER REVEW**/

--Generate tables for review
select distinct ntr.concept_id_napdi, ntr.concept_id_rxnorm, ntr.concept_name_napdi, ntr.concept_name_rxnorm from scratch_sanya.np_to_rxnorm ntr 

select distinct ntr.concept_name_napdi, count(*) from scratch_sanya.np_to_rxnorm ntr 
group by ntr.concept_name_napdi 

--Query to find codes used in FAERS data
select t.drug_concept_id, count(distinct t.rid)
from (
 ( 
 select drug_concept_id, primaryid rid
 from faers.standard_drug_outcome_drilldown sdod 
  inner join scratch_sanya.np_rxnorm_faers_match nrfm on sdod.drug_concept_id = nrfm.concept_id_rxnorm 
  where primaryid is not null 
    and isr is null
 )
 union
 ( 
 select drug_concept_id, isr rid
 from faers.standard_drug_outcome_drilldown sdod 
  inner join scratch_sanya.np_rxnorm_faers_match nrfm on sdod.drug_concept_id = nrfm.concept_id_rxnorm 
  where primaryid is not null 
    and isr is not null
 )
) t
group by t.drug_concept_id

--queries to get RxNorm mappings - add to example queries on GitHub
select * from staging_vocabulary.concept c 
where c.concept_id = '-7001523'

select * from staging_vocabulary.concept c where concept_class_id = 'Cinnamon';

--cinnamon
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
and c1.concept_id = -7000976

--ginger
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
and c1.concept_id = -7001032

--all NP mappings to RxNorm
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'

--cinnamon constituents
select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7000976'

--cinnamon constituents RxNorm mappings
with cinn_cons as (
select c1.concept_name np_name, c1.concept_id np_id,  c2.concept_name np_const_name, c2.concept_id np_const_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7001529
)
select cinn_cons.np_id, cinn_cons.np_name, cr2.concept_id_1 napdi_const_id, cinn_cons.np_const_name, 
cr2.concept_id_2 rxnorm_id, c.concept_name rxnorm_name
from staging_vocabulary.concept_relationship cr2 
inner join cinn_cons on cinn_cons.np_const_id = cr2.concept_id_1 
inner join staging_vocabulary.concept c on c.concept_id = cr2.concept_id_2 
where cr2.relationship_id = 'napdi_const_maps_to'

--constituent mappings sample
select distinct c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'

with np_cons as (
select distinct c1.concept_name np_name, c1.concept_id np_id, c3.concept_name np_const_name, c3.concept_id np_const_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id in (-7000976, -7001047, -7001077, -7001021, -7000976, -7001198, -7001224,
   -7001012, -7000966, -7001032, -7001092, -7001008, -7001117, -7000994, -7001153, -7001114,
   -7001156, -7001087, -7001025, -7001083, -7001209, -7001094, -7001050)
)
select np_cons.np_id, np_cons.np_name, cr2.concept_id_1 np_const_id, np_cons.np_const_name, 
cr2.concept_id_2 rxnorm_id, c.concept_name rxnorm_name
from staging_vocabulary.concept_relationship cr2 
inner join np_cons on np_cons.np_const_id = cr2.concept_id_1 
inner join staging_vocabulary.concept c on c.concept_id = cr2.concept_id_2 
where cr2.relationship_id = 'napdi_const_maps_to'

---SCRATCH

--N=14,755 (Ingredient)
--N=3,151 (Precise Ingredient)
select count(*) from staging_vocabulary.concept c 
where c.vocabulary_id = 'RxNorm' and c.concept_class_id = 'Precise Ingredient'

--N=18
select distinct c.concept_class_id  from staging_vocabulary.concept c 
where c.vocabulary_id = 'RxNorm'

--N=2,289 (total is 5061 with spelling variations)
select count(*) from staging_vocabulary.concept c 
where c.vocabulary_id = 'NAPDI' and c.concept_class_id != 'NaPDI NP Spelling Variation'

--DO - find all strings from 14755+3151 with 2289 possible sub strings
--find all RxNorm NP concepts - Latin binomial, common names, constituents
--very very long with all matches
with nps as ( 
 select distinct concept_name np_name
 from staging_vocabulary.concept c 
 where c.vocabulary_id ='NAPDI'
)
select * 
from staging_vocabulary.concept c2 
inner join nps on upper(c2.concept_name) like concat('%',nps.np_name,'%')
where c2.vocabulary_id = 'RxNorm' and nps.np_name = 'GREEN TEA'
;

select * from staging_vocabulary.concept c 
where c.vocabulary_id = 'RxNorm' and upper(c.concept_name) like concat('%', 'GREEN TEA', '%')

select * from staging_vocabulary.concept c
where c.concept_id = -7005067

select * from scratch_feb2022_np_vocab.lb_to_constituent ltc 
where ltc.latin_binomial = 'Camellia sinensis'

select * from scratch_feb2022_np_vocab.lb_to_common_name_and_pt ltcnap 
where ltcnap.pt = 'Tao'

with pt_lb_mapping as ( 
  select distinct c1.concept_name pt, regexp_replace(c2.concept_name, '.*\[(.*)\]','\1' ) lb
  from staging_vocabulary.concept c1 
     inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
  where cr.relationship_id = 'napdi_is_pt_of'
)
select distinct pt_lb_mapping.lb, regexp_replace(c2.concept_name, '\[.*\]','' ) mappings
from staging_vocabulary.concept c1 
     inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
     inner join pt_lb_mapping on c1.concept_name = pt_lb_mapping.pt
where cr.relationship_id = 'napdi_is_pt_of'
order by  pt_lb_mapping.lb
;

select * from staging_vocabulary.concept c 
where c.vocabulary_id = 'RxNorm'

select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
concept_class_id concept_class_id_napdi
from staging_vocabulary.concept c 
where c.vocabulary_id ='NAPDI'
and c.concept_class_id != 'NaPDI NP Spelling Variation' and c.concept_class_id = 'Feverfew'

--substring match to rxnorm - save to temp (no extra cost for non indexed fields)
with nps as ( 
select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
concept_class_id concept_class_id_napdi
from staging_vocabulary.concept c 
where c.vocabulary_id ='NAPDI'
and c.concept_class_id != 'NaPDI NP Spelling Variation'
), rxns as (
 select distinct c2.concept_id concept_id_rxnorm, c2.vocabulary_id, c2.standard_concept, 
 upper(c2.concept_name) concept_name_rxnorm, c2.concept_class_id concept_class_id_rxnorm,
 c2.valid_start_date, c2.valid_end_date
 from staging_vocabulary.concept c2 
 where c2.vocabulary_id = 'RxNorm'
)
insert into scratch_sanya.np_rxnorm_substring_temp
(concept_id_rxnorm, concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
concept_class_id_napdi, concept_code, valid_start_date, valid_end_date)
select rxns.concept_id_rxnorm, rxns.concept_name_rxnorm, nps.concept_id_napdi, 
nps.concept_name_napdi, rxns.concept_class_id_rxnorm, nps.concept_class_id_napdi, nps.concept_code, 
rxns.valid_start_date, rxns.valid_end_date
from rxns inner join nps on rxns.concept_name_rxnorm like concat('%',nps.concept_name_napdi,'%')  
where nps.concept_name_napdi = 'GREEN TEA'
;

with nps as ( 
select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
concept_class_id concept_class_id_napdi
from staging_vocabulary.concept c 
where c.vocabulary_id ='NAPDI'
and c.concept_class_id != 'NaPDI NP Spelling Variation'
), rxns as (
 select distinct c2.concept_id concept_id_rxnorm, c2.vocabulary_id, c2.standard_concept, 
 upper(c2.concept_name) concept_name_rxnorm, c2.concept_class_id concept_class_id_rxnorm,
 c2.valid_start_date, c2.valid_end_date
 from staging_vocabulary.concept c2 
 where c2.vocabulary_id = 'RxNorm'
)
insert into scratch_sanya.np_rxnorm_substring_temp (concept_id_rxnorm, 
concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm, 
concept_class_id_napdi, concept_code, valid_start_date, valid_end_date)
select rxns.concept_id_rxnorm, rxns.concept_name_rxnorm, nps.concept_id_napdi, 
nps.concept_name_napdi, rxns.concept_class_id_rxnorm, nps.concept_class_id_napdi, nps.concept_code, 
rxns.valid_start_date, rxns.valid_end_date
from rxns inner join nps on rxns.concept_name_rxnorm like concat('%',nps.concept_name_napdi,'%')
;

