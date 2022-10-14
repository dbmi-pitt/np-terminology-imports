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
 select distinct concept_name concept_name_napdi, concept_id concept_id_napdi, concept_code,
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
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = upper(nps.concept_name_napdi)
where vocabulary_id ='RxNorm' and nps.np_concept_class_id = 'NaPDI NP Constituent'
;

--rxnorm np name matches (N=7)
with nps as ( 
 select distinct concept_name concept_name_napdi, concept_id concept_id_napdi, concept_code,
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
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = upper(nps.concept_name_napdi)
where vocabulary_id ='RxNorm' and nps.np_concept_class_id != 'NaPDI NP Constituent'
;

--substring match to rxnorm - save to temp (no extra cost for non indexed fields)
with nps as ( 
 select distinct upper(concept_name) concept_name_napdi, concept_id concept_id_napdi, concept_code,
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

--for all NPs - substring match
--N=14,674 matches in RxNorm for all NP terms (query time=14 minutes)
with nps as ( 
 select distinct upper(concept_name) concept_name_napdi, concept_id concept_id_napdi, concept_code,
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

--N=3366
insert into scratch_sanya.np_to_rxnorm (concept_id_rxnorm, concept_id_napdi, concept_name_rxnorm,
concept_name_napdi, concept_class_id_rxnorm, concept_code, valid_start_date, valid_end_date)
select npx.concept_id_rxnorm, npx.concept_id_napdi, npx.concept_name_rxnorm, npx.concept_name_napdi,
npx.concept_class_id_rxnorm, npx.concept_code, npx.valid_start_date, npx.valid_end_date 
from scratch_sanya.np_rxnorm_substring_temp npx
where npx.concept_class_id_napdi = 'NaPDI Preferred Term'

--N=11308
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
										'2000-01-01', '2099-02-22', '');

--add to vocabulary.concept_relationship
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

select count(*) from scratch_sanya.np_to_rxnorm ntr 

--add values to concept_relationship
--N=3373 (exact+substring match of np names)
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
valid_start_date, valid_end_date, invalid_reason)
select concept_id_napdi, concept_id_rxnorm,  'napdi_np_maps_to', valid_start_date, valid_end_date, ''
from scratch_sanya.np_to_rxnorm

--N=11431 (exact+substring match of constituents)
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
valid_start_date, valid_end_date, invalid_reason)
select concept_id_napdi, concept_id_rxnorm,  'napdi_const_maps_to', valid_start_date, valid_end_date, ''
from scratch_sanya.np_const_to_rxnorm

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
