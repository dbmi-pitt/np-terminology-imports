-- napdi_np_vocab_workflow_OCT_2022.sql

-- Search path for this workflow
set search_path to scratch_oct2022_np_vocab;

-- Step 1) Obtaining NP latin binomials to common name mapping

-- Obtain the table of latin binomials to common names that Sanya created from G-SRS and 
-- mark preferred names using either ones we identified from manual mapping or by selecting
-- the first common name


-- scratch_oct2022_np_vocab.np_faers_reference_set definitionk
DROP TABLE scratch_oct2022_np_vocab.np_faers_reference_set;
CREATE TABLE scratch_oct2022_np_vocab.np_faers_reference_set (
	seq serial4 NOT NULL,
	drug_name_original varchar NULL,
	related_common_name varchar NULL,
	related_latin_binomial varchar NULL,
	lookup_value varchar NULL,
	concept_id int4 NULL,
	CONSTRAINT np_faers_reference_set_pkey PRIMARY KEY (seq)
);
-- Load the data from file 



DROP TABLE scratch_oct2022_np_vocab.lb_to_common_names_tsv;
CREATE TABLE scratch_oct2022_np_vocab.lb_to_common_names_tsv (
	latin_binomial varchar(29) NULL,
	common_name varchar(29) NULL
);

DROP table if exists scratch_oct2022_np_vocab.test_srs_np;
CREATE TABLE scratch_oct2022_np_vocab.test_srs_np (
	related_latin_binomial varchar(255) NOT NULL,
	related_common_name varchar(40) NULL,
	dtype varchar(10) NULL,
	substance_uuid varchar(40) NULL,
	created timestamp NULL,
	"class" int4 NULL,
	status varchar(255) NULL,
	modifications_uuid varchar(40) NULL,
	approval_id varchar(20) NULL,
	structure_id varchar(40) NULL,
	structurally_diverse_uuid varchar(40) NULL,
	name_uuid varchar(40) NULL,
	internal_references text NULL,
	owner_uuid varchar(40) NULL,
	name varchar(255) NULL,
	"type" varchar(32) NULL,
	preferred bool NULL,
	display_name bool NULL,
	structdiv_uuid varchar(40) NULL,
	source_material_class varchar(255) NULL,
	source_material_state varchar(255) NULL,
	source_material_type varchar(255) NULL,
	organism_family varchar(255) NULL,
	organism_author varchar(255) NULL,
	organism_genus varchar(255) NULL,
	organism_species varchar(255) NULL,
	part_location varchar(255) NULL,
	part text NULL,
	parent_substance_uuid varchar(40) NULL
);


-- scratch_oct2022_np_vocab.test_srs_np_part definition

-- Drop table

DROP TABLE if exists scratch_oct2022_np_vocab.test_srs_np_part;
CREATE TABLE scratch_oct2022_np_vocab.test_srs_np_part (
	related_latin_binomial varchar(255) NOT NULL,
	related_common_name varchar(40) NULL,
	dtype varchar(10) NULL,
	substance_uuid varchar(40) NULL,
	created timestamp NULL,
	"class" int4 NULL,
	status varchar(255) NULL,
	modifications_uuid varchar(40) NULL,
	approval_id varchar(20) NULL,
	structure_id varchar(40) NULL,
	structurally_diverse_uuid varchar(40) NULL,
	name_uuid varchar(40) NULL,
	internal_references text NULL,
	owner_uuid varchar(40) NULL,
	"name" varchar(255) NULL,
	"type" varchar(32) NULL,
	preferred bool NULL,
	display_name bool NULL,
	structdiv_uuid varchar(40) NULL,
	source_material_class varchar(255) NULL,
	source_material_state varchar(255) NULL,
	source_material_type varchar(255) NULL,
	organism_family varchar(255) NULL,
	organism_author varchar(255) NULL,
	organism_genus varchar(255) NULL,
	organism_species varchar(255) NULL,
	part_location varchar(255) NULL,
	part text NULL,
	parent_substance_uuid varchar(40) NULL
);


DROP TABLE if exists scratch_oct2022_np_vocab.test_srs_np_constituent;
CREATE TABLE scratch_oct2022_np_vocab.test_srs_np_constituent (
	uuid varchar(40) NULL,
	substance_uuid varchar(40) NULL,
	constituent_uuid varchar(40) NULL,
	related_common_name varchar(255) NULL,
	related_latin_binomial varchar(255) NULL,
	relation_type varchar(255) NULL,
	constituent_name varchar(255) NULL,
	constituent_type varchar(20) NULL
);

-----

-- Load the data - currently created by a Python script - see ??

----

-- NOTE: although this queries the NP Part table, the same number of latin binomials result as if it didn't 
-- TODO: extend the vocabulary to address NP parts which themselves could be important, have unique common names, etc. 
drop table if exists scratch_oct2022_np_vocab.lb_to_common_name_and_pt;
with remote_lb as (
	select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np.substance_uuid concept_code 
	from scratch_oct2022_np_vocab.lb_to_common_names_tsv inner join scratch_oct2022_np_vocab.test_srs_np 
	            on lb_to_common_names_tsv.latin_binomial = test_srs_np.related_latin_binomial
 	union
	select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np_part.substance_uuid concept_code 
	from scratch_oct2022_np_vocab.lb_to_common_names_tsv inner join scratch_oct2022_np_vocab.test_srs_np_part 
	  		 	on lb_to_common_names_tsv.latin_binomial = test_srs_np_part.related_latin_binomial
), curated_pt as (
  select distinct latin_binomial, common_name,
         case when nfrs.lookup_value is not null then 1
              else null 
         end preferred
  from remote_lb left outer join scratch_oct2022_np_vocab.np_faers_reference_set nfrs on upper(remote_lb.common_name) = upper(nfrs.lookup_value)
), not_mapped as (
  select latin_binomial, max(preferred) mapped
  from curated_pt 
  group by latin_binomial 
), pick_one as (
  select distinct not_mapped.latin_binomial, max(curated_pt.common_name) common_name, 1 preferred
  from not_mapped inner join curated_pt on not_mapped.latin_binomial = curated_pt.latin_binomial
  where not_mapped.mapped is null
  group by not_mapped.latin_binomial
), all_pt as (
  select latin_binomial, common_name pt
  from curated_pt
  where preferred = 1
  union
  select latin_binomial, common_name pt
  from pick_one
  where preferred = 1
)
select remote_lb.latin_binomial, remote_lb.common_name, all_pt.pt, max(remote_lb.concept_code) 
into scratch_oct2022_np_vocab.lb_to_common_name_and_pt
from remote_lb inner join all_pt on remote_lb.latin_binomial = all_pt.latin_binomial 
group by remote_lb.latin_binomial, remote_lb.common_name, all_pt.pt
order by latin_binomial 
;
-- 945

select * from scratch_oct2022_np_vocab.lb_to_common_name_and_pt where pt = 'Aloe vera';

-- Step 2) Obtaining NP latin binomials to constituent mapping 
drop table if exists scratch_oct2022_np_vocab.lb_to_constituent;
with remote_const as (
   select distinct related_latin_binomial, constituent_name, constituent_uuid concept_code 
			from scratch_oct2022_np_vocab.test_srs_np_constituent

)
select *
into scratch_oct2022_np_vocab.lb_to_constituent
from remote_const
;
-- 915

 
-- Step 3) Obtaining manually curated spelling variations
select distinct related_latin_binomial latin_binomial, related_common_name pt, drug_name_original spelling_variation
into scratch_oct2022_np_vocab.lb_to_spelling_var
from scratch_oct2022_np_vocab.np_faers_reference_set nfrs 
order by latin_binomial 
;
-- 2772

-- (TODO) -- after backing up data and files, add a union to the table that Sanya will provide that will
---          provide the 2022 annotated mappings of previlusly unmapped drug name strings. Provide to Teams the
---          merged file with a readme and the SQL 


/* 
 -- Step 4) adding relationships, concepts, and concept relationship mappings
 
 concept table: 
 - add concepts napdi_has_const, napdi_is_const_of, napdi_pt, napdi_is_pt_of, napdi_spell_vr, napdi_is_spell_vr_of
 
 relationship table: 
 - add relationships: napdi_has_const/napdi_is_const_of; napdi_pt/napdi_is_pt_of; napdi_spell_vr/napdi_is_spell_vr_of; napdi_np_maps_to; napdi_const_maps_to
 
 concept table:
 - add all of the common names from scratch_oct2022_np_vocab.lb_to_common_name_and_pt
 - add all of the LBs from scratch_oct2022_np_vocab.lb_to_common_name_and_pt
 - add all of the constituents from scratch_oct2022_np_vocab.lb_to_constituent
 - add all of the spelling variations from scratch_oct2022_np_vocab.lb_to_spelling_var

 concept_relationship: 
 -- from scratch_oct2022_np_vocab.lb_to_common_name_and_pt to napdi_pt/napdi_is_pt_of
 -- from scratch_oct2022_np_vocab.lb_to_constituent to napdi_has_const/napdi_is_const_of
 -- from scratch_oct2022_np_vocab.lb_to_spelling_var to napdi_spell_vr/napdi_is_spell_vr_of
 

 */
create temporary sequence if not exists napdi_concept_sequence as integer increment by -1 maxvalue -7000000;

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
									valid_start_date, valid_end_date, invalid_reason) 
								VALUES (-9900000, 'NaPDI custom terms', 'Metadata', 'Domain', 'Domain', '', 'OMOP generated', 
									'2000-01-01', '2099-02-22', '');
INSERT INTO staging_vocabulary.domain (domain_id, domain_name, domain_concept_id) 
								VALUES ('NaPDI research', 'NaPDI research', -9900000);
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
									valid_start_date, valid_end_date, invalid_reason) 
								VALUES (-9990000, 'NaPDI Natural Product', 'Metadata', 'Concept Class', 'Concept Class', '', 'OMOP generated', 
									'2000-01-01', '2099-02-22', '');
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
									valid_start_date, valid_end_date, invalid_reason) 
								VALUES (-9990001, 'NaPDI NP Constituent', 'Metadata', 'Concept Class', 'Concept Class', '', 'OMOP generated', 
									'2000-01-01', '2099-02-22', '');																
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
									valid_start_date, valid_end_date, invalid_reason) 
								VALUES (-9990002, 'NaPDI NP Spelling Variation', 'Metadata', 'Concept Class', 'Concept Class', '', 'OMOP generated', 
									'2000-01-01', '2099-02-22', '');							
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
									valid_start_date, valid_end_date, invalid_reason) 
								VALUES (-9990003, 'NaPDI Preferred Term', 'Metadata', 'Concept Class', 'Concept Class', '', 'OMOP generated', 
									'2000-01-01', '2099-02-22', '');	
end;
-- 6

-- add concepts napdi_has_const, napdi_is_const_of, napdi_pt, napdi_is_pt_of, napdi_spell_vr, napdi_is_spell_vr_of, napdi_const_maps_to, napdi_np_maps_to
START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_has_const', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_is_const_of', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');									
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_pt', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_is_pt_of', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_spell_vr', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');								
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_is_spell_vr_of', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_np_maps_to', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');									
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_const_maps_to', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');
end;
-- 8


start transaction;
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_is_const_of'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_has_const')
                            select 'napdi_has_const', 'NaPDI has constituent', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;                                      		 
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_has_const'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_is_const_of')
                            select 'napdi_is_const_of', 'NaPDI is constituent of', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_is_pt_of'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_pt')
                            select 'napdi_pt', 'NaPDI NP preferred term', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;                           
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_pt'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_is_pt_of')
                            select 'napdi_is_pt_of', 'NaPDI is preferred term of ', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;                           
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_is_spell_vr_of'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_spell_vr')
                            select 'napdi_spell_vr', 'NaPDI spelling variation', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;                           
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_spell_vr'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_is_spell_vr_of')
                            select 'napdi_is_spell_vr_of', 'NaPDI is spelling variation of', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;                           
end;
-- 6


/*
concept table:
 - add all of the common names from scratch_oct2022_np_vocab.lb_to_common_name_and_pt
 - add all of the LBs from scratch_oct2022_np_vocab.lb_to_common_name_and_pt
 - add all of the constituents from scratch_oct2022_np_vocab.lb_to_constituent
 - add all of the spelling variations from scratch_oct2022_np_vocab.lb_to_spelling_var
*/
START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), concat(lbpt.common_name, '[', lbpt.latin_binomial, ']'), 'NaPDI research', 'NAPDI', lbpt.pt, '', lbpt.max, 
										'2000-01-01', '2099-02-22', ''
	   from scratch_oct2022_np_vocab.lb_to_common_name_and_pt lbpt
	   ;
end;
-- 945

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with pt as (select distinct lbpt.pt from scratch_oct2022_np_vocab.lb_to_common_name_and_pt lbpt)
	   select nextval('napdi_concept_sequence'), pt.pt, 'NaPDI research', 'NAPDI', 'NaPDI Preferred Term', '', pt.pt, 
										'2000-01-01', '2099-02-22', ''
	   from pt
	   ;
end;
-- 283

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with distinct_lbs as (select distinct lbpt.latin_binomial, lbpt.pt, lbpt.max from scratch_oct2022_np_vocab.lb_to_common_name_and_pt lbpt)
  	     select nextval('napdi_concept_sequence'), concat(distinct_lbs.latin_binomial, '[', distinct_lbs.latin_binomial, ']'), 'NaPDI research', 'NAPDI', distinct_lbs.pt, '', distinct_lbs.max, 
										'2000-01-01', '2099-02-22', ''
	     from distinct_lbs
	     where concat(distinct_lbs.latin_binomial, '[', distinct_lbs.latin_binomial, ']') not in (select c.concept_name from staging_vocabulary.concept c where c.vocabulary_id = 'NAPDI')
	   ;
end;
-- 287

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with distinct_const as (select distinct lbconst.constituent_name, lbconst.concept_code from scratch_oct2022_np_vocab.lb_to_constituent lbconst)
	   select nextval('napdi_concept_sequence'), distinct_const.constituent_name, 'NaPDI research', 'NAPDI', 'NaPDI NP Constituent', '', distinct_const.concept_code, 
										'2000-01-01', '2099-02-22', ''
	   from distinct_const
	   ;
end;
-- 702



START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), left(lbspell.spelling_variation, 255), 'NaPDI research', 'NAPDI', 'NaPDI NP Spelling Variation', '', lbspell.pt, 
										'2000-01-01', '2099-02-22', ''
	   from scratch_oct2022_np_vocab.lb_to_spelling_var lbspell
	   ;
end;
-- 2772


/*
relationship and concept_relationship: 
 -- from scratch_oct2022_np_vocab.lb_to_common_name_and_pt to napdi_pt/napdi_is_pt_of
 -- from scratch_oct2022_np_vocab.lb_to_constituent to napdi_has_const/napdi_is_const_of 
 -- from scratch_oct2022_np_vocab.lb_to_spelling_var to napdi_spell_vr/napdi_is_spell_vr_of
*/
start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        with distinct_cn as (select distinct common_name, latin_binomial, pt from scratch_oct2022_np_vocab.lb_to_common_name_and_pt)
		select c1.concept_id,  c2.concept_id,  'napdi_pt', '2000-01-01', '2099-02-22', ''
        from distinct_cn lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.common_name, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 945

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        with distinct_cn as (select distinct common_name, latin_binomial, pt from scratch_oct2022_np_vocab.lb_to_common_name_and_pt)
		select c2.concept_id, c1.concept_id, 'napdi_is_pt_of', '2000-01-01', '2099-02-22', ''
        from distinct_cn lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.common_name, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 945

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
		with distinct_lb as (select distinct latin_binomial, pt from scratch_oct2022_np_vocab.lb_to_common_name_and_pt)
		select c1.concept_id,  c2.concept_id,  'napdi_pt', '2000-01-01', '2099-02-22', ''
        from distinct_lb lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.latin_binomial, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
--  290

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
		with distinct_lb as (select distinct latin_binomial, pt from scratch_oct2022_np_vocab.lb_to_common_name_and_pt)        
		select c2.concept_id, c1.concept_id, 'napdi_is_pt_of', '2000-01-01', '2099-02-22', ''
        from distinct_lb lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.latin_binomial, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
--  290


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        select c1.concept_id, c2.concept_id, 'napdi_has_const', '2000-01-01', '2099-02-22', ''
        from scratch_oct2022_np_vocab.lb_to_constituent lbconst 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbconst.related_latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on lbconst.constituent_name  = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id != 'NaPDI NP Spelling Variation' 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Constituent'
        ;
end;
-- 4788


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        select c2.concept_id, c1.concept_id,  'napdi_is_const_of', '2000-01-01', '2099-02-22', ''
        from scratch_oct2022_np_vocab.lb_to_constituent lbconst 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbconst.related_latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on lbconst.constituent_name  = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id != 'NaPDI NP Spelling Variation' 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Constituent'
        ;
end;
-- 4788

-- (TODO) -- modify to use the merged mappings with the 2022 findings
start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c1.concept_id, c2.concept_id, 'napdi_spell_vr', '2000-01-01', '2099-02-22', ''
        from scratch_oct2022_np_vocab.lb_to_spelling_var lbspell 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbspell.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on left(lbspell.spelling_variation, 255) = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Constituent')
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Spelling Variation'        
        ;
end;
-- 10805

-- (TODO) -- modify to use the merged mappings with the 2022 findings
start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c2.concept_id, c1.concept_id,  'napdi_is_spell_vr_of', '2000-01-01', '2099-02-22', ''
        from scratch_oct2022_np_vocab.lb_to_spelling_var lbspell 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbspell.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on left(lbspell.spelling_variation, 255) = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Constituent')
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Spelling Variation'        
        ;
end;
-- 10805

--
----- RxNorm NP mappings -------------------
--
drop table if exists scratch_oct2022_np_vocab.np_to_rxnorm;
create table if not exists scratch_oct2022_np_vocab.np_to_rxnorm (
concept_id_rxnorm int4 not null,
concept_id_napdi int4 not null,
concept_name_rxnorm varchar,
concept_name_napdi varchar,
concept_class_id_rxnorm varchar,
concept_code varchar,
valid_start_date date,
valid_end_date date
);

drop table if exists scratch_oct2022_np_vocab.np_const_to_rxnorm;
create table if not exists scratch_oct2022_np_vocab.np_const_to_rxnorm (
concept_id_rxnorm int4 not null,
concept_id_napdi int4 not null,
concept_name_rxnorm varchar,
concept_name_napdi varchar,
concept_class_id_rxnorm varchar,
concept_code varchar,
valid_start_date date,
valid_end_date date
);

drop table if exists scratch_oct2022_np_vocab.np_rxnorm_substring_temp;
create table if not exists scratch_oct2022_np_vocab.np_rxnorm_substring_temp (
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

-- query to find RxNorm concepts - exact matches constituents
with nps as ( 
 select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
 concept_class_id np_concept_class_id
 from staging_vocabulary.concept c 
 where c.vocabulary_id ='NAPDI'
) 
insert into scratch_oct2022_np_vocab.np_const_to_rxnorm
  (concept_id_rxnorm, concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
   concept_code, valid_start_date, valid_end_date)
select c2.concept_id concept_id_rxnorm, c2.concept_name concept_name_rxnorm, nps.concept_id_napdi, 
	   nps.concept_name_napdi, c2.concept_class_id concept_class_id_rxnorm, nps.concept_code, 
	   c2.valid_start_date, c2.valid_end_date
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = upper(nps.concept_name_napdi)
where vocabulary_id ='RxNorm' and nps.np_concept_class_id = 'NaPDI NP Constituent'
;
-- 122

--rxnorm np name matches
with nps as ( 
 select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
 concept_class_id np_concept_class_id
 from staging_vocabulary.concept c 
 where c.vocabulary_id ='NAPDI'
) 
insert into scratch_oct2022_np_vocab.np_to_rxnorm 
(concept_id_rxnorm, concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
 concept_code, valid_start_date, valid_end_date)
select c2.concept_id concept_id_rxnorm, c2.concept_name concept_name_rxnorm, nps.concept_id_napdi, 
	   nps.concept_name_napdi, c2.concept_class_id concept_class_id_rxnorm, nps.concept_code, 
       c2.valid_start_date, c2.valid_end_date
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = upper(nps.concept_name_napdi)
where vocabulary_id ='RxNorm' and nps.np_concept_class_id != 'NaPDI NP Constituent'
;
-- 14


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
-- 20312

-- Get just the NP mappings
-- truncate table scratch_oct2022_np_vocab.np_to_rxnorm; 
insert into scratch_oct2022_np_vocab.np_to_rxnorm (concept_id_rxnorm, concept_id_napdi, concept_name_rxnorm,
	  concept_name_napdi, concept_class_id_rxnorm, concept_code, valid_start_date, valid_end_date)
select npx.concept_id_rxnorm, npx.concept_id_napdi, npx.concept_name_rxnorm, npx.concept_name_napdi,
	   npx.concept_class_id_rxnorm, npx.concept_code, npx.valid_start_date, npx.valid_end_date 
from scratch_oct2022_np_vocab.np_rxnorm_substring_temp npx
where npx.concept_class_id_napdi = 'NaPDI Preferred Term'
-- 2265

-

-- After manual review, these were issues that needed fixed
/*
 * Deletions
19102962	-7001929	EXCEDRIN QUICK TAB SPEARMINT	SPEARMINT	Brand Name	Spearmint	1970-01-01	2018-08-05
19048945	-7001718	TAO BRAND OF TROLEANDOMYCIN	TAO	Brand Name	Tao	1970-01-01	2099-12-31
36229133	-7001718	TAO BRAND OF TROLEANDOMYCIN ORAL PRODUCT	TAO	Branded Dose Group	Tao	2016-08-01	2099-12-31
36229134	-7001718	TAO BRAND OF TROLEANDOMYCIN PILL	TAO	Branded Dose Group	Tao	2016-08-01	2099-12-31
*/
delete from scratch_oct2022_np_vocab.np_to_rxnorm np where np.concept_id_rxnorm in (19102962,19048945,36229133,36229134);

/*
 * Bugs
 Garlic appears to be missing (see below)
 
 * Additions
 ** Map to -7001825	Olive tree in addition to current mapping
792710	-7001888	OLIVE OIL / SOYBEAN OIL INJECTABLE PRODUCT	SOYBEAN	Clinical Dose Group	Soybean	2018-02-05	2099-12-31
792711	-7001888	OLIVE OIL / SOYBEAN OIL INJECTION	SOYBEAN	Clinical Drug Form	Soybean	2018-02-05	2099-12-31
792712	-7001888	1000 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION	SOYBEAN	Quant Clinical Drug	Soybean	2018-02-05	2099-12-31
792714	-7001888	OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML [CLINOLIPID]	SOYBEAN	Branded Drug Comp	Soybean	2018-02-05	2099-12-31
792715	-7001888	OLIVE OIL / SOYBEAN OIL INJECTION [CLINOLIPID]	SOYBEAN	Branded Drug Form	Soybean	2018-02-05	2099-12-31
792717	-7001888	1000 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION [CLINOLIPID]	SOYBEAN	Quant Branded Drug	Soybean	2018-02-05	2099-12-31
792718	-7001888	OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION	SOYBEAN	Clinical Drug	Soybean	2018-02-05	2099-12-31
792719	-7001888	OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION [CLINOLIPID]	SOYBEAN	Branded Drug	Soybean	2018-02-05	2099-12-31
792720	-7001888	500 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION	SOYBEAN	Quant Clinical Drug	Soybean	2018-02-05	2099-12-31
792721	-7001888	500 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION [CLINOLIPID]	SOYBEAN	Quant Branded Drug	Soybean	2018-02-05	2099-12-31
792722	-7001888	250 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION	SOYBEAN	Quant Clinical Drug	Soybean	2018-02-05	2099-12-31
792723	-7001888	250 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION [CLINOLIPID]	SOYBEAN	Quant Branded Drug	Soybean	2018-02-05	2099-12-31
792724	-7001888	100 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION	SOYBEAN	Quant Clinical Drug	Soybean	2018-02-05	2099-12-31
792725	-7001888	100 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION [CLINOLIPID]	SOYBEAN	Quant Branded Drug	Soybean	2018-02-05	2099-12-31
36029812	-7001888	OLIVE OIL / SOYBEAN OIL	SOYBEAN	Multiple Ingredients	Soybean	2017-10-27	2099-12-31

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001825, concept_name_rxnorm, upper('Olive tree'), concept_class_id_rxnorm, 'Olive tree', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (792710,792711,792712,792714,792715,792717,792718,792719,792720,792721,792722,792723,792724,792725,36029812)
;
-- 15


/*
** map to -7001910	Purplegranadilla (passion flower) in addition to current mapping
1306911	-7001686	PASSION FLOWER EXTRACT 500 MG / VALERIAN ROOT EXTRACT 500 MG ORAL CAPSULE	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
1307103	-7001686	KAVA PREPARATION 75 MG / PASSION FLOWER EXTRACT 15 MG / VALERIAN ROOT EXTRACT 200 MG ORAL CAPSULE	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
1307103	-7001680	KAVA PREPARATION 75 MG / PASSION FLOWER EXTRACT 15 MG / VALERIAN ROOT EXTRACT 200 MG ORAL CAPSULE	KAVA	Clinical Drug	Kava	1970-01-01	2019-02-03
1307185	-7001686	KAVA PREPARATION 75 MG / PASSION FLOWER EXTRACT 15 MG / VALERIAN EXTRACT 200 MG ORAL CAPSULE	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
1307185	-7001680	KAVA PREPARATION 75 MG / PASSION FLOWER EXTRACT 15 MG / VALERIAN EXTRACT 200 MG ORAL CAPSULE	KAVA	Clinical Drug	Kava	1970-01-01	2019-02-03
19070128	-7001686	KAVA PREPARATION 75 MG / PASSION FLOWER EXTRACT 30 MG / VALERIAN ROOT EXTRACT 200 MG ORAL CAPSULE	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
19070128	-7001680	KAVA PREPARATION 75 MG / PASSION FLOWER EXTRACT 30 MG / VALERIAN ROOT EXTRACT 200 MG ORAL CAPSULE	KAVA	Clinical Drug	Kava	1970-01-01	2019-02-03
19071260	-7001686	PASSION FLOWER EXTRACT 500 MG / VALERIAN ROOT 500 MG ORAL CAPSULE	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
36027321	-7001686	PASSION FLOWER EXTRACT / VALERIAN EXTRACT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36028080	-7001686	PASSION FLOWER EXTRACT / VALERIAN ROOT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36028254	-7001686	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN EXTRACT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36028254	-7001680	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN EXTRACT	KAVA	Multiple Ingredients	Kava	2010-08-30	2099-12-31
36029023	-7001686	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36029023	-7001680	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT	KAVA	Multiple Ingredients	Kava	2010-08-30	2099-12-31
36218979	-7001686	PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT ORAL PRODUCT	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
36218980	-7001686	PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT PILL	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
36225121	-7001686	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT ORAL PRODUCT	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
36225121	-7001680	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT ORAL PRODUCT	KAVA	Clinical Dose Group	Kava	2016-08-01	2019-02-03
36225122	-7001686	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT PILL	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
36225122	-7001680	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT PILL	KAVA	Clinical Dose Group	Kava	2016-08-01	2019-02-03
40053669	-7001686	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT ORAL CAPSULE	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
40053669	-7001680	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT ORAL CAPSULE	KAVA	Clinical Drug Form	Kava	1970-01-01	2019-02-03
40053670	-7001686	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT ORAL CAPSULE	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
40053670	-7001680	KAVA PREPARATION / PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT ORAL CAPSULE	KAVA	Clinical Drug Form	Kava	1970-01-01	2019-02-03
40071331	-7001686	PASSION FLOWER EXTRACT / VALERIAN ROOT EXTRACT ORAL CAPSULE	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
40071332	-7001686	PASSION FLOWER EXTRACT / VALERIAN EXTRACT ORAL CAPSULE	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
19023600	-7001686	PASSIFLORA INCARNATA EXTRACT 450 MG / VALERIAN ROOT EXTRACT 100 MG ORAL TABLET	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-03-31
36218977	-7001686	PASSIFLORA INCARNATA EXTRACT / VALERIAN ROOT EXTRACT ORAL PRODUCT	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-03-31
36218978	-7001686	PASSIFLORA INCARNATA EXTRACT / VALERIAN ROOT EXTRACT PILL	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-03-31
40071320	-7001686	PASSIFLORA INCARNATA EXTRACT / VALERIAN ROOT EXTRACT ORAL TABLET	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-03-31

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001910, concept_name_rxnorm, upper('Purplegranadilla'), concept_class_id_rxnorm, 'Purplegranadilla', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (1306911,1307103,1307103,1307185,1307185,19070128,19070128,19071260,36027321,36028080,36028254,36028254,36029023,36029023,36218979,36218980,36225121,36225121,36225122,36225122,40053669,40053669,40053670,40053670,40071331,40071332,19023600,36218977,36218978,40071320)
;
-- 30


/*

 ** map to -7001776	St. John's-wort in addition to current mapping
993684	-7001813	BLACK COHOSH EXTRACT 20 MG / MAGNESIUM OXIDE 100 MG / ST. JOHN'S WORT EXTRACT 300 MG ORAL TABLET	BLACK COHOSH	Clinical Drug	Black cohosh	1970-01-01	2019-02-03
1398077	-7001681	SIBERIAN GINSENG ROOT 90 MG / ST. JOHN'S WORT EXTRACT 450 MG ORAL TABLET	SIBERIAN GINSENG	Clinical Drug	Siberian ginseng	1970-01-01	2019-02-03
36027078	-7001681	SIBERIAN GINSENG ROOT / ST. JOHN'S WORT EXTRACT	SIBERIAN GINSENG	Multiple Ingredients	Siberian ginseng	2010-08-30	2099-12-31
36028679	-7001813	BLACK COHOSH EXTRACT / MAGNESIUM OXIDE / ST. JOHN'S WORT EXTRACT	BLACK COHOSH	Multiple Ingredients	Black cohosh	2010-08-30	2099-12-31
36214876	-7001681	SIBERIAN GINSENG ROOT / ST. JOHN'S WORT EXTRACT ORAL PRODUCT	SIBERIAN GINSENG	Clinical Dose Group	Siberian ginseng	2016-08-01	2019-02-03
36214877	-7001681	SIBERIAN GINSENG ROOT / ST. JOHN'S WORT EXTRACT PILL	SIBERIAN GINSENG	Clinical Dose Group	Siberian ginseng	2016-08-01	2019-02-03
36215813	-7001813	BLACK COHOSH EXTRACT / MAGNESIUM OXIDE / ST. JOHN'S WORT EXTRACT ORAL PRODUCT	BLACK COHOSH	Clinical Dose Group	Black cohosh	2016-08-01	2019-02-03
36215814	-7001813	BLACK COHOSH EXTRACT / MAGNESIUM OXIDE / ST. JOHN'S WORT EXTRACT PILL	BLACK COHOSH	Clinical Dose Group	Black cohosh	2016-08-01	2019-02-03
40015404	-7001813	BLACK COHOSH EXTRACT / MAGNESIUM OXIDE / ST. JOHN'S WORT EXTRACT ORAL TABLET	BLACK COHOSH	Clinical Drug Form	Black cohosh	1970-01-01	2019-02-03
40081462	-7001681	SIBERIAN GINSENG ROOT / ST. JOHN'S WORT EXTRACT ORAL TABLET	SIBERIAN GINSENG	Clinical Drug Form	Siberian ginseng	1970-01-01	2019-02-03

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, , concept_name_rxnorm, upper(''), concept_class_id_rxnorm, '', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in ()
;

/*

** map to -7001879	Yellow root (goldenseal) in addition to current mapping
1306782	-7001709	ECHINACEA PREPARATION 200 MG / GOLDENSEAL EXTRACT 200 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1306783	-7001709	ECHINACEA PREPARATION 450 MG / GOLDENSEAL EXTRACT 450 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1306784	-7001709	ECHINACEA PREPARATION 740 MG / GOLDENSEAL EXTRACT 90 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1306785	-7001709	ECHINACEA ANGUSTIFOLIA EXTRACT 75 MG / GOLDENSEAL EXTRACT 100 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1306786	-7001709	ECHINACEA PURPUREA EXTRACT 350 MG / GOLDENSEAL EXTRACT 100 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1306817	-7001709	ECHINACEA PREPARATION 112 MG / GOLDENSEAL EXTRACT 25 MG ORAL TABLET	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1389147	-7001709	ECHINACEA PREPARATION 180 MG / GOLDENSEAL EXTRACT 45 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1389148	-7001709	ECHINACEA PREPARATION 450 MG / GOLDENSEAL EXTRACT 50 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1389149	-7001709	ECHINACEA PREPARATION 700 MG / GOLDENSEAL EXTRACT 75 MG ORAL TABLET	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1389174	-7001709	ECHINACEA PREPARATION 75 MG / GOLDEN SEAL ROOT EXTRACT 100 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1389229	-7001709	ECHINACEA PREPARATION 350 MG / GOLDENSEAL EXTRACT 100 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1389230	-7001709	ECHINACEA PREPARATION 75 MG / GOLDENSEAL EXTRACT 100 MG ORAL TABLET	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1389257	-7001709	ECHINACEA PREPARATION 250 MG / GOLDENSEAL EXTRACT 200 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	2006-07-09	2019-02-03
1391200	-7001709	ECHINACEA ANGUSTIFOLIA EXTRACT 180 MG / GOLDENSEAL EXTRACT 45 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
1396834	-7001709	ECHINACEA ANGUSTIFOLIA ROOT EXTRACT 75 MG / GOLDEN SEAL ROOT 100 MG ORAL TABLET	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-03-31
1396835	-7001709	ECHINACEA PREPARATION 350 MG / GOLDEN SEAL ROOT 100 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-03-31
1396853	-7001709	ECHINACEA PURPUREA AERIAL PARTS EXTRACT 100 MG / GOLDEN SEAL ROOT 25 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
19066122	-7001709	ECHINACEA PREPARATION 350 MG / GOLDEN SEAL ROOT 50 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-03-31
19069255	-7001709	ECHINACEA ROOT EXTRACT 300 MG / GOLDEN SEAL ROOT 50 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
19069259	-7001709	ECHINACEA PREPARATION 100 MG / GOLDEN SEAL ROOT EXTRACT 100 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
19070031	-7001709	ASCORBIC ACID 30 MG / ECHINACEA PREPARATION 200 MG / GOLDEN SEAL ROOT 45 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-03-31
19112827	-7001709	ECHINACEA PURPUREA EXTRACT 75 MG / GOLDEN SEAL EXTRACT 100 MG ORAL TABLET	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
19121524	-7001709	24 HR ECHINACEA PREPARATION 8.33 MG/HR / GOLDENSEAL EXTRACT 4.17 MG/HR TRANSDERMAL PATCH	ECHINACEA	Quant Clinical Drug	Echinacea	2005-11-13	2016-06-05
19124772	-7001709	ECHINACEA ROOT EXTRACT 25 MG / GOLDEN SEAL ROOT 113 MG ORAL TABLET	ECHINACEA	Clinical Drug	Echinacea	2006-11-19	2019-02-03
19126417	-7001709	ECHINACEA PREPARATION 75 MG / GOLDENSEAL EXTRACT 75 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	2007-05-27	2019-02-03
19133338	-7001709	ECHINACEA PREPARATION 255 MG / GOLDENSEAL EXTRACT 50 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	2009-02-01	2019-02-03
36027085	-7001709	ASCORBIC ACID / ECHINACEA PREPARATION / GOLDEN SEAL ROOT	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36027169	-7001709	ECHINACEA PURPUREA AERIAL PARTS EXTRACT / GOLDEN SEAL ROOT	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36027275	-7001709	ECHINACEA ANGUSTIFOLIA EXTRACT / GOLDENSEAL PREPARATION	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36027284	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT EXTRACT	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36027514	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36027718	-7001709	ECHINACEA PREPARATION / GOLDENSEAL PREPARATION	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36027758	-7001709	ECHINACEA ROOT EXTRACT / GOLDEN SEAL ROOT	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36027944	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDEN SEAL EXTRACT	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36028234	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDENSEAL PREPARATION	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36028685	-7001709	ECHINACEA ANGUSTIFOLIA ROOT / GOLDEN SEAL ROOT	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
36217661	-7001709	ASCORBIC ACID / ECHINACEA PREPARATION / GOLDEN SEAL ROOT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-03-31
36217662	-7001709	ASCORBIC ACID / ECHINACEA PREPARATION / GOLDEN SEAL ROOT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-03-31
36226668	-7001709	ECHINACEA PREPARATION / GOLDENSEAL EXTRACT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36226674	-7001709	ECHINACEA PURPUREA AERIAL PARTS EXTRACT / GOLDEN SEAL ROOT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36226675	-7001709	ECHINACEA PURPUREA AERIAL PARTS EXTRACT / GOLDEN SEAL ROOT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36226680	-7001709	ECHINACEA ROOT EXTRACT / GOLDEN SEAL ROOT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36226681	-7001709	ECHINACEA ROOT EXTRACT / GOLDEN SEAL ROOT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227134	-7001709	ECHINACEA ANGUSTIFOLIA EXTRACT / GOLDENSEAL EXTRACT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227135	-7001709	ECHINACEA ANGUSTIFOLIA EXTRACT / GOLDENSEAL EXTRACT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227148	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT EXTRACT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227149	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT EXTRACT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227150	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-03-31
36227151	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-03-31
36227152	-7001709	ECHINACEA PREPARATION / GOLDENSEAL EXTRACT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227501	-7001709	ECHINACEA ANGUSTIFOLIA ROOT EXTRACT / GOLDEN SEAL ROOT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-03-31
36227502	-7001709	ECHINACEA ANGUSTIFOLIA ROOT EXTRACT / GOLDEN SEAL ROOT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-03-31
36227506	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDEN SEAL EXTRACT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227507	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDEN SEAL EXTRACT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227508	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDENSEAL EXTRACT ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
36227509	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDENSEAL EXTRACT PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
40012513	-7001709	ASCORBIC ACID / ECHINACEA PREPARATION / GOLDEN SEAL ROOT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-03-31
40034106	-7001709	ECHINACEA ANGUSTIFOLIA EXTRACT / GOLDENSEAL EXTRACT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40034122	-7001709	ECHINACEA ANGUSTIFOLIA ROOT EXTRACT / GOLDEN SEAL ROOT ORAL TABLET	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-03-31
40034358	-7001709	ECHINACEA ROOT EXTRACT / GOLDEN SEAL ROOT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40034745	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT EXTRACT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40034746	-7001709	ECHINACEA PREPARATION / GOLDEN SEAL ROOT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-03-31
40034747	-7001709	ECHINACEA PREPARATION / GOLDENSEAL EXTRACT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40034748	-7001709	ECHINACEA PREPARATION / GOLDENSEAL EXTRACT ORAL TABLET	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40035106	-7001709	ECHINACEA PURPUREA AERIAL PARTS EXTRACT / GOLDEN SEAL ROOT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40035117	-7001709	ECHINACEA EXTRACT / GOLDEN SEAL ROOT EXTRACT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40035121	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDEN SEAL EXTRACT ORAL TABLET	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40035122	-7001709	ECHINACEA PURPUREA EXTRACT / GOLDENSEAL EXTRACT ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
40125946	-7001709	ECHINACEA PREPARATION / GOLDENSEAL EXTRACT TRANSDERMAL PATCH	ECHINACEA	Clinical Drug Form	Echinacea	2005-11-13	2016-06-05
40135515	-7001709	ECHINACEA ROOT EXTRACT / GOLDEN SEAL ROOT ORAL TABLET	ECHINACEA	Clinical Drug Form	Echinacea	2006-11-19	2019-02-03
40240687	-7001709	ECHINACEA PURPUREA EXTRACT 250 MG / GOLDENSEAL EXTRACT 250 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	2011-07-31	2019-02-03
42903063	-7001709	ECHINACEA PREPARATION 8.33 MG/HR / GOLDENSEAL EXTRACT 4.17 MG/HR TRANSDERMAL PATCH	ECHINACEA	Clinical Drug	Echinacea	2012-12-03	2016-06-05

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001776, concept_name_rxnorm, upper('St. John''s-wort'), concept_class_id_rxnorm, 'St. John''s-wort', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (993684,1398077,36027078,36028679,36214876,36214877,36215813,36215814,40015404,40081462)
;

/*
 
  ** map to -7001873	Tang-kuei (angelica) in addition to current mapping
1391703	-7001813	ANGELICA SINENSIS PREPARATION 50 MG / BLACK COHOSH EXTRACT 75 MG ORAL CAPSULE	BLACK COHOSH	Clinical Drug	Black cohosh	1970-01-01	2019-02-03
36029055	-7001813	ANGELICA SINENSIS PREPARATION / BLACK COHOSH EXTRACT	BLACK COHOSH	Multiple Ingredients	Black cohosh	2010-08-30	2099-12-31
36217773	-7001813	ANGELICA SINENSIS PREPARATION / BLACK COHOSH EXTRACT ORAL PRODUCT	BLACK COHOSH	Clinical Dose Group	Black cohosh	2016-08-01	2019-02-03
36217774	-7001813	ANGELICA SINENSIS PREPARATION / BLACK COHOSH EXTRACT PILL	BLACK COHOSH	Clinical Dose Group	Black cohosh	2016-08-01	2019-02-03
40014098	-7001813	ANGELICA SINENSIS PREPARATION / BLACK COHOSH EXTRACT ORAL CAPSULE	BLACK COHOSH	Clinical Drug Form	Black cohosh	1970-01-01	2019-02-03

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001873, concept_name_rxnorm, upper('Tang-kuei'), concept_class_id_rxnorm, 'Tang-kuei', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (1391703,36029055,36217773,36217774,40014098)
;
-- 5

/*

  ** map to -7001735	Plantain in addition to current mapping
1355594	-7001735	ENGLISH PLANTAIN POLLEN EXTRACT / SHEEP SORREL POLLEN EXTRACT INJECTABLE PRODUCT	PLANTAIN	Clinical Dose Group	Plantain	2019-02-04	2099-12-31
1355595	-7001735	ENGLISH PLANTAIN POLLEN EXTRACT / SHEEP SORREL POLLEN EXTRACT INJECTABLE SOLUTION	PLANTAIN	Clinical Drug Form	Plantain	2019-02-04	2099-12-31
1355596	-7001735	ENGLISH PLANTAIN POLLEN EXTRACT 10000 UNT/ML / SHEEP SORREL POLLEN EXTRACT 10000 UNT/ML INJECTABLE SOLUTION	PLANTAIN	Clinical Drug	Plantain	2019-02-04	2099-12-31
1355597	-7001716	SHEEP SORREL POLLEN EXTRACT 10000 UNT/ML / YELLOW DOCK POLLEN EXTRACT 10000 UNT/ML INJECTABLE SOLUTION	YELLOW DOCK	Clinical Drug	Yellow dock	2019-02-04	2099-12-31
1355613	-7001735	ENGLISH PLANTAIN POLLEN EXTRACT 20000 UNT/ML / SHEEP SORREL POLLEN EXTRACT 20000 UNT/ML INJECTABLE SOLUTION	PLANTAIN	Clinical Drug	Plantain	2019-02-04	2099-12-31

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001735, concept_name_rxnorm, upper('Plantain'), concept_class_id_rxnorm, 'Plantain', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (1355594,1355595,1355596,1355597,1355613)
;
-- 5

/* TODO!!!
  ** map to [needs concept id] garlic  in addition to current mapping
1401492	-7001835	GARLIC PREPARATION 0.2 MG / PARSLEY EXTRACT 1 MG ORAL CAPSULE	PARSLEY	Clinical Drug	Parsley	1970-01-01	2019-02-03
1401493	-7001835	GARLIC PREPARATION 100 MG / PARSLEY EXTRACT 200 MG ORAL TABLET	PARSLEY	Clinical Drug	Parsley	1970-01-01	2019-02-03
1401494	-7001835	GARLIC PREPARATION 600 MG / PARSLEY EXTRACT 100 MG ORAL TABLET	PARSLEY	Clinical Drug	Parsley	1970-01-01	2019-02-03
1401495	-7001835	GARLIC PREPARATION 480 MG / PARSLEY EXTRACT 48 MG ORAL TABLET	PARSLEY	Clinical Drug	Parsley	1970-01-01	2019-02-03
1401496	-7001835	GARLIC PREPARATION 2 MG / PARSLEY EXTRACT 0.2 MG ORAL CAPSULE	PARSLEY	Clinical Drug	Parsley	1970-01-01	2019-02-03
19069445	-7001888	GARLIC PREPARATION 500 MG / SOYBEAN LECITHIN 500 MG ORAL CAPSULE	SOYBEAN	Clinical Drug	Soybean	1970-01-01	2019-03-31
19071074	-7001835	GARLIC PREPARATION 100 MG / PARSLEY EXTRACT 50 MG ORAL TABLET	PARSLEY	Clinical Drug	Parsley	1970-01-01	2019-02-03
19121421	-7001774	GARLIC PREPARATION 300 MG / GINKGO BILOBA EXTRACT 60 MG / GINSENG PREPARATION 100 MG ORAL TABLET	GINKGO	Clinical Drug	Ginkgo	2005-11-13	2019-03-31
36027617	-7001774	GARLIC PREPARATION / GINKGO BILOBA EXTRACT / GINSENG PREPARATION	GINKGO	Multiple Ingredients	Ginkgo	2010-08-30	2099-12-31
36027748	-7001835	GARLIC PREPARATION / PARSLEY EXTRACT	PARSLEY	Multiple Ingredients	Parsley	2010-08-30	2099-12-31
36029363	-7001835	CHLOROPHYLL / GARLIC PREPARATION / PARSLEY EXTRACT	PARSLEY	Multiple Ingredients	Parsley	2011-07-07	2099-12-31
36216504	-7001835	CHLOROPHYLL / GARLIC PREPARATION / PARSLEY EXTRACT ORAL PRODUCT	PARSLEY	Clinical Dose Group	Parsley	2016-08-01	2019-02-03
36216505	-7001835	CHLOROPHYLL / GARLIC PREPARATION / PARSLEY EXTRACT PILL	PARSLEY	Clinical Dose Group	Parsley	2016-08-01	2019-02-03
36222888	-7001774	GARLIC PREPARATION / GINKGO BILOBA EXTRACT / GINSENG PREPARATION ORAL PRODUCT	GINKGO	Clinical Dose Group	Ginkgo	2016-08-01	2019-03-31
36222889	-7001774	GARLIC PREPARATION / GINKGO BILOBA EXTRACT / GINSENG PREPARATION PILL	GINKGO	Clinical Dose Group	Ginkgo	2016-08-01	2019-03-31
36222896	-7001835	GARLIC PREPARATION / PARSLEY EXTRACT ORAL PRODUCT	PARSLEY	Clinical Dose Group	Parsley	2016-08-01	2019-02-03
36222897	-7001835	GARLIC PREPARATION / PARSLEY EXTRACT PILL	PARSLEY	Clinical Dose Group	Parsley	2016-08-01	2019-02-03
36222898	-7001888	GARLIC PREPARATION / SOYBEAN LECITHIN ORAL PRODUCT	SOYBEAN	Clinical Dose Group	Soybean	2016-08-01	2019-03-31
36222899	-7001888	GARLIC PREPARATION / SOYBEAN LECITHIN PILL	SOYBEAN	Clinical Dose Group	Soybean	2016-08-01	2019-03-31
40047458	-7001835	GARLIC PREPARATION / PARSLEY EXTRACT ORAL CAPSULE	PARSLEY	Clinical Drug Form	Parsley	1970-01-01	2019-02-03
40047459	-7001835	GARLIC PREPARATION / PARSLEY EXTRACT ORAL TABLET	PARSLEY	Clinical Drug Form	Parsley	1970-01-01	2019-02-03
40047460	-7001888	GARLIC PREPARATION / SOYBEAN LECITHIN ORAL CAPSULE	SOYBEAN	Clinical Drug Form	Soybean	1970-01-01	2019-03-31
40126306	-7001774	GARLIC PREPARATION / GINKGO BILOBA EXTRACT / GINSENG PREPARATION ORAL TABLET	GINKGO	Clinical Drug Form	Ginkgo	2005-11-13	2019-03-31
40240578	-7001835	CHLOROPHYLL / GARLIC PREPARATION / PARSLEY EXTRACT ORAL CAPSULE	PARSLEY	Clinical Drug Form	Parsley	2011-07-31	2019-02-03
40240580	-7001835	CHLOROPHYLL 0.028 MG / GARLIC PREPARATION 500 MG / PARSLEY EXTRACT 100 MG ORAL CAPSULE	PARSLEY	Clinical Drug	Parsley	2011-07-31	2019-02-03
  
  
  */

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, , concept_name_rxnorm, upper(''), concept_class_id_rxnorm, '', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in ()
;

/*
  
  ** map to -7001798	Shrub-trefoil (Hops) in addition to current mapping
1398501	-7001686	HOPS EXTRACT 200 MG / SKULLCAP PREPARATION 30 MG / VALERIAN ROOT EXTRACT 150 MG ORAL CAPSULE	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
19066407	-7001686	HOPS EXTRACT 60 MG / VALERIAN ROOT EXTRACT 250 MG ORAL TABLET	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
19066975	-7001686	HOPS EXTRACT 60 MG / VALERIAN ROOT 250 MG ORAL TABLET	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
19099694	-7001686	HOPS EXTRACT 60 MG / VALERIAN EXTRACT 250 MG ORAL TABLET	VALERIAN	Clinical Drug	Valerian	1970-01-01	2019-02-03
36027168	-7001686	HOPS EXTRACT / VALERIAN EXTRACT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36027511	-7001686	HOPS EXTRACT / VALERIAN ROOT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36028222	-7001686	HOPS EXTRACT / SKULLCAP PREPARATION / VALERIAN EXTRACT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36029072	-7001686	HOPS EXTRACT / VALERIAN ROOT EXTRACT	VALERIAN	Multiple Ingredients	Valerian	2010-08-30	2099-12-31
36225115	-7001686	HOPS EXTRACT / SKULLCAP PREPARATION / VALERIAN ROOT EXTRACT ORAL PRODUCT	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
36225116	-7001686	HOPS EXTRACT / SKULLCAP PREPARATION / VALERIAN ROOT EXTRACT PILL	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
36227461	-7001686	HOPS EXTRACT / VALERIAN ROOT EXTRACT ORAL PRODUCT	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
36227462	-7001686	HOPS EXTRACT / VALERIAN ROOT EXTRACT PILL	VALERIAN	Clinical Dose Group	Valerian	2016-08-01	2019-02-03
40043508	-7001686	HOPS EXTRACT / SKULLCAP PREPARATION / VALERIAN ROOT EXTRACT ORAL CAPSULE	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
40043509	-7001686	HOPS EXTRACT / VALERIAN ROOT EXTRACT ORAL TABLET	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
40043510	-7001686	HOPS EXTRACT / VALERIAN ROOT ORAL TABLET	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
40043511	-7001686	HOPS EXTRACT / VALERIAN ROOT EXTRACT ORAL TABLET	VALERIAN	Clinical Drug Form	Valerian	1970-01-01	2019-02-03
  
 */

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001798, concept_name_rxnorm, upper('Shrub-trefoil'), concept_class_id_rxnorm, 'Shrub-trefoil', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (1398501,19066407,19066975,19099694,36027168,36027511,36028222,36029072,36225115,36225116,36227461,36227462,40043508,40043509,40043510,40043511)
;
-- 16


/*  
  
  ** map to -7001849	Panax ginseng (asian ginseng) in addition to current mapping
19067004	-7001774	GINKGO BILOBA EXTRACT 60 MG / KOREAN GINSENG ROOT EXTRACT 100 MG ORAL CAPSULE	GINKGO	Clinical Drug	Ginkgo	1970-01-01	2019-02-03

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001849, concept_name_rxnorm, upper('Panax ginseng'), concept_class_id_rxnorm, 'Panax ginseng', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (19067004)
;
-- 1 

/*

  ** map to -7001830	Sicklepod senna in addition to current mapping
19070164	-7001836	FENNEL SEED PREPARATION 31 MG / SENNA LEAVES 325 MG / SENNOSIDES, USP 65 MG ORAL TABLET	FENNEL	Clinical Drug	Fennel	1970-01-01	2019-03-31
36027429	-7001836	FENNEL SEED PREPARATION / SENNA LEAVES / SENNA, USP	FENNEL	Multiple Ingredients	Fennel	2010-08-30	2099-12-31
36226151	-7001836	FENNEL SEED PREPARATION / SENNA LEAVES / SENNOSIDES, USP ORAL PRODUCT	FENNEL	Clinical Dose Group	Fennel	2016-08-01	2019-03-31
36226152	-7001836	FENNEL SEED PREPARATION / SENNA LEAVES / SENNOSIDES, USP PILL	FENNEL	Clinical Dose Group	Fennel	2016-08-01	2019-03-31
40037628	-7001836	FENNEL SEED PREPARATION / SENNA LEAVES / SENNOSIDES, USP ORAL TABLET	FENNEL	Clinical Drug Form	Fennel	1970-01-01	2019-03-31

*/

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001830, concept_name_rxnorm, upper('Sicklepod senna'), concept_class_id_rxnorm, 'Sicklepod senna', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (19070164,36027429,36226151,36226152,40037628)
;
-- 5

/*

  ** map to -7001787	Yohimbe in addition to current mapping
19121317	-7001774	ARGININE 25 MG / GINKGO BILOBA EXTRACT 25 MG / YOHIMBINE 500 MG ORAL CAPSULE	GINKGO	Clinical Drug	Ginkgo	2005-11-13	2019-03-31
36213139	-7001774	ARGININE / GINKGO BILOBA EXTRACT / YOHIMBINE ORAL PRODUCT	GINKGO	Clinical Dose Group	Ginkgo	2016-08-01	2019-03-31
36213140	-7001774	ARGININE / GINKGO BILOBA EXTRACT / YOHIMBINE PILL	GINKGO	Clinical Dose Group	Ginkgo	2016-08-01	2019-03-31
40125111	-7001774	ARGININE / GINKGO BILOBA EXTRACT / YOHIMBINE ORAL CAPSULE	GINKGO	Clinical Drug Form	Ginkgo	2005-11-13	2019-03-31
 
 */

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001787, concept_name_rxnorm, upper('Yohimbe'), concept_class_id_rxnorm, 'Yohimbe', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (19121317,36213139,36213140,40125111)
;
-- 4

/*
 
  ** map to -7001929	Spearmint in addition to current mapping
19080588	-7001837	BENZOCAINE 0.325 MG/MG / LICORICE 0.812 MG/MG / MENTHOL 0.000325 MG/MG ORAL LOZENGE	LICORICE	Clinical Drug	Licorice	1970-01-01	2099-12-31
 
 */

insert into scratch_oct2022_np_vocab.np_to_rxnorm
select concept_id_rxnorm, -7001929, concept_name_rxnorm, upper('Spearmint'), concept_class_id_rxnorm, 'Spearmint', valid_start_date, valid_end_date 
from scratch_oct2022_np_vocab.np_to_rxnorm np
where concept_id_rxnorm in (19080588)
;
-- 1

/*
   
   ** change mapping to -7001849	Panax ginseng
19070349	-7001681	KOREAN GINSENG PREPARATION 300 MG / SIBERIAN GINSENG PREPARATION 450 MG / VITAMIN B 12 0.3 MG ORAL CAPSULE	SIBERIAN GINSENG	Clinical Drug	Siberian ginseng	1970-01-01	2019-03-31
 
*/

update scratch_oct2022_np_vocab.np_to_rxnorm
set concept_id_napdi = -7001849, concept_name_napdi = 'PANAX GINSENG', concept_code = 'Panax ginseng'
where concept_id_rxnorm = 19070349
;
-- 1


insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
		valid_start_date, valid_end_date, invalid_reason)
select concept_id_napdi, concept_id_rxnorm,  'napdi_np_maps_to', valid_start_date, valid_end_date, ''
from scratch_oct2022_np_vocab.np_to_rxnorm
;
-- 2353



-- NOTE: NOT RAN YET! -  constituent mappings need to review
-- 
-- Get just the constituent mappings
insert into scratch_oct2022_np_vocab.np_const_to_rxnorm (concept_id_rxnorm, concept_id_napdi, concept_name_rxnorm,
	  concept_name_napdi, concept_class_id_rxnorm, concept_code, valid_start_date, valid_end_date)
select npx.concept_id_rxnorm, npx.concept_id_napdi, npx.concept_name_rxnorm, npx.concept_name_napdi,
	   npx.concept_class_id_rxnorm, npx.concept_code, npx.valid_start_date, npx.valid_end_date 
from scratch_oct2022_np_vocab.np_rxnorm_substring_temp npx
where npx.concept_class_id_napdi = 'NaPDI NP Constituent'
-- 10749

--N=11431 (exact+substring match of constituents)
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
		valid_start_date, valid_end_date, invalid_reason)
select concept_id_napdi, concept_id_rxnorm,  'napdi_const_maps_to', valid_start_date, valid_end_date, ''
from scratch_oct2022_np_vocab.np_const_to_rxnorm
;



---------------------------------------------
----------------- SCRATCH -------------------
---------------------------------------------

-- Test using Kratom 
select * from staging_vocabulary.concept c where concept_class_id = 'Kratom';
/*
-7001276	Kratom[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		dac1ac7a-f1bb-42d7-ab9c-0bf06d0d9825	2000-01-01	2099-02-22	
-7001277	Kratum[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		dac1ac7a-f1bb-42d7-ab9c-0bf06d0d9825	2000-01-01	2099-02-22	
-7002103	Mitragyna speciosa[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		dac1ac7a-f1bb-42d7-ab9c-0bf06d0d9825	2000-01-01	2099-02-22	
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7001276'
;
/*
Kratom[Mitragyna speciosa]	-7001276	Kratom	-7001796
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
   and c1.concept_id = '-7001796'
;
/*
Kratom	-7001796	Mitragyna speciosa[Mitragyna speciosa]	-7002103
Kratom	-7001796	Kratum[Mitragyna speciosa]	-7001277
Kratom	-7001796	Kratom[Mitragyna speciosa]	-7001276
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7001277'
;
/*
Kratum[Mitragyna speciosa]	-7001277	SPECIOGYNINE	-7002890
Kratum[Mitragyna speciosa]	-7001277	SPECIOCILIATIN	-7002646
Kratum[Mitragyna speciosa]	-7001277	PAYNANTHEINE	-7002624
Kratum[Mitragyna speciosa]	-7001277	MITRAGYNINE	-7002474
Kratum[Mitragyna speciosa]	-7001277	7-HYDROXYMITRAGYNINE	-7002460
Kratum[Mitragyna speciosa]	-7001277	MITRAPHYLLINE	-7002227
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7001796'
;
/*
Kratom	-7001796	7-HYDROXYMITRAGYNINE	-7002460
Kratom	-7001796	MITRAGYNINE	-7002474
Kratom	-7001796	MITRAPHYLLINE	-7002227
Kratom	-7001796	PAYNANTHEINE	-7002624
Kratom	-7001796	SPECIOCILIATIN	-7002646
Kratom	-7001796	SPECIOGYNINE	-7002890
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = '-7002474'
;
/*
MITRAGYNINE	-7002474	Mitragyna speciosa[Mitragyna speciosa]	-7002103
MITRAGYNINE	-7002474	Kratum[Mitragyna speciosa]	-7001277
MITRAGYNINE	-7002474	Kratom[Mitragyna speciosa]	-7001276
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7002474'
;
/*
MITRAGYNINE	-7002474	Kratom	-7001796
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = '-7001276'
;
/*
Kratom[Mitragyna speciosa]	-7001276	WHOLE HERBS PREMIUM MAENG DA KRATOM	-7004544
Kratom[Mitragyna speciosa]	-7001276	WHITE MAENG DA KRATOM 250G	-7004543
Kratom[Mitragyna speciosa]	-7001276	WHITE MAENG DA HERBAL TEA KRATOM	-7004542
Kratom[Mitragyna speciosa]	-7001276	VIVA ZEN KRATOM	-7004541
Kratom[Mitragyna speciosa]	-7001276	VIVAZEN BOTANICALS MAENG DA KRATOM	-7004540
Kratom[Mitragyna speciosa]	-7001276	UNICORN DUST STRAIN OF KRATOM	-7004539
Kratom[Mitragyna speciosa]	-7001276	TRAIN WRECK KRATOM	-7004538
Kratom[Mitragyna speciosa]	-7001276	TAUNTON BAY SOAP COMPANY RED VEIN TEA-1LB. PACKAGE (KRATOM)	-7004537
Kratom[Mitragyna speciosa]	-7001276	SUPERIOR RED DRAGON KRATOM	-7004536
Kratom[Mitragyna speciosa]	-7001276	SUPER GREEN KRATOM POWDER	-7004535
Kratom[Mitragyna speciosa]	-7001276	SUPER GREEN HORN KRATOM	-7004534
Kratom[Mitragyna speciosa]	-7001276	SLOW-MO HIPPO KRATOM	-7004533
Kratom[Mitragyna speciosa]	-7001276	R.H. NATURAL PRODUCTS KRATOM	-7004532
Kratom[Mitragyna speciosa]	-7001276	RED VEIN MAENG DA (KRATOM)	-7004531
Kratom[Mitragyna speciosa]	-7001276	RED VEIN KRATOM	-7004530
Kratom[Mitragyna speciosa]	-7001276	RED VEIN BORNEO KRATOM	-7004529
Kratom[Mitragyna speciosa]	-7001276	RED THAI KRATOM	-7004528
Kratom[Mitragyna speciosa]	-7001276	RED MAENG DA KRATOM (MITRAGYNA SPECIOSA)	-7004527
Kratom[Mitragyna speciosa]	-7001276	RED DEVIL KRATOM WATER SOLUBLE CBD	-7004526
Kratom[Mitragyna speciosa]	-7001276	RED BORNEO KRATOM BUMBLE BEE	-7004525
Kratom[Mitragyna speciosa]	-7001276	RED BALI KRATOM	-7004524
Kratom[Mitragyna speciosa]	-7001276	RAW FORM ORGANICS MAENG DA 150 RUBY CAPSULES KRATOM	-7004523
Kratom[Mitragyna speciosa]	-7001276	PREMIUM RED MAENG DA KRATOM	-7004522
Kratom[Mitragyna speciosa]	-7001276	PREMIUM RED MAENG DA CRAZY KRATOM	-7004521
Kratom[Mitragyna speciosa]	-7001276	PREMIUM KRATOM PHOENIX RED VEIN BALI	-7004520
Kratom[Mitragyna speciosa]	-7001276	POWDERED KRATOM	-7004519
Kratom[Mitragyna speciosa]	-7001276	O.P.M.S. LIQUID KRATOM	-7004518
Kratom[Mitragyna speciosa]	-7001276	O.P.M.S. KRATOM	-7004517
Kratom[Mitragyna speciosa]	-7001276	NUTRIZONE KRATOM PAIN OUT MAENG DA	-7004516
Kratom[Mitragyna speciosa]	-7001276	NATURE'S REMEDY KRATOM	-7004515
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNINE KRATOM	-7004514
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNINE (KRATOM)	-7004513
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNA SPECIOSA (MITRAGYNINE)	-7004512
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNA SPECIOSA LEAF	-7004511
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME) (KRATOM)	-7004510
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME)	-7004509
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNA SPECIOSA KORTHALS	-7004508
Kratom[Mitragyna speciosa]	-7001276	MITRAGYNA SPECIOSA	-7004507
Kratom[Mitragyna speciosa]	-7001276	MAENG DA POWDER KRATOM HERBAL DIETARY SUPPLEMENT	-7004506
Kratom[Mitragyna speciosa]	-7001276	MAENG DA KRATOM MITROGYNA SPECIOSA	-7004505
Kratom[Mitragyna speciosa]	-7001276	MAENG DA KRATOM	-7004504
Kratom[Mitragyna speciosa]	-7001276	LYFT PREMIUM BALI KRATOM HERBAL SUPPLEMENT POWDER	-7004503
Kratom[Mitragyna speciosa]	-7001276	LUCKY SEVEN KRATOM	-7004502
Kratom[Mitragyna speciosa]	-7001276	KRAVE KRATOM	-7004501
Kratom[Mitragyna speciosa]	-7001276	KRAVE, BLUE MAGIC KRATOM	-7004500
Kratom[Mitragyna speciosa]	-7001276	KRATOM- USED SUPER GREEN, GREEN BALI, RED MAGNEA DA	-7004499
Kratom[Mitragyna speciosa]	-7001276	KRATOM SUPPLEMENT	-7004498
Kratom[Mitragyna speciosa]	-7001276	KRATOM - SPECIFICALLY ^GREEN MALAYSIAN^	-7004497
Kratom[Mitragyna speciosa]	-7001276	KRATOM SILVER THAI	-7004496
Kratom[Mitragyna speciosa]	-7001276	KRATOM RED DRAGON	-7004495
Kratom[Mitragyna speciosa]	-7001276	KRATOM POWDER	-7004494
Kratom[Mitragyna speciosa]	-7001276	KRATOM (MITRAGYNINE)	-7004493
Kratom[Mitragyna speciosa]	-7001276	KRATOM (MITRAGYNA SPECIOSA LEAF)	-7004492
Kratom[Mitragyna speciosa]	-7001276	KRATOM MITRAGYNA SPECIOSA	-7004491
Kratom[Mitragyna speciosa]	-7001276	KRATOM (MITRAGYNA SPECIOSA)	-7004490
Kratom[Mitragyna speciosa]	-7001276	KRATOM (MITRAGYNA) (MITRAGYNINE)	-7004489
Kratom[Mitragyna speciosa]	-7001276	KRATOM (MITRAGYNA)	-7004488
Kratom[Mitragyna speciosa]	-7001276	KRATOM MAGNA RED	-7004487
Kratom[Mitragyna speciosa]	-7001276	(KRATOM) KRAOMA.COM TRANQUIL KRAOMA	-7004486
Kratom[Mitragyna speciosa]	-7001276	KRATOM INDO	-7004485
Kratom[Mitragyna speciosa]	-7001276	KRATOM IN A UNMARKED BAG	-7004484
Kratom[Mitragyna speciosa]	-7001276	KRATOM HERBAL SUPPLEMENT	-7004483
Kratom[Mitragyna speciosa]	-7001276	KRATOM GREEN MAGNA DA	-7004482
Kratom[Mitragyna speciosa]	-7001276	KRATOM EXTRACT	-7004481
Kratom[Mitragyna speciosa]	-7001276	KRATOM ELEPHANT WHITE THAI	-7004480
Kratom[Mitragyna speciosa]	-7001276	KRATOM CAPSULES	-7004479
Kratom[Mitragyna speciosa]	-7001276	KRATOM 3 OZ.	-7004478
Kratom[Mitragyna speciosa]	-7001276	KRATOM	-7004477
Kratom[Mitragyna speciosa]	-7001276	KRAKEN KRATOM	-7004476
Kratom[Mitragyna speciosa]	-7001276	KRABOT KRATOM FINELY GROUND POWDER	-7004475
Kratom[Mitragyna speciosa]	-7001276	KLARITY KRATOM: MAENG DA CAPSULES	-7004474
Kratom[Mitragyna speciosa]	-7001276	INDO KRATOM	-7004473
Kratom[Mitragyna speciosa]	-7001276	HERBAL SUBSTANCE KRATOM	-7004472
Kratom[Mitragyna speciosa]	-7001276	HERBAL SALVATION KRATOM	-7004471
Kratom[Mitragyna speciosa]	-7001276	GREEN STRAIN TROPICAL KRATOM	-7004470
Kratom[Mitragyna speciosa]	-7001276	GREEN M BATIK AND RED BATIK KRATOM	-7004469
Kratom[Mitragyna speciosa]	-7001276	GREEN MALAY KRATOM	-7004468
Kratom[Mitragyna speciosa]	-7001276	GREEN BORNEO KRATOM	-7004467
Kratom[Mitragyna speciosa]	-7001276	FEELIN' GROOVY KRATOM	-7004466
Kratom[Mitragyna speciosa]	-7001276	EMERALD LEAF BALI KRATOM (HERBALSMITRAGYNINE)	-7004465
Kratom[Mitragyna speciosa]	-7001276	EMERALD KRATOM POWDER	-7004464
Kratom[Mitragyna speciosa]	-7001276	EARTH KRATOM ORGANIC RED MAENG DA	-7004463
Kratom[Mitragyna speciosa]	-7001276	CLUB 13 KRATOM MAENG DA RED 90GM	-7004462
Kratom[Mitragyna speciosa]	-7001276	CAROLINA KRATOM RED JONGKONG 100 GRAM POWDER	-7004461
Kratom[Mitragyna speciosa]	-7001276	CALCIUM KRATOMOS	-7004460
Kratom[Mitragyna speciosa]	-7001276	BRILLIANT ELIXIR, CHOCOLATE LOVER W/ KRATOM	-7004459
Kratom[Mitragyna speciosa]	-7001276	BLUE MAGIC KRAVE KRATOM	-7004458
Kratom[Mitragyna speciosa]	-7001276	BALI KRATOM	-7004457
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
   and c1.concept_id = '-7004457'
;
/*
BALI KRATOM	-7004457	Mitragyna speciosa[Mitragyna speciosa]	-7002103
BALI KRATOM	-7004457	Kratum[Mitragyna speciosa]	-7001277
BALI KRATOM	-7004457	Kratom[Mitragyna speciosa]	-7001276
 */

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = '-7001796'
;
/*
Same 88 as above
 */

----------------------------------------------------------------------------------------------------
-- Test using Cinnamon
select * from staging_vocabulary.concept c where concept_class_id = 'Cinnamon';
/*
 -7000940	Cassia[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon		9e19900a-c256-4161-a4cb-37967de32e4f	2000-01-01	2099-02-22	
-7000941	Cassia cinnamon[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon		9e19900a-c256-4161-a4cb-37967de32e4f	2000-01-01	2099-02-22	
-7000942	Chinese cinnamon[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon		9e19900a-c256-4161-a4cb-37967de32e4f	2000-01-01	2099-02-22	
-7000943	Chinese cinnamon tree[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon		9e19900a-c256-4161-a4cb-37967de32e4f	2000-01-01	2099-02-22	
-7000944	Cinnamon[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon		9e19900a-c256-4161-a4cb-37967de32e4f	2000-01-01	2099-02-22	
-7000945	Rou gui[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon		9e19900a-c256-4161-a4cb-37967de32e4f	2000-01-01	2099-02-22	
-7000946	Ceylon cinnamon[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon		9d42816a-df2d-41f8-a728-c6955510a842	2000-01-01	2099-02-22	
-7000947	Cinnamon[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon		9d42816a-df2d-41f8-a728-c6955510a842	2000-01-01	2099-02-22	
-7000948	True cinnamon[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon		9d42816a-df2d-41f8-a728-c6955510a842	2000-01-01	2099-02-22	
-7000949	Tvak[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon		9d42816a-df2d-41f8-a728-c6955510a842	2000-01-01	2099-02-22	
-7002167	Cinnamomum verum[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon		9d42816a-df2d-41f8-a728-c6955510a842	2000-01-01	2099-02-22	
-7002170	Cinnamomum cassia[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon		9e19900a-c256-4161-a4cb-37967de32e4f	2000-01-01	2099-02-22	
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7002170'
;
/*
Cinnamomum cassia[Cinnamomum cassia]	-7002170	Cinnamon	-7001659
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
   and c1.concept_id = '-7001659'
;
/*
Cinnamon	-7001659	Cinnamomum cassia[Cinnamomum cassia]	-7002170
Cinnamon	-7001659	Cinnamomum verum[Cinnamomum verum]	-7002167
Cinnamon	-7001659	Tvak[Cinnamomum verum]	-7000949
Cinnamon	-7001659	True cinnamon[Cinnamomum verum]	-7000948
Cinnamon	-7001659	Cinnamon[Cinnamomum verum]	-7000947
Cinnamon	-7001659	Ceylon cinnamon[Cinnamomum verum]	-7000946
Cinnamon	-7001659	Rou gui[Cinnamomum cassia]	-7000945
Cinnamon	-7001659	Cinnamon[Cinnamomum cassia]	-7000944
Cinnamon	-7001659	Chinese cinnamon tree[Cinnamomum cassia]	-7000943
Cinnamon	-7001659	Chinese cinnamon[Cinnamomum cassia]	-7000942
Cinnamon	-7001659	Cassia cinnamon[Cinnamomum cassia]	-7000941
Cinnamon	-7001659	Cassia[Cinnamomum cassia]	-7000940
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7000940'
;
/*
Cassia[Cinnamomum cassia]	-7000940	O-METHOXYCINNAMALDEHYDE, (E)-	-7002881
Cassia[Cinnamomum cassia]	-7000940	CHINESE CINNAMON OIL	-7002860
Cassia[Cinnamomum cassia]	-7000940	CINNAMTANNIN B2	-7002760
Cassia[Cinnamomum cassia]	-7000940	O-METHOXYCINNAMALDEHYDE	-7002716
Cassia[Cinnamomum cassia]	-7000940	CINNAMIC ACID	-7002694
Cassia[Cinnamomum cassia]	-7000940	METHYL EUGENOL	-7002668
Cassia[Cinnamomum cassia]	-7000940	SALICYLALDEHYDE	-7002564
Cassia[Cinnamomum cassia]	-7000940	CINNAMYL ACETATE	-7002463
Cassia[Cinnamomum cassia]	-7000940	CINNAMYL ALCOHOL	-7002404
Cassia[Cinnamomum cassia]	-7000940	EUGENOL	-7002300
Cassia[Cinnamomum cassia]	-7000940	CINNAMALDEHYDE	-7002254
Cassia[Cinnamomum cassia]	-7000940	COUMARIN	-7002250
Cassia[Cinnamomum cassia]	-7000940	2-METHOXYCINNAMIC ACID	-7002231
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7001659'
;
/*
Cinnamon	-7001659	2-METHOXYCINNAMIC ACID	-7002231
Cinnamon	-7001659	BENZALDEHYDE	-7002786
Cinnamon	-7001659	BENZYL BENZOATE	-7002661
Cinnamon	-7001659	CARYOPHYLLENE	-7002290
Cinnamon	-7001659	CHINESE CINNAMON OIL	-7002860
Cinnamon	-7001659	CINNAMALDEHYDE	-7002254
Cinnamon	-7001659	CINNAMIC ACID	-7002694
Cinnamon	-7001659	CINNAMTANNIN B1	-7002536
Cinnamon	-7001659	CINNAMTANNIN B2	-7002760
Cinnamon	-7001659	CINNAMYL ACETATE	-7002463
Cinnamon	-7001659	CINNAMYL ALCOHOL	-7002404
Cinnamon	-7001659	COUMARIN	-7002250
Cinnamon	-7001659	EUCALYPTOL	-7002349
Cinnamon	-7001659	EUGENOL	-7002300
Cinnamon	-7001659	LINALOOL, (+/-)-	-7002820
Cinnamon	-7001659	M-CYMENE	-7002317
Cinnamon	-7001659	METHYL EUGENOL	-7002668
Cinnamon	-7001659	O-CYMENE	-7002919
Cinnamon	-7001659	O-METHOXYCINNAMALDEHYDE	-7002716
Cinnamon	-7001659	O-METHOXYCINNAMALDEHYDE, (E)-	-7002881
Cinnamon	-7001659	P-CYMENE	-7002502
Cinnamon	-7001659	PHELLANDRENE	-7002618
Cinnamon	-7001659	PINENE	-7002552
Cinnamon	-7001659	SAFROLE	-7002909
Cinnamon	-7001659	SALICYLALDEHYDE	-7002564
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = '-7002564'
;
/*
SALICYLALDEHYDE	-7002564	Cinnamomum cassia[Cinnamomum cassia]	-7002170
SALICYLALDEHYDE	-7002564	Rou gui[Cinnamomum cassia]	-7000945
SALICYLALDEHYDE	-7002564	Cinnamon[Cinnamomum cassia]	-7000944
SALICYLALDEHYDE	-7002564	Chinese cinnamon tree[Cinnamomum cassia]	-7000943
SALICYLALDEHYDE	-7002564	Chinese cinnamon[Cinnamomum cassia]	-7000942
SALICYLALDEHYDE	-7002564	Cassia cinnamon[Cinnamomum cassia]	-7000941
SALICYLALDEHYDE	-7002564	Cassia[Cinnamomum cassia]	-7000940
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7002564'
;
/*
SALICYLALDEHYDE	-7002564	Cinnamon	-7001659

*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = '-7002167'
;
/*
Cinnamomum verum[Cinnamomum verum]	-7002167	SAIREITO /08000901/ (ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, ATRACTYLODES LANCEA RHIZOME, BUPLEURUM FALCATUM ROOT, CINNAMOMUM CASSIA BARK, GLYCYRRHIZA SPP. ROOT, PANAX GINSENG ROOT, PINELLIA TERNATA TUBER, POLYPORUS UMBELLATUS SCLEROTIUM, PORIA COC	-7005670
Cinnamomum verum[Cinnamomum verum]	-7002167	CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7005608
Cinnamomum verum[Cinnamomum verum]	-7002167	LOOSE LEAF TEA (YERBA MATE) W/DANDELION ROOT+TUMERIC+CINNAMON+OREGAN+THYME	-7005058
Cinnamomum verum[Cinnamomum verum]	-7002167	SAIREITO /08000901/ (ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, ATRACTYLODES LANCEA RHIZOME, BUPLEURUM FALCATUM ROOT, CINNAMOMUM CASSIA BARK, GLYCYRRHIZA SPP. ROOT, PANAX GINSENG ROOT, PINELLIA TERNATA TUBER, POLYPORUS UMBELLATUS SCLEROTIUM, PORIA COC	-7004727
Cinnamomum verum[Cinnamomum verum]	-7002167	WALGREENS BRAND CINNAMON CAPSULES	-7003667
Cinnamomum verum[Cinnamomum verum]	-7002167	VITAMIN E CINNAMON	-7003666
Cinnamomum verum[Cinnamomum verum]	-7002167	VITAMIN C-CINNAMON	-7003665
Cinnamomum verum[Cinnamomum verum]	-7002167	S.M. (CALCIUM CARBONATE, CINNAMOMUM VERUM POWDER, COPTIS TRIFOLIA, DIA	-7003664
Cinnamomum verum[Cinnamomum verum]	-7002167	SHOSEIRYUTO [ASARUM SPP. ROOT;CINNAMOMUM CASSIA BARK;EPHEDRA SPP. HERB	-7003663
Cinnamomum verum[Cinnamomum verum]	-7002167	SHOSEIRYUTO [ASARUM SPP. ROOT;CINNAMOMUM CASSIA BARK;EPHEDRA SPP.	-7003662
Cinnamomum verum[Cinnamomum verum]	-7002167	SAIREITO /08000901/ (ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, ATRACTYLODES LANCEA RHIZOME, BUPLEURUM FALCATUM ROOT, CINNAMOMUM CASSIA BARK, GLYCYRRHIZA SPP. ROOT, PANAX GINSENG ROOT, PINELLIA TERNATA TUBER, POLYPORUS UMBELLATUS SCLEROTIUM, PORIA COC	-7003661
Cinnamomum verum[Cinnamomum verum]	-7002167	RYOKEIJUTSUKANTO [ATRACTYLODES LANCEA RHIZOME;CINNAMOMUM CASSIA BARK;G	-7003660
Cinnamomum verum[Cinnamomum verum]	-7002167	PURITAN'S PRIDE CINNAMON WITH HIGH POTENCY CHROMIUM COMPLEX	-7003659
Cinnamomum verum[Cinnamomum verum]	-7002167	MISTURA CARMINATIVA (CARAWAY (+) CARDAMOM SEED (+) CINNAMON (+) GLYCER	-7003658
Cinnamomum verum[Cinnamomum verum]	-7002167	MENTHOL/THYMOL/SYZYGIUM AROMATICUM OIL/CINNAMOMUM VERUM BARK OIL/EUCALYPTUS GLOBULUS OIL/FOENICULUM	-7003657
Cinnamomum verum[Cinnamomum verum]	-7002167	MAOTO (CINNAMOMUM CASSIA BARK, EPHEDRA SPP. HERB, GLYCYRRHIZA SPP. ROOT, PRUNIUS SPP. SEED)	-7003656
Cinnamomum verum[Cinnamomum verum]	-7002167	LOOSE LEAF TEA (YERBA MATE) W/DANDELION ROOT+TUMERIC+CINNAMON+OREGAN+THYME	-7003655
Cinnamomum verum[Cinnamomum verum]	-7002167	KEISHIKASHAKUYAKUTO [CINNAMOMUM CASSIA BARK;GLYCYRRHIZA SPP. ROOT;PAEO	-7003654
Cinnamomum verum[Cinnamomum verum]	-7002167	KAKKONTOKASENKYUSHIN'I [CINNAMOMUM CASSIA BARK;CNIDIUM OFFICINALE RHIZ	-7003653
Cinnamomum verum[Cinnamomum verum]	-7002167	KAKKONTO [CINNAMOMUM CASSIA BARK;EPHEDRA SPP. HERB;GLYCYRRHIZA SPP. RO	-7003652
Cinnamomum verum[Cinnamomum verum]	-7002167	KAKKONTO [CINNAMOMUM CASSIA BARK;EPHEDRA SPP.	-7003651
Cinnamomum verum[Cinnamomum verum]	-7002167	KAIGEN (CAFFEINE, CINNAMON, GLYCYRRHIZA EXTRACT, METHYLEPHEDRINE HYDRO	-7003650
Cinnamomum verum[Cinnamomum verum]	-7002167	INNAMON (CINNAMON VERUM) CAPSULE	-7003649
Cinnamomum verum[Cinnamomum verum]	-7002167	HONEY + CINNAMON	-7003648
Cinnamomum verum[Cinnamomum verum]	-7002167	HERBAL SUPPLEMENT WITH CINNAMON	-7003647
Cinnamomum verum[Cinnamomum verum]	-7002167	HACHIMIJIOGAN (ACONITUM SPP. PROCESSED ROOT, ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, CINNAMOMUM CASSIA BARK, CORNUS OFFICINALIS FRUIT, DIOSCOREA SPP. RHIZOME, PAEONIA X SUFFRUTICOSA ROOT BARK, PORIA COCOS SCLEROTIUM, REHMANNIA GLUTINOSA ROOT)	-7003646
Cinnamomum verum[Cinnamomum verum]	-7002167	GOSHAJINKIGAN (ACHYRANTHES BIDENTATA ROOT, ACONITUM SPP. PROCESSED ROOT, ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, CINNAMOMUM CASSIA BARK, CORNUS OFFICINALIS FRUIT, DIOSCOREA SPP. RHIZOME, PAEONIA X SUFFRUTICOSA ROOT BARK, PLANTAGO ASIATICA SEED, POR	-7003645
Cinnamomum verum[Cinnamomum verum]	-7002167	GOREISAN /08015901/ (ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, ATRACTYLODES SPP. RHIZOME, CINNAMOMIUM CASSIA BARK, POLYPORUS UMBELLATUS SCLEROTIUM, PORIA COCOS SCLEROTIUM)	-7003644
Cinnamomum verum[Cinnamomum verum]	-7002167	^GOOD HERBS ORIGINAL^ TEA - ROOBOIS, ROSEHIPS, CHAMOMILE, CINNAMON, LE	-7003643
Cinnamomum verum[Cinnamomum verum]	-7002167	COQ10 WITH CINNAMON	-7003642
Cinnamomum verum[Cinnamomum verum]	-7002167	CO-Q 10 W/CINNAMON	-7003641
Cinnamomum verum[Cinnamomum verum]	-7002167	CONNAMON	-7003640
Cinnamomum verum[Cinnamomum verum]	-7002167	CNNAMON (CINNAMOMUM VERUM) (CINNAMOMUM VERUM)	-7003639
Cinnamomum verum[Cinnamomum verum]	-7002167	CNNAMON	-7003638
Cinnamomum verum[Cinnamomum verum]	-7002167	CINSULIN (CINNAMON AND CHROMIUM PICOLINATE)	-7003637
Cinnamomum verum[Cinnamomum verum]	-7002167	CINSULILN (CINNAMON, VITAMIN D3, 500IU)	-7003636
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNULIN PF (WATER CINNAMON EXTRACT)	-7003635
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNOMON	-7003634
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNNAMON	-7003633
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNMON	-7003632
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNIMOV	-7003631
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNIMON	-7003630
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNIMAN	-7003629
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNEMON	-7003628
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNEMIN	-7003627
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAOMIN	-7003626
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON WITH HONEY AND TEA COMBINATION	-7003625
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON WITH CHROMIUM (CINNAMOMUM VERUM)	-7003624
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON WITH CHROMIUM	-7003623
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON W/CHROMIUM	-7003622
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON W/ CHROMIUM	-7003621
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON VERUM	-7003620
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON TEA	-7003619
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON RED HOTS	-7003618
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON PLUS CHROMIUM METABOLISM SUPPORT	-7003617
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON PLUS CHROMIUM	-7003616
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON PILLS (CINNAMOMUM VERUM)	-7003615
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON GARLIC	-7003614
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON EXTRACT TABLETS	-7003613
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON EXTRACT [CINNAMOMUM CASSIA TWIG]	-7003612
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON EXTRACT [CINNAMOMUM BURMANNI]	-7003611
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON EXTRACT	-7003610
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON CR	-7003609
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON COMPLEX	-7003608
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CNINAMOMUM VERUM)	-7003607
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON CITRACAL + D CLOTRIMAZOLE	-7003606
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOUM VERUM) (CAPSULES)	-7003605
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMON VERUM)	-7003604
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOM VERUM)	-7003603
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOMUN VERUM) (CINNAMOMUM VERUM)	-7003602
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOMUN VERUM)	-7003601
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOMUM VERUM)(UNKNOWN)	-7003600
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOMUM VERUM) (UNKNOWN)	-7003599
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOMUM VERUM) TABLET, 500 MG	-7003598
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON(CINNAMOMUM VERUM)	-7003597
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINNAMOMUM VERUM)	-7003596
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON ( CINNAMOMUM VERUM)	-7003595
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON [CINNAMOMUM CASSIA BARK]	-7003594
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON (CINAMOMUM VERUM)	-7003593
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON/CHRONDROITIN	-7003592
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON/CHROMIUM	-7003591
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON CHROMIUM	-7003590
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON + CHROMIUM	-7003589
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON/CHROME	-7003588
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON+CHROM	-7003587
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON/CHRM	-7003586
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON CAPSULE (CINNAMOMUM VERUM) (CINNAMOMUM VERUM)	-7003585
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON BARK W/CHROMIMUN	-7003584
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON BARK EXTRACT	-7003583
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON BARK CASSIA	-7003582
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON AND HONEY	-7003581
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON AND GREEN TEA	-7003580
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON AND CHROMIUM	-7003579
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON 500 MG PLUS CHROMIUM METABOLISM SUPPORT	-7003578
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON 1000/PLUS CHROMIUM	-7003577
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON 1000 MG WITH CHROMIUM	-7003576
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON                           /05656704/	-7003575
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON                           /05656701/	-7003574
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON                           /01647506/	-7003573
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON /01647501/ (CINNAMOMUM VERUM)	-7003572
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON /01647501/	-7003571
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON                           /01647501/	-7003570
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM VERUM OIL	-7003569
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM VERUM (CINNAMOMUM VERUM)	-7003568
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM VERUM BARK	-7003567
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM VERUM	-7003566
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM SPP.	-7003565
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM CASSIA BARK, PAEONIA LACTIFLORA ROOT, PAEONIA X SUFFRUTICOS	-7003564
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM CASSIA BARK/EPHEDRA SPP. HERB/GLYCYRRHIZA SPP. ROOT/PAEONIA LACTIFLORA ROOT/PUERARIA LOBA	-7003563
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM CASSIA BARK	-7003562
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM CASSIA	-7003561
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOMUM CAMPHORA	-7003560
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMOM	-7003559
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMO	-7003558
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMMON	-7003557
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMINT	-7003556
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMIN	-7003555
Cinnamomum verum[Cinnamomum verum]	-7002167	CINMMON	-7003554
Cinnamomum verum[Cinnamomum verum]	-7002167	CINAMON	-7003553
Cinnamomum verum[Cinnamomum verum]	-7002167	CINAMMON	-7003552
Cinnamomum verum[Cinnamomum verum]	-7002167	CINAMMOMUM VERUM	-7003551
Cinnamomum verum[Cinnamomum verum]	-7002167	CINAMMN	-7003550
Cinnamomum verum[Cinnamomum verum]	-7002167	CINAMIN	-7003549
Cinnamomum verum[Cinnamomum verum]	-7002167	CINAIMON	-7003548
Cinnamomum verum[Cinnamomum verum]	-7002167	CIMNAMON	-7003547
Cinnamomum verum[Cinnamomum verum]	-7002167	CIMMAMON	-7003546
Cinnamomum verum[Cinnamomum verum]	-7002167	CHROMIUM W/CINNAMOMUM VERUM/ZINC	-7003545
Cinnamomum verum[Cinnamomum verum]	-7002167	CHROMIUM;CINNAMON	-7003544
Cinnamomum verum[Cinnamomum verum]	-7002167	CHROMIUM/CINNAMON	-7003543
Cinnamomum verum[Cinnamomum verum]	-7002167	CHROMIUM/CINNAMOMUM VERUM/ZINC	-7003542
Cinnamomum verum[Cinnamomum verum]	-7002167	CHROMIUM, CINNAMOMUM VERUM	-7003541
Cinnamomum verum[Cinnamomum verum]	-7002167	CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7003540
Cinnamomum verum[Cinnamomum verum]	-7002167	CASSIA CINNAMON	-7003539
Cinnamomum verum[Cinnamomum verum]	-7002167	CASSIA (CINNAMON)	-7003538
Cinnamomum verum[Cinnamomum verum]	-7002167	CASSIA (CINNAMOMUM VERUM) (CINNAMOMUM VERUM)	-7003537
Cinnamomum verum[Cinnamomum verum]	-7002167	CASSIA	-7003536
Cinnamomum verum[Cinnamomum verum]	-7002167	CABAGIN (CINNAMON, POWDERED, DIASTASE, LIQURICE, MENTHOL, NISIN, SCOPO	-7003535
Cinnamomum verum[Cinnamomum verum]	-7002167	BERBERTINE + CINNAMON BARK	-7003534
Cinnamomum verum[Cinnamomum verum]	-7002167	ATRACTYLODES LANCEA RHIZOME/ALISMA RHIZOME/POLYPORUS SCLEROTIUM/PORIA SCLEROTIUM/CINNAMON BARK	-7003533
Cinnamomum verum[Cinnamomum verum]	-7002167	ASARUM SPP. ROOT, CINNAMOMUM CASSIA BARK, EPHEDRA SPP. HERB, GLYCYRRHI	-7003532
Cinnamomum verum[Cinnamomum verum]	-7002167	ALPHA LIPSIS SOID.CINNAMON	-7003531
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON AND GREEN TEA	-7003244
Cinnamomum verum[Cinnamomum verum]	-7002167	CINNAMON GARLIC	-7003000
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
    and c1.concept_id = '-7003244'  
;
/*
CINNAMON AND GREEN TEA	-7003244	Cinnamomum verum[Cinnamomum verum]	-7002167
CINNAMON AND GREEN TEA	-7003244	Camellia sinensis[Camellia sinensis]	-7001977
CINNAMON AND GREEN TEA	-7003244	Tvak[Cinnamomum verum]	-7000949
CINNAMON AND GREEN TEA	-7003244	True cinnamon[Cinnamomum verum]	-7000948
CINNAMON AND GREEN TEA	-7003244	Cinnamon[Cinnamomum verum]	-7000947
CINNAMON AND GREEN TEA	-7003244	Ceylon cinnamon[Cinnamomum verum]	-7000946
CINNAMON AND GREEN TEA	-7003244	White Tea[Camellia sinensis]	-7000890
CINNAMON AND GREEN TEA	-7003244	Tea[Camellia sinensis]	-7000889
CINNAMON AND GREEN TEA	-7003244	Oolong tea[Camellia sinensis]	-7000888
CINNAMON AND GREEN TEA	-7003244	Green tea[Camellia sinensis]	-7000887
CINNAMON AND GREEN TEA	-7003244	Black tea[Camellia sinensis]	-7000886
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
    and c1.concept_id = '-7001659'
;

/*
Cinnamon	-7001659	cinnamon preparation	1307310
Cinnamon	-7001659	Cinnamon Preparation 500 MG	1307311
Cinnamon	-7001659	Cinnamon Preparation 500 MG Oral Capsule	1307352
Cinnamon	-7001659	cinnamon bark	1359143
Cinnamon	-7001659	cinnamon bark 500 MG Oral Capsule	19112600
Cinnamon	-7001659	cinnamon bark 500 MG	19112601
Cinnamon	-7001659	CHROMIUM PICOLINATE / Cinnamon Bark	36029533
Cinnamon	-7001659	cinnamon bark Oral Product	36216559
Cinnamon	-7001659	cinnamon bark Pill	36216560
Cinnamon	-7001659	Cinnamon Preparation Oral Product	36216561
Cinnamon	-7001659	Cinnamon Preparation Pill	36216562
Cinnamon	-7001659	cinnamon allergenic extract Injectable Product	36219824
Cinnamon	-7001659	chromium picolinate / cinnamon bark Oral Product	36246787
Cinnamon	-7001659	chromium picolinate / cinnamon bark Pill	36246788
Cinnamon	-7001659	cinnamon bark Oral Capsule	40028295
Cinnamon	-7001659	Cinnamon Preparation Oral Capsule	40133415
Cinnamon	-7001659	Cinnamon Preparation 500 MG Oral Tablet	40163080
Cinnamon	-7001659	Cinnamon Preparation Oral Tablet	40163081
Cinnamon	-7001659	cinnamon allergenic extract	40172100
Cinnamon	-7001659	cinnamon allergenic extract 50 MG/ML	40172101
Cinnamon	-7001659	cinnamon allergenic extract 50 MG/ML Injectable Solution	40172102
Cinnamon	-7001659	cinnamon allergenic extract Injectable Solution	40172103
Cinnamon	-7001659	cinnamon allergenic extract 100 MG/ML	40174181
Cinnamon	-7001659	cinnamon allergenic extract 100 MG/ML Injectable Solution	40174182
Cinnamon	-7001659	Chinese cinnamon leaf oil	42898601
Cinnamon	-7001659	Chinese cinnamon oil	42898727
Cinnamon	-7001659	Chinese cinnamon extract	42898728
Cinnamon	-7001659	cinnamon oil	42898867
Cinnamon	-7001659	cinnamon oil, bark	42900310
Cinnamon	-7001659	cinnamon oil, leaf	42900311
Cinnamon	-7001659	chromium picolinate / cinnamon bark Oral Capsule	42901036
Cinnamon	-7001659	chromium picolinate 0.1 MG / cinnamon bark 500 MG Oral Capsule	42901713
Cinnamon	-7001659	cinnamon bark 1000 MG	43525468
Cinnamon	-7001659	cinnamon bark 1000 MG Oral Capsule	43525717
 */

---------------------------------------


-- Test using Hemp extract
select * from staging_vocabulary.concept c where concept_class_id = 'Hemp extract';
/*
-7000891	Cannibidiol[Cannabis sativa]	NaPDI research	NAPDI	Hemp extract		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22	
-7000892	CBD[Cannabis sativa]	NaPDI research	NAPDI	Hemp extract		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22	
-7000893	Da ma[Cannabis sativa]	NaPDI research	NAPDI	Hemp extract		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22	
-7000894	Hemp[Cannabis sativa]	NaPDI research	NAPDI	Hemp extract		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22	
-7000895	Hemp extract[Cannabis sativa]	NaPDI research	NAPDI	Hemp extract		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22	
-7000896	Marijuana[Cannabis sativa]	NaPDI research	NAPDI	Hemp extract		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22	
-7002102	Cannabis sativa[Cannabis sativa]	NaPDI research	NAPDI	Hemp extract		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22		
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7000896'
;
/*
Marijuana[Cannabis sativa]	-7000896	Hemp extract	-7001676
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
   and c1.concept_id = '-7001676'
;
/*
Hemp extract	-7001676	Cannabis sativa[Cannabis sativa]	-7002102
Hemp extract	-7001676	Marijuana[Cannabis sativa]	-7000896
Hemp extract	-7001676	Hemp extract[Cannabis sativa]	-7000895
Hemp extract	-7001676	Hemp[Cannabis sativa]	-7000894
Hemp extract	-7001676	Da ma[Cannabis sativa]	-7000893
Hemp extract	-7001676	CBD[Cannabis sativa]	-7000892
Hemp extract	-7001676	Cannibidiol[Cannabis sativa]	-7000891
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7000891'
;
/*
Cannibidiol[Cannabis sativa]	-7000891	4-THUJANOL, CIS-(+/-)-	-7002917
Cannibidiol[Cannabis sativa]	-7000891	.GAMMA.-BISABOLENE, (Z)-	-7002884
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-CADINENE, (+)-	-7002872
Cannibidiol[Cannabis sativa]	-7000891	CAMPHENE	-7002870
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-LONGIPINENE	-7002840
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-PHELLANDRENE	-7002828
Cannibidiol[Cannabis sativa]	-7000891	QUERCETIN	-7002824
Cannibidiol[Cannabis sativa]	-7000891	LINALOOL, (+/-)-	-7002820
Cannibidiol[Cannabis sativa]	-7000891	CANNABIDIVARIN	-7002819
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-SELINENE	-7002802
Cannibidiol[Cannabis sativa]	-7000891	CANNABICITRAN	-7002788
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-EUDESMOL	-7002777
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-PINENE	-7002775
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-OCIMENE, (3E)-	-7002739
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-FARNESENE, (6Z)-	-7002732
Cannibidiol[Cannabis sativa]	-7000891	CANNABIGEROLIC ACID	-7002731
Cannibidiol[Cannabis sativa]	-7000891	CANNABICHROMENE	-7002724
Cannibidiol[Cannabis sativa]	-7000891	TETRAHYDROCANNABIVARIN	-7002723
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-TERPINEOL	-7002715
Cannibidiol[Cannabis sativa]	-7000891	FENCHONE, (+/-)-	-7002707
Cannibidiol[Cannabis sativa]	-7000891	CANNABIVARIN	-7002685
Cannibidiol[Cannabis sativa]	-7000891	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID B	-7002672
Cannibidiol[Cannabis sativa]	-7000891	CANNABIDIOL	-7002647
Cannibidiol[Cannabis sativa]	-7000891	YLANGENE	-7002640
Cannibidiol[Cannabis sativa]	-7000891	CANNABICHROMENIC ACID, (+)-	-7002634
Cannibidiol[Cannabis sativa]	-7000891	HUMULENE	-7002632
Cannibidiol[Cannabis sativa]	-7000891	TERPINOLENE	-7002605
Cannibidiol[Cannabis sativa]	-7000891	LINOLENIC ACID	-7002602
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-CARYOPHYLLENE OXIDE	-7002590
Cannibidiol[Cannabis sativa]	-7000891	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID A	-7002585
Cannibidiol[Cannabis sativa]	-7000891	CANNABINOL	-7002575
Cannibidiol[Cannabis sativa]	-7000891	3-BUTYL-.DELTA.9-TETRAHYDROCANNABINOL	-7002546
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-BISABOLOL, (-)-EPI-	-7002545
Cannibidiol[Cannabis sativa]	-7000891	.DELTA.-7-CIS-ISOTETRAHYDROCANNABIVARIN	-7002543
Cannibidiol[Cannabis sativa]	-7000891	N-CAFFEOYLTYRAMINE	-7002531
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-PINENE	-7002523
Cannibidiol[Cannabis sativa]	-7000891	BORNEOL	-7002507
Cannibidiol[Cannabis sativa]	-7000891	CANNABICHROMEVARIN	-7002498
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-OCIMENE, (3Z)-	-7002491
Cannibidiol[Cannabis sativa]	-7000891	IPSDIENOL	-7002484
Cannibidiol[Cannabis sativa]	-7000891	APIGENIN	-7002476
Cannibidiol[Cannabis sativa]	-7000891	ORIENTIN	-7002472
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-FARNESENE	-7002467
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-ELEMENE	-7002431
Cannibidiol[Cannabis sativa]	-7000891	.DELTA.9-TETRAHYDROCANNABINOLIC ACID	-7002428
Cannibidiol[Cannabis sativa]	-7000891	OLEIC ACID	-7002418
Cannibidiol[Cannabis sativa]	-7000891	CANNABIGEROLIC ACID MONOMETHYL ETHER	-7002407
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-BERGAMOTENE, (E)-(-)-	-7002406
Cannibidiol[Cannabis sativa]	-7000891	LUTEOLIN	-7002401
Cannibidiol[Cannabis sativa]	-7000891	.GAMMA.-BISABOLENE, (E)-	-7002399
Cannibidiol[Cannabis sativa]	-7000891	.DELTA.8-TETRAHYDROCANNABINOL	-7002395
Cannibidiol[Cannabis sativa]	-7000891	3-CARENE	-7002392
Cannibidiol[Cannabis sativa]	-7000891	VITEXIN	-7002380
Cannibidiol[Cannabis sativa]	-7000891	MYRCENE	-7002375
Cannibidiol[Cannabis sativa]	-7000891	.DELTA.1-TETRAHYDROCANNABIORCOL	-7002361
Cannibidiol[Cannabis sativa]	-7000891	DRONABINOL	-7002346
Cannibidiol[Cannabis sativa]	-7000891	GROSSAMIDE	-7002340
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-EUDESMOL	-7002324
Cannibidiol[Cannabis sativa]	-7000891	KAEMPFEROL	-7002319
Cannibidiol[Cannabis sativa]	-7000891	CANNABIDIOLIC ACID	-7002313
Cannibidiol[Cannabis sativa]	-7000891	.BETA.-FENCHYL ALCOHOL	-7002302
Cannibidiol[Cannabis sativa]	-7000891	.ALPHA.-THUJENE, (+/-)-	-7002297
Cannibidiol[Cannabis sativa]	-7000891	CARYOPHYLLENE	-7002290
Cannibidiol[Cannabis sativa]	-7000891	LIMONENE, (+/-)-	-7002281
Cannibidiol[Cannabis sativa]	-7000891	CANNABIGEROVARIN	-7002278
Cannibidiol[Cannabis sativa]	-7000891	LINOLEIC ACID	-7002249
Cannibidiol[Cannabis sativa]	-7000891	GUAIOL	-7002246
Cannibidiol[Cannabis sativa]	-7000891	.DELTA.-9-CIS-TETRAHYDROCANNABINOL, (-)-	-7002224
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7001676'
;
/*
Hemp extract	-7001676	3-BUTYL-.DELTA.9-TETRAHYDROCANNABINOL	-7002546
Hemp extract	-7001676	3-CARENE	-7002392
Hemp extract	-7001676	4-THUJANOL, CIS-(+/-)-	-7002917
Hemp extract	-7001676	.ALPHA.-BERGAMOTENE, (E)-(-)-	-7002406
Hemp extract	-7001676	.ALPHA.-BISABOLOL, (-)-EPI-	-7002545
Hemp extract	-7001676	.ALPHA.-CADINENE, (+)-	-7002872
Hemp extract	-7001676	.ALPHA.-EUDESMOL	-7002324
Hemp extract	-7001676	.ALPHA.-FARNESENE	-7002467
Hemp extract	-7001676	.ALPHA.-LONGIPINENE	-7002840
Hemp extract	-7001676	.ALPHA.-PINENE	-7002775
Hemp extract	-7001676	.ALPHA.-SELINENE	-7002802
Hemp extract	-7001676	.ALPHA.-TERPINEOL	-7002715
Hemp extract	-7001676	.ALPHA.-THUJENE, (+/-)-	-7002297
Hemp extract	-7001676	APIGENIN	-7002476
Hemp extract	-7001676	.BETA.-CARYOPHYLLENE OXIDE	-7002590
Hemp extract	-7001676	.BETA.-ELEMENE	-7002431
Hemp extract	-7001676	.BETA.-EUDESMOL	-7002777
Hemp extract	-7001676	.BETA.-FARNESENE, (6Z)-	-7002732
Hemp extract	-7001676	.BETA.-FENCHYL ALCOHOL	-7002302
Hemp extract	-7001676	.BETA.-OCIMENE, (3E)-	-7002739
Hemp extract	-7001676	.BETA.-OCIMENE, (3Z)-	-7002491
Hemp extract	-7001676	.BETA.-PHELLANDRENE	-7002828
Hemp extract	-7001676	.BETA.-PINENE	-7002523
Hemp extract	-7001676	BORNEOL	-7002507
Hemp extract	-7001676	CAMPHENE	-7002870
Hemp extract	-7001676	CANNABICHROMENE	-7002724
Hemp extract	-7001676	CANNABICHROMENIC ACID, (+)-	-7002634
Hemp extract	-7001676	CANNABICHROMEVARIN	-7002498
Hemp extract	-7001676	CANNABICITRAN	-7002788
Hemp extract	-7001676	CANNABIDIOL	-7002647
Hemp extract	-7001676	CANNABIDIOLIC ACID	-7002313
Hemp extract	-7001676	CANNABIDIVARIN	-7002819
Hemp extract	-7001676	CANNABIGEROLIC ACID	-7002731
Hemp extract	-7001676	CANNABIGEROLIC ACID MONOMETHYL ETHER	-7002407
Hemp extract	-7001676	CANNABIGEROVARIN	-7002278
Hemp extract	-7001676	CANNABINOL	-7002575
Hemp extract	-7001676	CANNABIVARIN	-7002685
Hemp extract	-7001676	CARYOPHYLLENE	-7002290
Hemp extract	-7001676	.DELTA.1-TETRAHYDROCANNABIORCOL	-7002361
Hemp extract	-7001676	.DELTA.-7-CIS-ISOTETRAHYDROCANNABIVARIN	-7002543
Hemp extract	-7001676	.DELTA.8-TETRAHYDROCANNABINOL	-7002395
Hemp extract	-7001676	.DELTA.-9-CIS-TETRAHYDROCANNABINOL, (-)-	-7002224
Hemp extract	-7001676	.DELTA.9-TETRAHYDROCANNABINOLIC ACID	-7002428
Hemp extract	-7001676	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID A	-7002585
Hemp extract	-7001676	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID B	-7002672
Hemp extract	-7001676	DRONABINOL	-7002346
Hemp extract	-7001676	FENCHONE, (+/-)-	-7002707
Hemp extract	-7001676	.GAMMA.-BISABOLENE, (E)-	-7002399
Hemp extract	-7001676	.GAMMA.-BISABOLENE, (Z)-	-7002884
Hemp extract	-7001676	GROSSAMIDE	-7002340
Hemp extract	-7001676	GUAIOL	-7002246
Hemp extract	-7001676	HUMULENE	-7002632
Hemp extract	-7001676	IPSDIENOL	-7002484
Hemp extract	-7001676	KAEMPFEROL	-7002319
Hemp extract	-7001676	LIMONENE, (+/-)-	-7002281
Hemp extract	-7001676	LINALOOL, (+/-)-	-7002820
Hemp extract	-7001676	LINOLEIC ACID	-7002249
Hemp extract	-7001676	LINOLENIC ACID	-7002602
Hemp extract	-7001676	LUTEOLIN	-7002401
Hemp extract	-7001676	MYRCENE	-7002375
Hemp extract	-7001676	N-CAFFEOYLTYRAMINE	-7002531
Hemp extract	-7001676	OLEIC ACID	-7002418
Hemp extract	-7001676	ORIENTIN	-7002472
Hemp extract	-7001676	QUERCETIN	-7002824
Hemp extract	-7001676	TERPINOLENE	-7002605
Hemp extract	-7001676	TETRAHYDROCANNABIVARIN	-7002723
Hemp extract	-7001676	VITEXIN	-7002380
Hemp extract	-7001676	YLANGENE	-7002640 
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = '-7002640'
;
/*
YLANGENE	-7002640	Cannabis sativa[Cannabis sativa]	-7002102
YLANGENE	-7002640	Marijuana[Cannabis sativa]	-7000896
YLANGENE	-7002640	Hemp extract[Cannabis sativa]	-7000895
YLANGENE	-7002640	Hemp[Cannabis sativa]	-7000894
YLANGENE	-7002640	Da ma[Cannabis sativa]	-7000893
YLANGENE	-7002640	CBD[Cannabis sativa]	-7000892
YLANGENE	-7002640	Cannibidiol[Cannabis sativa]	-7000891
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7002640'
;
/*
YLANGENE	-7002640	Hemp extract	-7001676
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = '-7000891'
;
/*
 Cannibidiol[Cannabis sativa]	-7000891	WHITE RECLUSE MARIJUANA FLOWERS	-7003508
Cannibidiol[Cannabis sativa]	-7000891	WAX, MARIJUANA	-7003507
Cannibidiol[Cannabis sativa]	-7000891	VAPORIZER CBD/MARIJUANA	-7003506
Cannibidiol[Cannabis sativa]	-7000891	VAPORIZED MEDICINAL MARIJUANA	-7003505
Cannibidiol[Cannabis sativa]	-7000891	VAPORIZED MARIJUANA	-7003504
Cannibidiol[Cannabis sativa]	-7000891	VAPING MARIJUANA	-7003503
Cannibidiol[Cannabis sativa]	-7000891	VAPE PEN (MARIJUANA)	-7003502
Cannibidiol[Cannabis sativa]	-7000891	UNSPECIFIED MEDICAL MARIJUANA	-7003501
Cannibidiol[Cannabis sativa]	-7000891	THC (PURE MARIJUANA IN PILL FORM) PRN	-7003500
Cannibidiol[Cannabis sativa]	-7000891	THC MARIJUANA	-7003499
Cannibidiol[Cannabis sativa]	-7000891	TETRAHYDROCANNABINOL (MARIJUANA,HASH)	-7003498
Cannibidiol[Cannabis sativa]	-7000891	TASTY GUMMIES FULL SPECTRUM HEMP OIL ASSORTED FRUIT FLAVORS 40 GUMMIES	-7003497
Cannibidiol[Cannabis sativa]	-7000891	R.L.V. 500MG HEMP EXTRACT ISOLATE	-7003496
Cannibidiol[Cannabis sativa]	-7000891	RESET BIOSCIENCE BALANCE 300MG 99%+ NANO LIPOSOMAL ORGANIC HEMP CBD	-7003495
Cannibidiol[Cannabis sativa]	-7000891	RECREATIONAL MARIJUANA	-7003494
Cannibidiol[Cannabis sativa]	-7000891	QUEEN CITY HEMP CBD	-7003493
Cannibidiol[Cannabis sativa]	-7000891	QUEEN CITY HEMP 500MG (CBD)	-7003492
Cannibidiol[Cannabis sativa]	-7000891	PROCANNA--HEMP OIL	-7003491
Cannibidiol[Cannabis sativa]	-7000891	POT	-7003490
Cannibidiol[Cannabis sativa]	-7000891	PLUS CBD OIL HEMP DROPS PEPPERMINT EXTRA STRENGTH	-7003489
Cannibidiol[Cannabis sativa]	-7000891	PLATINUM AND DANK BRAND MARIJUANA CARTRIDGES (VAPE THCNICOTINE)	-7003488
Cannibidiol[Cannabis sativa]	-7000891	PEACE + WELLNESS ELEVATE (CBD/HEMP OIL INFUSED)	-7003487
Cannibidiol[Cannabis sativa]	-7000891	PAX 3 VAPORIZER ^NOTHING WAS OBTAINED, BUT DRIED MARIJUANA WAS USED IN THE PAX 3 DEVICE	-7003486
Cannibidiol[Cannabis sativa]	-7000891	ORIGINAL FORMULA HEMP EXTRACT OLIVE OIL FLAVOR CHARLOTTES WEB [CBD]	-7003485
Cannibidiol[Cannabis sativa]	-7000891	ORIGINAL FORMULA HEMP EXTRACT OIL MINT CHOCOLATE FLAVOR CHARLOTTES WEB [CBD]	-7003484
Cannibidiol[Cannabis sativa]	-7000891	OIL MARIJUANA	-7003483
Cannibidiol[Cannabis sativa]	-7000891	NUTRITIONAL FRONTIERS FULL SPECTRUM HEMP EXTREME OIL (HEMP EXTRACT)	-7003482
Cannibidiol[Cannabis sativa]	-7000891	METAGENICS, HEMP OIL BROAD SPECTRUM HEMP EXTRACT, 30ML BOTTLE WITH A 1	-7003481
Cannibidiol[Cannabis sativa]	-7000891	MED MARIJUANA	-7003480
Cannibidiol[Cannabis sativa]	-7000891	MEDICINAL RECREAT CANABIS HEMP F	-7003479
Cannibidiol[Cannabis sativa]	-7000891	MEDICINAL MARIJUANA (CANNABIS SATIVA)	-7003478
Cannibidiol[Cannabis sativa]	-7000891	MEDICINAL MARIJUANA	-7003477
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA TINCTURE OILS	-7003476
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA (RSO)	-7003475
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA (PRESCRIPTION)	-7003474
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA OIL	-7003473
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA (MARIJUANA)	-7003472
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA/CBD	-7003471
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA CARD HOLDER	-7003470
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA (CANNABIS SATIVA)	-7003469
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL MARIJUANA	-7003468
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL CANNABIS OIL VIOLET CBD	-7003467
Cannibidiol[Cannabis sativa]	-7000891	MEDICAL CANNABIS CBD	-7003466
Cannibidiol[Cannabis sativa]	-7000891	MAXIMUM STRENGTH HEMP EXTRACT OIL, 60MG/SERVING	-7003465
Cannibidiol[Cannabis sativa]	-7000891	MARJUANA	-7003464
Cannibidiol[Cannabis sativa]	-7000891	MARIJUNAN	-7003463
Cannibidiol[Cannabis sativa]	-7000891	MARIJUNA	-7003462
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANNA	-7003461
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA WAX	-7003460
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA VAPING PRODUCT	-7003459
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA VAPING LIQUID	-7003458
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA VAPE	-7003457
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA TINCTURE	-7003456
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA/ THC/ HEMP/HASH	-7003455
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA TBD HERB MEDICAL CENTER, 12509 OXNARD ST. N.HOLLYW	-7003454
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA/SYNTHETIC PILL FORM OF MARIJUANA (CANNABIS SATIVA)	-7003453
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA SUPPLEMENTS (THC AND CBD)	-7003452
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA OIL VAPING	-7003451
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA OIL	-7003450
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA, N.O.S	-7003449
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (NO PREF. NAME)	-7003448
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (MEDICAL)	-7003447
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (MARIJUANA) UNKNOWN	-7003446
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA(MARIJUANA)	-7003445
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (MARIJUANA)	-7003444
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA             (MARIJUANA)	-7003443
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA LIQUID	-7003442
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA KUSH	-7003441
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA HERB, THC OILS	-7003440
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA HERB DEVICE USED: LOKEE BRAND VAPE^	-7003439
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA GUMMIES	-7003438
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA FOR MEDICAL USE	-7003437
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA FLOWER	-7003436
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA EXTRACT	-7003435
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA EDIBLES	-7003434
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA DROPS	-7003433
Cannibidiol[Cannabis sativa]	-7000891	^MARIJUANA CREAM^	-7003432
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNIBIS SATIVA)	-7003431
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNIBAS)	-7003430
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS SATIVA) UNKNOWN	-7003429
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS SATIVA)(CANNABIS SATIVA)	-7003428
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS SATIVA) (CANNABIS SATIVA)	-7003427
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS SATIVA)	-7003426
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS SATIVA )	-7003425
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA    (CANNABIS SATIVA)	-7003424
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS, CANNABIS SATIVA)	-7003423
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA                   (CANNABIS, CANNABIS SATIVA)	-7003422
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS0	-7003421
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA(CANNABIS)	-7003420
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS,)	-7003419
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS, )	-7003418
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA (CANNABIS)	-7003417
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA  (CANNABIS)	-7003416
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA  (CANNABIS	-7003415
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA                                        (CANNABIS)	-7003414
Cannibidiol[Cannabis sativa]	-7000891	MARIJUANA	-7003413
Cannibidiol[Cannabis sativa]	-7000891	MARIJUA	-7003412
Cannibidiol[Cannabis sativa]	-7000891	MARIJAUNA	-7003411
Cannibidiol[Cannabis sativa]	-7000891	MARIJANA	-7003410
Cannibidiol[Cannabis sativa]	-7000891	MARIIJUANA	-7003409
Cannibidiol[Cannabis sativa]	-7000891	MARIHUANA	-7003408
Cannibidiol[Cannabis sativa]	-7000891	MARAJUANA	-7003407
Cannibidiol[Cannabis sativa]	-7000891	LEGAL MARIJUANA	-7003406
Cannibidiol[Cannabis sativa]	-7000891	LEGALIZED MARIJUANA	-7003405
Cannibidiol[Cannabis sativa]	-7000891	K2/MARIJUANA	-7003404
Cannibidiol[Cannabis sativa]	-7000891	ILLEGAL MARIJUANA	-7003403
Cannibidiol[Cannabis sativa]	-7000891	HEMPZ	-7003402
Cannibidiol[Cannabis sativa]	-7000891	HEMPWORX CBD OIL	-7003401
Cannibidiol[Cannabis sativa]	-7000891	HEMPWORX	-7003400
Cannibidiol[Cannabis sativa]	-7000891	HEMPVANA HEEL TASTIC	-7003399
Cannibidiol[Cannabis sativa]	-7000891	HEMPVANA	-7003398
Cannibidiol[Cannabis sativa]	-7000891	HEMPTRANCE NATURAL CBD GUMMIES	-7003397
Cannibidiol[Cannabis sativa]	-7000891	HEMPSEED	-7003396
Cannibidiol[Cannabis sativa]	-7000891	HEMP SEED	-7003395
Cannibidiol[Cannabis sativa]	-7000891	HEMP PROTEIN POWDER	-7003394
Cannibidiol[Cannabis sativa]	-7000891	HEMP MED EX: RSHO BLUE, GOLD	-7003393
Cannibidiol[Cannabis sativa]	-7000891	HEMPLUCID CBD OIL 1000 MG VAPING	-7003392
Cannibidiol[Cannabis sativa]	-7000891	HEMPANOL	-7003391
Cannibidiol[Cannabis sativa]	-7000891	HASHISH	-7003390
Cannibidiol[Cannabis sativa]	-7000891	EDIBLE MARIJUANA	-7003389
Cannibidiol[Cannabis sativa]	-7000891	DRUG - MARIJUANA	-7003388
Cannibidiol[Cannabis sativa]	-7000891	DISPENSARY MARIJUANA PLANT AND WAX CARTRIDGES.	-7003387
Cannibidiol[Cannabis sativa]	-7000891	CYPRESS HEMP CBD OMEGAS	-7003386
Cannibidiol[Cannabis sativa]	-7000891	CTFO (CHANGING THE FUTURE OUTCOME) 10XPURE CBD HEMP OIL	-7003385
Cannibidiol[Cannabis sativa]	-7000891	COLESVAM MEDICAL MARIJUANA (CBD)	-7003384
Cannibidiol[Cannabis sativa]	-7000891	CBD OIL (MEDICAL MARIJUANA)	-7003383
Cannibidiol[Cannabis sativa]	-7000891	CBD OIL / HEMP OIL	-7003382
Cannibidiol[Cannabis sativa]	-7000891	CBD OIL HEMP-DERIVED CANNABIDIOL FULL SPECTRUM HEMP SUPPL	-7003381
Cannibidiol[Cannabis sativa]	-7000891	CBDISTILLERY 33MG CBD PER SERVING FULL SPECTRUM HEMP SUPPLEMENT	-7003380
Cannibidiol[Cannabis sativa]	-7000891	CBD HEMP FLOWER (CBDTHC)	-7003379
Cannibidiol[Cannabis sativa]	-7000891	CBD HEMP EXTRACT	-7003378
Cannibidiol[Cannabis sativa]	-7000891	CBD EXTRACT	-7003377
Cannibidiol[Cannabis sativa]	-7000891	CANNIMED OIL	-7003376
Cannibidiol[Cannabis sativa]	-7000891	CANNIBIS	-7003375
Cannibidiol[Cannabis sativa]	-7000891	CANNIBIDIOL OIL	-7003374
Cannibidiol[Cannabis sativa]	-7000891	CANNIBAL	-7003373
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS VAPING	-7003372
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS TEA	-7003371
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SPRAY	-7003370
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SMOKING	-7003369
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SATIVA SUBSP. SATIVA FLOWERING TOP	-7003368
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SATIVA SUBSP. INDICA TOP	-7003367
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SATIVA OIL	-7003366
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SATIVA (HEMP HEARTS)	-7003365
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SATIVA FLOWER	-7003364
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SATIVA EXTRACT	-7003363
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS SATIVA	-7003362
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS RESIN	-7003361
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS LOTIONS	-7003360
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS INDICA SMOKE	-7003359
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS, INDICA	-7003358
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS INDICA	-7003357
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS BEDICA OLIE	-7003356
Cannibidiol[Cannabis sativa]	-7000891	CANNABIS BEDICA	-7003355
Cannibidiol[Cannabis sativa]	-7000891	CANNABIOL	-7003354
Cannibidiol[Cannabis sativa]	-7000891	CANNABINIODS	-7003353
Cannibidiol[Cannabis sativa]	-7000891	CANABIS LOTION	-7003352
Cannibidiol[Cannabis sativa]	-7000891	CALM REST AND RELAX HEMP EXTRACT FORMULA (CBG)	-7003351
Cannibidiol[Cannabis sativa]	-7000891	CALM REST AND RELAX HEMP EXTRACT FORMULA (CBD)	-7003350
Cannibidiol[Cannabis sativa]	-7000891	AMOS HEMPS [CBD OIL IN MCT OIL]	-7003349
Cannibidiol[Cannabis sativa]	-7000891	ALLERGY INJECTIONS - CAT DANDER,DOG DANDER,MOLD MIX,HEMP	-7003348
Cannibidiol[Cannabis sativa]	-7000891	ADVANCED CBD OIL WITH TERPENES (FROM HEMP)	-7003347
Cannibidiol[Cannabis sativa]	-7000891	ACETAMINOPHEN/CODINE MEDICAL MARIJUANA	-7003346
Cannibidiol[Cannabis sativa]	-7000891	60MG, PLANT?BASED CANNABINOIDS PER 1ML HEMP EXTRACT MINT CHOCOLATE FLA	-7003345
Cannibidiol[Cannabis sativa]	-7000891	2 HITS OF MARIJUANA	-7003344
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
    and c1.concept_id = '-7003344'  
;
/*
2 HITS OF MARIJUANA	-7003344	Cannabis sativa[Cannabis sativa]	-7002102
2 HITS OF MARIJUANA	-7003344	Marijuana[Cannabis sativa]	-7000896
2 HITS OF MARIJUANA	-7003344	Hemp extract[Cannabis sativa]	-7000895
2 HITS OF MARIJUANA	-7003344	Hemp[Cannabis sativa]	-7000894
2 HITS OF MARIJUANA	-7003344	Da ma[Cannabis sativa]	-7000893
2 HITS OF MARIJUANA	-7003344	CBD[Cannabis sativa]	-7000892
2 HITS OF MARIJUANA	-7003344	Cannibidiol[Cannabis sativa]	-7000891
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
    and c1.concept_id = '-7001676'
;
-- no results



---------------------------------------


-- Test using Licorice
select * from staging_vocabulary.concept c where concept_class_id in ('Licorice', 'Liquorice','Zhangguogancao');
/*
-7001126	Guang guo gan cao[Glycyrrhiza glabra]	NaPDI research	NAPDI	Liquorice		ffb21b4e-ae7e-41d5-97b3-3656d1353dc3	2000-01-01	2099-02-22	
-7001127	Licorice[Glycyrrhiza glabra]	NaPDI research	NAPDI	Liquorice		ffb21b4e-ae7e-41d5-97b3-3656d1353dc3	2000-01-01	2099-02-22	
-7001128	Liquorice[Glycyrrhiza glabra]	NaPDI research	NAPDI	Liquorice		ffb21b4e-ae7e-41d5-97b3-3656d1353dc3	2000-01-01	2099-02-22	
-7001129	Licorice[Glycyrrhiza inflata]	NaPDI research	NAPDI	Zhangguogancao		59eda220-bbb5-4dc0-b57a-d010a4222c03	2000-01-01	2099-02-22	
-7001130	Zhangguogancao[Glycyrrhiza inflata]	NaPDI research	NAPDI	Zhangguogancao		59eda220-bbb5-4dc0-b57a-d010a4222c03	2000-01-01	2099-02-22	
-7001131	Chinese licorice[Glycyrrhiza uralensis]	NaPDI research	NAPDI	Licorice		dc6f848b-18bb-4620-81b1-b2af39721bd9	2000-01-01	2099-02-22	
-7001132	Gan cao[Glycyrrhiza uralensis]	NaPDI research	NAPDI	Licorice		dc6f848b-18bb-4620-81b1-b2af39721bd9	2000-01-01	2099-02-22	
-7001133	Licorice[Glycyrrhiza uralensis]	NaPDI research	NAPDI	Licorice		dc6f848b-18bb-4620-81b1-b2af39721bd9	2000-01-01	2099-02-22	
-7001933	Glycyrrhiza uralensis[Glycyrrhiza uralensis]	NaPDI research	NAPDI	Licorice		dc6f848b-18bb-4620-81b1-b2af39721bd9	2000-01-01	2099-02-22	
-7001943	Glycyrrhiza glabra[Glycyrrhiza glabra]	NaPDI research	NAPDI	Liquorice		ffb21b4e-ae7e-41d5-97b3-3656d1353dc3	2000-01-01	2099-02-22	
-7002049	Glycyrrhiza inflata[Glycyrrhiza inflata]	NaPDI research	NAPDI	Zhangguogancao		59eda220-bbb5-4dc0-b57a-d010a4222c03	2000-01-01	2099-02-22	
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id in (-7001933, -7001943, -7002049)
;
/*
Glycyrrhiza inflata[Glycyrrhiza inflata]	-7002049	Zhangguogancao	-7001885
Glycyrrhiza glabra[Glycyrrhiza glabra]	-7001943	Liquorice	-7001769
Glycyrrhiza uralensis[Glycyrrhiza uralensis]	-7001933	Licorice	-7001837
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
   and c1.concept_id = -7001837
;
/*
Licorice	-7001837	Glycyrrhiza uralensis[Glycyrrhiza uralensis]	-7001933
Licorice	-7001837	Licorice[Glycyrrhiza uralensis]	-7001133
Licorice	-7001837	Gan cao[Glycyrrhiza uralensis]	-7001132
Licorice	-7001837	Chinese licorice[Glycyrrhiza uralensis]	-7001131
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7001132
;
/*
Gan cao[Glycyrrhiza uralensis]	-7001132	GLYCYRRHIZIN	-7002903
Gan cao[Glycyrrhiza uralensis]	-7001132	DEHYDROGLYASPERIN C	-7002874
Gan cao[Glycyrrhiza uralensis]	-7001132	3-O-METHYLGLYCYROL	-7002865
Gan cao[Glycyrrhiza uralensis]	-7001132	URALSAPONIN V	-7002818
Gan cao[Glycyrrhiza uralensis]	-7001132	LICORICESAPONIN A3	-7002789
Gan cao[Glycyrrhiza uralensis]	-7001132	DEHYDROGLYASPERIN D	-7002751
Gan cao[Glycyrrhiza uralensis]	-7001132	MEDICARPIN	-7002748
Gan cao[Glycyrrhiza uralensis]	-7001132	FORMONONETIN	-7002696
Gan cao[Glycyrrhiza uralensis]	-7001132	URALSAPONIN Y	-7002657
Gan cao[Glycyrrhiza uralensis]	-7001132	GLYASPERIN F	-7002617
Gan cao[Glycyrrhiza uralensis]	-7001132	LIQUIRITIN APIOSIDE	-7002616
Gan cao[Glycyrrhiza uralensis]	-7001132	LICORICESAPONIN G2	-7002609
Gan cao[Glycyrrhiza uralensis]	-7001132	URALSAPONIN F	-7002588
Gan cao[Glycyrrhiza uralensis]	-7001132	SEMILICOISOFLAVONE B	-7002560
Gan cao[Glycyrrhiza uralensis]	-7001132	LICORICIDIN	-7002540
Gan cao[Glycyrrhiza uralensis]	-7001132	ALLOLICOISOFLAVONE A	-7002520
Gan cao[Glycyrrhiza uralensis]	-7001132	LICORICONE	-7002518
Gan cao[Glycyrrhiza uralensis]	-7001132	GLYCYRIN	-7002500
Gan cao[Glycyrrhiza uralensis]	-7001132	URALSAPONIN C	-7002499
Gan cao[Glycyrrhiza uralensis]	-7001132	KANZONOL P	-7002488
Gan cao[Glycyrrhiza uralensis]	-7001132	URALSAPONIN X	-7002465
Gan cao[Glycyrrhiza uralensis]	-7001132	ISOGLYCYROL	-7002389
Gan cao[Glycyrrhiza uralensis]	-7001132	4',7-DIHYDROXYFLAVONE	-7002359
Gan cao[Glycyrrhiza uralensis]	-7001132	LICOISOFLAVONE B	-7002358
Gan cao[Glycyrrhiza uralensis]	-7001132	LIQUIRITIN	-7002330
Gan cao[Glycyrrhiza uralensis]	-7001132	LICORICESAPONIN E2	-7002329
Gan cao[Glycyrrhiza uralensis]	-7001132	JARANOL	-7002301
Gan cao[Glycyrrhiza uralensis]	-7001132	3-O-(.BETA.-D-GLUCURONOPYRANOSYL-(1->2)-.BETA.-D-GALACTOPYRANOSYL)GLYCYRRHETIC ACID	-7002298
Gan cao[Glycyrrhiza uralensis]	-7001132	TRIHYDROXYCHALCONE	-7002268
Gan cao[Glycyrrhiza uralensis]	-7001132	VESTITOL, (-)-	-7002257
Gan cao[Glycyrrhiza uralensis]	-7001132	URALSAPONIN W	-7002256
Gan cao[Glycyrrhiza uralensis]	-7001132	LIQUIRITIGENIN	-7002233
Gan cao[Glycyrrhiza uralensis]	-7001132	ISOTRIFOLIOL	-7002223
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7001837
;
/*
Licorice	-7001837	3-O-(.BETA.-D-GLUCURONOPYRANOSYL-(1->2)-.BETA.-D-GALACTOPYRANOSYL)GLYCYRRHETIC ACID	-7002298
Licorice	-7001837	3-O-METHYLGLYCYROL	-7002865
Licorice	-7001837	4',7-DIHYDROXYFLAVONE	-7002359
Licorice	-7001837	ALLOLICOISOFLAVONE A	-7002520
Licorice	-7001837	DEHYDROGLYASPERIN C	-7002874
Licorice	-7001837	DEHYDROGLYASPERIN D	-7002751
Licorice	-7001837	FORMONONETIN	-7002696
Licorice	-7001837	GLYASPERIN F	-7002617
Licorice	-7001837	GLYCYRIN	-7002500
Licorice	-7001837	GLYCYRRHIZIN	-7002903
Licorice	-7001837	ISOGLYCYROL	-7002389
Licorice	-7001837	ISOTRIFOLIOL	-7002223
Licorice	-7001837	JARANOL	-7002301
Licorice	-7001837	KANZONOL P	-7002488
Licorice	-7001837	LICOISOFLAVONE B	-7002358
Licorice	-7001837	LICORICESAPONIN A3	-7002789
Licorice	-7001837	LICORICESAPONIN E2	-7002329
Licorice	-7001837	LICORICESAPONIN G2	-7002609
Licorice	-7001837	LICORICIDIN	-7002540
Licorice	-7001837	LICORICONE	-7002518
Licorice	-7001837	LIQUIRITIGENIN	-7002233
Licorice	-7001837	LIQUIRITIN	-7002330
Licorice	-7001837	LIQUIRITIN APIOSIDE	-7002616
Licorice	-7001837	MEDICARPIN	-7002748
Licorice	-7001837	SEMILICOISOFLAVONE B	-7002560
Licorice	-7001837	TRIHYDROXYCHALCONE	-7002268
Licorice	-7001837	URALSAPONIN C	-7002499
Licorice	-7001837	URALSAPONIN F	-7002588
Licorice	-7001837	URALSAPONIN V	-7002818
Licorice	-7001837	URALSAPONIN W	-7002256
Licorice	-7001837	URALSAPONIN X	-7002465
Licorice	-7001837	URALSAPONIN Y	-7002657
Licorice	-7001837	VESTITOL, (-)-	-7002257
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = -7002499
;
/*
URALSAPONIN C	-7002499	Glycyrrhiza uralensis[Glycyrrhiza uralensis]	-7001933
URALSAPONIN C	-7002499	Licorice[Glycyrrhiza uralensis]	-7001133
URALSAPONIN C	-7002499	Gan cao[Glycyrrhiza uralensis]	-7001132
URALSAPONIN C	-7002499	Chinese licorice[Glycyrrhiza uralensis]	-7001131
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = -7002499
;
/*
URALSAPONIN C	-7002499	Licorice	-7001837
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7001933
;
/*
EMPTY B/C WE DID NOT INCLUDE LICORICE IN THE REFERENCE SET FOR SPRING 2022
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
    and c1.concept_id =  
;
/*
EMPTY B/C WE DID NOT INCLUDE LICORICE IN THE REFERENCE SET FOR SPRING 2022
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
    and c1.concept_id = '-7001837'
;
/*
Licorice	-7001837	licorice 445 MG Oral Capsule	1353049
Licorice	-7001837	licorice 450 MG Oral Capsule	1353050
Licorice	-7001837	licorice 500 MG Oral Capsule	1353051
Licorice	-7001837	licorice 0.812 MG/MG	1353075
Licorice	-7001837	LICORICE ROOT	1390664
Licorice	-7001837	licorice 400 MG Oral Capsule	19023262
Licorice	-7001837	licorice root extract	19055215
Licorice	-7001837	LICORICE EXTRACT	19080470
Licorice	-7001837	benzocaine 0.325 MG/MG / licorice 0.812 MG/MG / menthol 0.000325 MG/MG Oral Lozenge	19080588
Licorice	-7001837	licorice 500 MG	19088180
Licorice	-7001837	licorice 400 MG	19088202
Licorice	-7001837	licorice 445 MG	19088203
Licorice	-7001837	licorice 450 MG	19088204
Licorice	-7001837	licorice 1 MG/ML / undecylenic acid 10 MG/ML Medicated Shampoo	19106045
Licorice	-7001837	Ammonium Chloride 80 MG / licorice root extract 120 MG Oral Lozenge	19108120
Licorice	-7001837	licorice root extract 120 MG	19108740
Licorice	-7001837	licorice 1 MG/ML	19112350
Licorice	-7001837	Licorice / Undecylenate	36028519
Licorice	-7001837	Ammonium Chloride / licorice root extract	36029126
Licorice	-7001837	Glycine / Licorice	36029437
Licorice	-7001837	Benzocaine / Licorice / Menthol	36030586
Licorice	-7001837	Ammonium Chloride / licorice root extract Oral Product	36215949
Licorice	-7001837	benzocaine / licorice / menthol Oral Product	36217923
Licorice	-7001837	licorice Oral Product	36218423
Licorice	-7001837	licorice Pill	36218424
Licorice	-7001837	licorice allergenic extract Injectable Product	36219886
Licorice	-7001837	glycine / licorice Oral Product	36238358
Licorice	-7001837	glycine / licorice Pill	36238359
Licorice	-7001837	glycine / licorice Chewable Product	36243760
Licorice	-7001837	benzocaine / licorice / menthol Lozenge Product	36243798
Licorice	-7001837	licorice / undecylenate Shampoo Product	36244173
Licorice	-7001837	Ammonium Chloride / licorice root extract Lozenge Product	36244357
Licorice	-7001837	Licorice / Undecylenic Acid Medicated Shampoo	40001260
Licorice	-7001837	licorice Oral Capsule	40001272
Licorice	-7001837	Ammonium Chloride / licorice root extract Oral Lozenge	40008999
Licorice	-7001837	benzocaine / licorice / menthol Oral Lozenge	40013490
Licorice	-7001837	licorice / undecylenate Medicated Shampoo	40138303
Licorice	-7001837	licorice allergenic extract	40226650
Licorice	-7001837	licorice allergenic extract 100 MG/ML	40226651
Licorice	-7001837	licorice allergenic extract 100 MG/ML Injectable Solution	40226652
Licorice	-7001837	licorice allergenic extract 50 MG/ML	40226653
Licorice	-7001837	licorice allergenic extract 50 MG/ML Injectable Solution	40226654
Licorice	-7001837	licorice allergenic extract Injectable Solution	40226655
Licorice	-7001837	glycine / licorice Chewable Tablet	42705441
Licorice	-7001837	glycine 50 MG / licorice 400 MG Chewable Tablet	42707710
 */


select *
from staging_vocabulary.concept c 
where c.concept_id = -7001837
;

select c1.concept_name np_pt, c1.concept_id np_concept_id,  c2.concept_name rxnorm_concept_name, c2.concept_id rx_norm_concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
;

----------------------------------------------

--- Obtain case counts of NPs from the NP drill-down table 
select c2.concept_class_id, count(distinct c1.concept_name) drug_name_str_cnts
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
group by c2.concept_class_id 
order by c2.concept_class_id  
;
/* -- 54 NPs with spell vars
Aloe vera	18
Ashwaganda	40
Barley grass	5
Beet root	7
Black cherry	7
Black cohosh	18
Butcher's-broom	18
Cat's-claw	18
Chamomile	4
Cinnamon	137
Echinacea	60
Fennel	1
Feverfew	13
Flax seed	327
Ginger	93
Ginkgo	93
Goji berry	1
Green tea	131
Guarana	20
Hemp extract	165
Horehound	3
Horse-chestnut	31
Ivy leaf	5
Karcura	2
Kava	3
Kratom	88
Lion's-tooth	32
Maca	32
Malabartamarind	3
Milk thistle	33
Miracle-fruit	15
Moringa	19
Mu zei	4
NaPDI Preferred Term	151
Niu bang zi	13
Olive tree	1
Oregano	2
Panax ginseng	40
Reishi	29
Rhodiola	24
Scrub-palmetto	164
Slippery elm	9
Stinging nettle	9
St. John's-wort	50
Swallowwort	15
Sweet Elder	19
Tang-kuei	10
Tulsi	10
Turmeric	36
Valerian	41
Wheat grass	3
Woodland hawthorn	33
Wood spider	26
Yohimbe	1
 */


-- Report counts by NP 
with np_cnt_by_isr as (
 select np_pt, count(distinct isr) isr_cnt
 from scratch_dedup_q1_2004_thr_q2_2022.combined_np_drug_outcome_drilldown cndod
 where np_pt != ''
 group by np_pt
), np_cnt_by_primaryid as ( 
 select np_pt, count(distinct primaryid) primaryid_cnt
 from scratch_dedup_q1_2004_thr_q2_2022.combined_np_drug_outcome_drilldown cndod
 where np_pt != ''
 group by np_pt
)
select t.np_pt, sum(t.rprt_id) rprt_cnt
from (
  select np_pt, isr_cnt rprt_id 
  from np_cnt_by_isr
  union
  select np_pt, primaryid_cnt rprt_id 
  from np_cnt_by_primaryid
) t
group by t.np_pt
order by t.np_pt
;
/* -- 167 unique NPs by common name
Aloe vera	781
Ashwaganda	323
Barley grass	28
Beet root	31
Bing lang	1
Black cherry	24
Black cohosh	755
Blackwalnut	1
Boldu	4
Butcher's-broom	40
Cabbage palm	5
Carilla gourd	13
Carrot	1
Cat's-claw	121
Celery	36
Chaga	9
Chamomile	108
Cinnamon	4243
Couchgrass	1
Cumin	2
Da hua long ya cao	1
Damiana	7
Dang shen	3
Dog rose	9
Echinacea	1107
Fennel	21
Fenugreek	95
Feverfew	153
Field balm	2
Flax seed	7960
Ginger	1646
Ginkgo	2526
Glossy buckthorn	2
Goji berry	8
Goose-grass	1
Grapevine	8
Green tea	857
Guarana	66
Guelder rose	1
Hare's-ear	3
Hazelnut tree	1
Hemp extract	7978
He shou wu	2
Horehound	4
Horse-chestnut	220
Huanglian	1
Indian bdellium-tree	11
Ivy leaf	26
Juniper	2
Karcura	17
Kava	60
Kiratatikta	13
Kola	1
Kratom	572
Kuso-no-ki	1
Licorice	8
Lion's-tooth	167
Liquorice	18
Maca	144
Magnolia-bark	1
Malabartamarind	20
Maocangzhu	1
Melissa	16
Milk thistle	2611
Miracle-fruit	52
Moringa	79
Motherwort	4
Muscadine grape	3
Mu zei	30
Myrtle	2
Nimtree	10
Niu bang zi	30
Olive tree	11
Oregano	234
Oregon-grape	2
Panax ginseng	194
Parsley	19
Parsley- Chinese	7
Pau d'Arco	9
Phyllanthusamarus	2
Pi ba	2
piprage	3
Pitch pine	1
Plantain	11
Prince's-pine	1
Purplegranadilla	2
Qiang huo	1
Quassia wood	1
Radicchio	7
Reishi	143
Rendongteng	1
Rhodiola	171
Rose laurel	5
Rosemary	44
Rotahorn	2
Rotten cheesefruit	25
Rue	3
Sage	55
Scots heather	1
Scrub-palmetto	3127
Seawrack	20
Seed- rape	2
Shallot	1
Shatavari	1
Shirley poppy	1
Siberian ginseng	22
Silktree albizia	2
Slippery elm	95
Slippery-root	9
Small centaury	1
Soybean	638
Spearmint	94
Stinging nettle	67
St. John's-bread	3
St. John's-wort	641
Suan cheng	13
Swallowwort	28
Sweet Elder	255
Sweet joe-pye-weed	1
Sweet marjoram	2
Sweetroot	2
Sweet wormwood	1
Talewort	5
Tamarind	5
Tang-kuei	44
Tetterwort	1
Thyme	16
Touch-me-not	1
Tribulus	15
Tulsi	42
Turmeric	8254
Tyfon	1
Uva ursi	12
Valencia orange	5
Valerian	633
Velvet-dock	14
Virginian skullcap	15
Vitex	29
Walnut	5
Water hyssop	13
Wax-dolls	1
Wheat grass	20
White Atractylodes	1
White oak	15
White pepper	6
Wild geranium	2
Wild yam	12
Witch-hazel	30
Woodland hawthorn	122
Wood spider	119
Wormwood	19
Wu hua guo	3
Wu wei zi	12
Xiakucao	1
Xing	1
Xuan guo wen zi cao	1
Yarrow	5
Yellow dock	5
Yellow gentian	15
Yellow root	22
Yerba mate	5
Yin yang huo	12
Yohimbe	1
Yucca	26
Yunzhi	9
zang bian da huang	1
Zhang ye da huang	27
*/

-- derive a year distribution (until 2022)  of reports counts by common name
with np_cnt_by_isr as (
 select np_pt, cndod.report_year, count(distinct isr) isr_cnt
 from scratch_dedup_q1_2004_thr_q2_2022.combined_np_drug_outcome_drilldown cndod
 where np_pt != '' 
  and report_year < 22
 group by np_pt, report_year 
), np_cnt_by_primaryid as ( 
 select np_pt, cndod.report_year, count(distinct primaryid) primaryid_cnt
 from scratch_dedup_q1_2004_thr_q2_2022.combined_np_drug_outcome_drilldown cndod
 where np_pt != ''
   and report_year < 22
 group by np_pt, report_year 
)
select t.np_pt, report_year, sum(t.rprt_id) rprt_cnt
into scratch_oct2022_np_vocab.report_counts_np_year
from (
  select np_pt, report_year, isr_cnt rprt_id 
  from np_cnt_by_isr
  union
  select np_pt, report_year, primaryid_cnt rprt_id 
  from np_cnt_by_primaryid
) t
group by t.np_pt, report_year
order by t.np_pt, report_year
;
-- 1203 results


--------------------

-- Attempted the same thing as the first query above in this block but by L.B.  - the issue is its probably better to just apply the 
-- same counts to each LB mapped to a common name
-- 172 L.B. (note filter on non LB 'Rheum' that seems to be an error in the DB)
with np_cnt_by_isr as (
 select np_pt, count(distinct isr) isr_cnt
 from scratch_dedup_q1_2004_thr_q2_2022.combined_np_drug_outcome_drilldown cndod
 where np_pt != ''
 group by np_pt
), np_cnt_by_primaryid as ( 
 select np_pt, count(distinct primaryid) primaryid_cnt
 from scratch_dedup_q1_2004_thr_q2_2022.combined_np_drug_outcome_drilldown cndod
 where np_pt != ''
 group by np_pt
), lb_sum as (
 select regexp_replace(c.concept_name, '.*\[(.*)\]','\1') lb, t.np_pt, sum(t.rprt_id) rprt_cnt
 from (
   select np_pt, isr_cnt rprt_id 
   from np_cnt_by_isr
   union
   select np_pt, primaryid_cnt rprt_id 
   from np_cnt_by_primaryid
 ) t inner join staging_vocabulary.concept c 
          on t.np_pt = c.concept_class_id 
 group by lb, t.np_pt
) 
select * 
from lb_sum
where lb != 'Rheum'
order by lb 
;
/*
 Acer rubrum	Rotahorn	6
Achillea millefolium	Yarrow	20
Acorus calamus	Sweetroot	14
Actaea racemosa	Black cohosh	3020
Aesculus hippocastanum	Horse-chestnut	660
Agrimonia eupatoria	Da hua long ya cao	3
Albizia julibrissin	Silktree albizia	12
Allium cepa	Shallot	3
Aloe vera	Aloe vera	4686
Angelica sinensis	Tang-kuei	308
Apium graveolens	Celery	72
Arctium lappa	Niu bang zi	420
Arctostaphylos uva	Uva ursi	48
Areca catechu	Bing lang	3
Artemisia absinthium	Wormwood	95
Artemisia annua	Sweet wormwood	4
Asparagus racemosus	Shatavari	3
Atractylodes lancea	Maocangzhu	2
Atractylodes macrocephala	White Atractylodes	5
Azadirachta indica	Nimtree	50
Bacopa monnieri	Water hyssop	52
Berberis aquifolium	Oregon-grape	18
Berberis vulgaris	piprage	15
Beta vulgaris	Beet root	558
Borago officinalis	Talewort	30
Brassica oleracea	Seed- rape	10
Brassica rapa	Tyfon	5
Bupleurum chinense	Hare's-ear	18
Calluna vulgaris	Scots heather	7
Camellia sinensis	Green tea	5142
Cannabis sativa	Hemp extract	55846
Centaurium erythraea	Small centaury	5
Ceratonia siliqua	St. John's-bread	21
Chelidonium majus	Swallowwort	168
Chimaphila umbellata	Prince's-pine	6
Cichorium intybus	Radicchio	28
Cinnamomum camphora	Kuso-no-ki	7
Cinnamomum cassia	Cinnamon	29701
Cinnamomum verum	Cinnamon	21215
Citrus aurantium	Suan cheng	52
Citrus sinensis	Valencia orange	35
Codonopsis pilosula	Dang shen	9
Cola nitida	Kola	4
Commiphora mukul	Indian bdellium-tree	44
Coptis chinensis	Huanglian	5
Coriandrum sativum	Parsley- Chinese	42
Corylus avellana	Hazelnut tree	7
Crataegus laevigata	Woodland hawthorn	854
Cuminum cyminum	Cumin	4
Curcuma longa	Turmeric	57778
Curcuma zedoaria	Karcura	34
Daucus carota	Carrot	2
Dioscorea villosa	Wild yam	36
Echinacea angustifolia	Echinacea	8856
Echinacea purpurea	Echinacea	4428
Eleutherococcus senticosus	Siberian ginseng	88
Elymus repens	Couchgrass	2
Epimedium grandif	Yin yang huo	60
Epimedium grandiflorum	Yin yang huo	60
Equisetum hyemale	Mu zei	90
Eriobotrya japonica	Pi ba	14
Eupatorium purpureum	Sweet joe-pye-weed	8
Euterpe oleracea	Cabbage palm	25
Ficus carica	Wu hua guo	12
Filipendula ulmaria	Xuan guo wen zi cao	5
Foeniculum vulgare	Fennel	63
Frangula alnus	Glossy buckthorn	8
Fucus vesiculosus	Seawrack	100
Fumaria officinalis	Wax-dolls	4
Galium aparine	Goose-grass	4
Ganoderma lucidum	Reishi	572
Garcinia gummi	Malabartamarind	40
Gentiana lutea	Yellow gentian	75
Geranium maculatum	Wild geranium	8
Ginkgo biloba	Ginkgo	12630
Glycine max	Soybean	4466
Glycyrrhiza glabra	Liquorice	72
Glycyrrhiza uralensis	Licorice	32
Gymnema sylvestre	Miracle-fruit	156
Hamamelis virginiana	Witch-hazel	180
Handroanthus heptaphyllus	Pau d'Arco	45
Harpagophytum procumbens	Wood spider	476
Hedera helix	Ivy leaf	104
Hordeum vulgare	Barley grass	84
Hydrastis canadensis	Yellow root	110
Hypericum perforatum	St. John's-wort	2564
Ilex paraguariensis	Yerba mate	35
Inonotus obliquus	Chaga	18
Inula helenium	Velvet-dock	56
Juglans nigra	Blackwalnut	2
Juglans regia	Walnut	10
Juniperus communis	Juniper	6
Leonurus cardiaca	Motherwort	12
Lepidium meyenii	Maca	432
Linum usitatissimum	Flax seed	39800
Lonicera japonica	Rendongteng	5
Lycium barbarum	Goji berry	56
Magnolia officinalis	Magnolia-bark	3
Marrubium vulgare	Horehound	12
Matricaria chamomilla	Chamomile	1296
Melissa officinalis	Melissa	80
Mentha spicata	Spearmint	282
Mimosa pudica	Touch-me-not	4
Mitragyna speciosa	Kratom	1716
Momordica charantia	Carilla gourd	91
Morinda citrifolia	Rotten cheesefruit	100
Moringa oleifera	Moringa	632
Myrtus communis	Myrtle	4
Nepeta cataria	Field balm	12
Nerium oleander	Rose laurel	15
Notopterygium incisum	Qiang huo	3
Ocimum tenuiflorum	Tulsi	210
Olea europaea	Olive tree	44
Origanum majorana	Sweet marjoram	6
Origanum vulgare	Oregano	702
Panax ginseng	Panax ginseng	1746
Papaver rhoeas	Shirley poppy	5
Passiflora edulis	Purplegranadilla	4
Paullinia cupana	Guarana	132
Pausinystalia johimbe	Yohimbe	2
Petroselinum crispum	Parsley	38
Peumus boldus	Boldu	16
Phyllanthus amarus	Phyllanthusamarus	4
Pinus palustris	Pitch pine	3
Piper methysticum	Kava	360
Piper nigrum	White pepper	30
Plantago major	Plantain	55
Prunella vulgaris	Xiakucao	7
Prunus armeniaca	Xing	4
Prunus serotina	Black cherry	96
Quassia amara	Quassia wood	3
Quercus alba	White oak	45
Reynoutria multiflora	He shou wu	8
Rheum australe	zang bian da huang	5
Rheum palmatum	Zhang ye da huang	135
Rhodiola rosea	Rhodiola	855
Rosa canina	Dog rose	36
Rosmarinus officinalis	Rosemary	88
Rumex crispus	Yellow dock	20
Ruscus aculeatus	Butcher's-broom	120
Ruta graveolens	Rue	6
Salvia officinalis	Sage	275
Sambucus canadensis	Sweet Elder	1020
Sambucus nigra	Sweet Elder	1020
Sanguinaria canadensis	Tetterwort	7
Schisandra chinensis	Wu wei zi	48
Scutellaria lateriflora	Virginian skullcap	120
Serenoa repens	Scrub-palmetto	15635
Silybum marianum	Milk thistle	26110
Swertia chirata	Kiratatikta	52
Symphytum officinale	Slippery-root	72
Tamarindus indica	Tamarind	25
Tanacetum parthenium	Feverfew	306
Taraxacum officinale	Lion's-tooth	668
Thymus vulgaris	Thyme	112
Trametes versicolor	Yunzhi	36
Tribulus terrestris	Tribulus	195
Trigonella foenum	Fenugreek	570
Triticum aestivum	Wheat grass	100
Turnera diffusa	Damiana	14
Ulmus rubra	Slippery elm	285
Uncaria tomentosa	Cat's-claw	242
Urtica dioica	Stinging nettle	469
Valeriana officinalis	Valerian	3165
Verbascum thapsus	Velvet-dock	140
Viburnum opulus	Guelder rose	3
Vitex agnus	Vitex	116
Vitis rotundifolia	Muscadine grape	12
Vitis vinifera	Grapevine	40
Withania somnifera	Ashwaganda	2261
Yucca filamentosa	Yucca	156
Zingiber officinale	Ginger	4938
 */

select * from staging_vocabulary.concept c where c.concept_id < 0 and  c.concept_name like 'Rheum%]';
-- Rheum	zang bian da huang

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
    and c1.concept_id =  
;

-- break down of specific products?


/*
EMPTY B/C WE DID NOT INCLUDE LICORICE IN THE REFERENCE SET FOR SPRING 2022
*/

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
    and c1.concept_id = '-7001837'
;












--------------------------------------------

-- napdi_np_vocab_workflow_feb2022 
--
-- Queries ran to update the staging_vocabulary to have NP latin binomials, common names, preferred common names, constituents
-- spelling variations, and relationships 



-----------

/*
cem_pitt_2022=# drop  EXTENSION dblink; 
DROP EXTENSION
cem_pitt_2022=# CREATE EXTENSION dblink schema scratch_oct2022_np_vocab;
CREATE EXTENSION
*/


-- SELECT dblink_connect('g_substance_reg', 'hostaddr=127.0.0.1 port=5432 dbname=g_substance_reg user= password=');  -- NOTE: edit username and pword

----- NOTE: This won't work until Sanya has a chance to recreate her scratch folder 
-- SELECT dblink_connect(hostaddr=127.0.0.1 port=5432 'g_substance_reg', 'dbname=gsrs_2022 user= password=');  -- NOTE: edit username and pword

/*
   SELECT * FROM dblink('g_substance_reg', 
   						'select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np.substance_uuid concept_code 
						 from scratch_oct2022_np_vocab.lb_to_common_names_tsv inner join scratch_oct2022_np_vocab.test_srs_np 
						 on lb_to_common_names_tsv.latin_binomial = test_srs_np.related_latin_binomial
 						 union
					     select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np_part.parent_substance_uuid concept_code 
						 from scratch_oct2022_np_vocab.lb_to_common_names_tsv inner join scratch_oct2022_np_vocab.test_srs_np_part 
						 on lb_to_common_names_tsv.latin_binomial = test_srs_np_part.related_latin_binomial
						 ') 
   						AS lb_to_common_names(latin_binomial varchar, common_name varchar, preferred integer, concept_code varchar)
   						
   						SELECT * FROM dblink('g_substance_reg', 
   						'select distinct related_latin_binomial, constituent_name, constituent_uuid concept_code 
						 from scratch_oct2022_np_vocab.test_srs_np_constituent') 
   						AS lb_to_constituent(latin_binomial varchar, constituent_name varchar, concept_code varchar)
 */
select distinct latin_binomial from (
	select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np.substance_uuid concept_code 
	from scratch_oct2022_np_vocab.lb_to_common_names_tsv inner join scratch_oct2022_np_vocab.test_srs_np 
	            on lb_to_common_names_tsv.latin_binomial = test_srs_np.related_latin_binomial
 	union
	select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np_part.substance_uuid concept_code 
	from scratch_oct2022_np_vocab.lb_to_common_names_tsv inner join scratch_oct2022_np_vocab.test_srs_np_part 
	  		 	on lb_to_common_names_tsv.latin_binomial = test_srs_np_part.related_latin_binomial
)t
;
-- 290

select distinct latin_binomial from (
	select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np.substance_uuid concept_code 
	from scratch_oct2022_np_vocab.lb_to_common_names_tsv inner join scratch_oct2022_np_vocab.test_srs_np 
	            on lb_to_common_names_tsv.latin_binomial = test_srs_np.related_latin_binomial
)t
;
-- 290



--N=    matches in RxNorm for all NP terms (query time= minutes)
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
--insert into scratch_oct2022_np_vocab.np_rxnorm_substring_temp (concept_id_rxnorm, 
 -- concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm, 
 -- concept_class_id_napdi, concept_code, valid_start_date, valid_end_date)
select distinct rxns.concept_id_rxnorm, rxns.concept_name_rxnorm, nps.concept_id_napdi, 
  nps.concept_name_napdi, rxns.concept_class_id_rxnorm, nps.concept_class_id_napdi, nps.concept_code, 
  rxns.valid_start_date, rxns.valid_end_date
from rxns inner join nps on regexp_match(rxns.concept_name_rxnorm, concat('[ /]',nps.concept_name_napdi,'[ $]')) != '{}'  
where nps.concept_name_napdi = 'TEA' 
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
), target_strs as (
  -- select concat(nps.concept_name_napdi, ' ') s, concept_name_napdi,  concept_id_napdi, concept_code, concept_class_id_napdi
  -- from nps
  -- union
  -- select concat(' ',nps.concept_name_napdi) s, concept_name_napdi, concept_id_napdi, concept_code, concept_class_id_napdi
  -- from nps
  -- union
  select concat(' ',nps.concept_name_napdi,' ') s, concept_name_napdi, concept_id_napdi, concept_code, concept_class_id_napdi
  from nps
)
--insert into scratch_oct2022_np_vocab.np_rxnorm_substring_temp (concept_id_rxnorm, 
 -- concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm, 
 -- concept_class_id_napdi, concept_code, valid_start_date, valid_end_date)
select distinct rxns.concept_id_rxnorm, rxns.concept_name_rxnorm, target_strs.concept_id_napdi, 
  target_strs.concept_name_napdi, rxns.concept_class_id_rxnorm, target_strs.concept_class_id_napdi, target_strs.concept_code, 
  rxns.valid_start_date, rxns.valid_end_date
-- from rxns inner join nps on rxns.concept_name_rxnorm like concat('%',nps.concept_name_napdi,'%')
  from rxns inner join target_strs on rxns.concept_name_rxnorm like concat('%',target_strs.s,'%')  
 where target_strs.concept_name_napdi = 'ACAI'     
--  where target_strs.concept_name_napdi = 'TEA'
;