-- napdi_np_vocab_workflow_feb2022 
--
-- Queries ran to update the staging_vocabulary to have NP latin binomials, common names, preferred common names, constituents
-- spelling variations, and relationships 

-- Search path for this workflow
set search_path to scratch_feb2022_np_vocab;

-- Step 1) Obtaining NP latin binomials to common name mapping

-- Obtain the table of latin binomials to common names that Sanya created from G-SRS and 
-- mark preferred names using either ones we identified from manual mapping or by selecting
-- the first common name

/*
cem=# drop  EXTENSION dblink; 
DROP EXTENSION
cem=# CREATE EXTENSION dblink schema scratch_feb2022_np_vocab;
CREATE EXTENSION
*/
SELECT dblink_connect('g_substance_reg', 'dbname=g_substance_reg user= password=');  -- NOTE: edit username and pword

drop table if exists scratch_feb2022_np_vocab.lb_to_common_name_and_pt;
with remote_lb as (
   SELECT * FROM dblink('g_substance_reg', 
   						'select distinct latin_binomial, common_name, cast(null as integer) preferred, test_srs_np.substance_uuid concept_code 
						 from scratch_sanya.lb_to_common_names_tsv inner join scratch_sanya.test_srs_np 
						 on lb_to_common_names_tsv.latin_binomial = test_srs_np.related_latin_binomial') 
   						AS lb_to_common_names(latin_binomial varchar, common_name varchar, preferred integer, concept_code varchar)
), curated_pt as (
  select distinct latin_binomial, common_name,
         case when nfrs.lookup_value is not null then 1
              else null 
         end preferred
  from remote_lb left outer join scratch_sanya.np_faers_reference_set nfrs on upper(remote_lb.common_name) = upper(nfrs.lookup_value)
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
into scratch_feb2022_np_vocab.lb_to_common_name_and_pt
from remote_lb inner join all_pt on remote_lb.latin_binomial = all_pt.latin_binomial 
group by remote_lb.latin_binomial, remote_lb.common_name, all_pt.pt
order by latin_binomial 
;
-- 958

select * from scratch_feb2022_np_vocab.lb_to_common_name_and_pt where pt = 'Aloe vera';

-- Step 2) Obtaining NP latin binomials to constituent mapping 
drop table if exists scratch_feb2022_np_vocab.lb_to_constituent;
with remote_const as (
   SELECT * FROM dblink('g_substance_reg', 
   						'select distinct related_latin_binomial, constituent_name, constituent_uuid concept_code 
						 from scratch_sanya.test_srs_np_constituent') 
   						AS lb_to_constituent(latin_binomial varchar, constituent_name varchar, concept_code varchar)
)
select *
into scratch_feb2022_np_vocab.lb_to_constituent
from remote_const
;
-- 984 


-- Step 3) Obtaining manually curated spelling variations
select distinct related_latin_binomial latin_binomial, related_common_name pt, drug_name_original spelling_variation
into scratch_feb2022_np_vocab.lb_to_spelling_var
from scratch_sanya.np_faers_reference_set nfrs 
order by latin_binomial 
;
-- 2679

/* 
 -- Step 4) adding relationships, concepts, and concept relationship mappings
 
 concept table: 
 - add concepts napdi_has_const, napdi_is_const_of, napdi_pt, napdi_is_pt_of, napdi_spell_vr, napdi_is_spell_vr_of
 
 relationship table: 
 - add relationships: napdi_has_const/napdi_is_const_of; napdi_pt/napdi_is_pt_of; napdi_spell_vr/napdi_is_spell_vr_of
 
 concept table:
 - add all of the common names from scratch_feb2022_np_vocab.lb_to_common_name_and_pt
 - add all of the LBs from scratch_feb2022_np_vocab.lb_to_common_name_and_pt
 - add all of the constituents from scratch_feb2022_np_vocab.lb_to_constituent
 - add all of the spelling variations from scratch_feb2022_np_vocab.lb_to_spelling_var

 concept_relationship: 
 -- from scratch_feb2022_np_vocab.lb_to_common_name_and_pt to napdi_pt/napdi_is_pt_of
 -- from scratch_feb2022_np_vocab.lb_to_constituent to napdi_has_const/napdi_is_const_of
 -- from scratch_feb2022_np_vocab.lb_to_spelling_var to napdi_spell_vr/napdi_is_spell_vr_of
 

 */
create temporary sequence if not exists napdi_concept_sequence as integer increment by -1 maxvalue -7000000;

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
									valid_start_date, valid_end_date, invalid_reason) 
								VALUES (-9900000, 'NaPDI custom terms', 'Metadata', 'Domain', 'Domain', '', 'OMOP generated', 
									'2000-01-01', '2099-02-22', '');
INSERT INTO staging_vocabulary.domain (domain_id, domain_name, domain_concept_id) VALUES ('NaPDI research', 'NaPDI research', -9900000);
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

-- add concepts napdi_has_const, napdi_is_const_of, napdi_pt, napdi_is_pt_of, napdi_spell_vr, napdi_is_spell_vr_of
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
end;
-- 6


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
 - add all of the common names from scratch_feb2022_np_vocab.lb_to_common_name_and_pt
 - add all of the LBs from scratch_feb2022_np_vocab.lb_to_common_name_and_pt
 - add all of the constituents from scratch_feb2022_np_vocab.lb_to_constituent
 - add all of the spelling variations from scratch_feb2022_np_vocab.lb_to_spelling_var
*/
START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), concat(lbpt.common_name, '[', lbpt.latin_binomial, ']'), 'NaPDI research', 'NAPDI', lbpt.pt, '', lbpt.max, 
										'2000-01-01', '2099-02-22', ''
	   from scratch_feb2022_np_vocab.lb_to_common_name_and_pt lbpt
	   ;
end;
-- 958

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with pt as (select distinct lbpt.pt from scratch_feb2022_np_vocab.lb_to_common_name_and_pt lbpt)
	   select nextval('napdi_concept_sequence'), pt.pt, 'NaPDI research', 'NAPDI', 'NaPDI Preferred Term', '', pt.pt, 
										'2000-01-01', '2099-02-22', ''
	   from pt
	   ;
end;
-- 295

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with distinct_lbs as (select distinct lbpt.latin_binomial, lbpt.pt, lbpt.max from scratch_feb2022_np_vocab.lb_to_common_name_and_pt lbpt)
  	     select nextval('napdi_concept_sequence'), concat(distinct_lbs.latin_binomial, '[', distinct_lbs.latin_binomial, ']'), 'NaPDI research', 'NAPDI', distinct_lbs.pt, '', distinct_lbs.max, 
										'2000-01-01', '2099-02-22', ''
	     from distinct_lbs
	     where concat(distinct_lbs.latin_binomial, '[', distinct_lbs.latin_binomial, ']') not in (select c.concept_name from staging_vocabulary.concept c where c.vocabulary_id = 'NAPDI')
	   ;
end;
-- 300

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with distinct_const as (select distinct lbconst.constituent_name, lbconst.concept_code from scratch_feb2022_np_vocab.lb_to_constituent lbconst)
	   select nextval('napdi_concept_sequence'), distinct_const.constituent_name, 'NaPDI research', 'NAPDI', 'NaPDI NP Constituent', '', distinct_const.concept_code, 
										'2000-01-01', '2099-02-22', ''
	   from distinct_const
	   ;
end;
-- 737



START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), left(lbspell.spelling_variation, 255), 'NaPDI research', 'NAPDI', 'NaPDI NP Spelling Variation', '', lbspell.pt, 
										'2000-01-01', '2099-02-22', ''
	   from scratch_feb2022_np_vocab.lb_to_spelling_var lbspell
	   ;
end;
-- 2679


/*
relationship and concept_relationship: 
 -- from scratch_feb2022_np_vocab.lb_to_common_name_and_pt to napdi_pt/napdi_is_pt_of
 -- from scratch_feb2022_np_vocab.lb_to_constituent to napdi_has_const/napdi_is_const_of 
 -- from scratch_feb2022_np_vocab.lb_to_spelling_var to napdi_spell_vr/napdi_is_spell_vr_of
*/
start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        with distinct_cn as (select distinct common_name, latin_binomial, pt from scratch_feb2022_np_vocab.lb_to_common_name_and_pt)
		select c1.concept_id,  c2.concept_id,  'napdi_pt', '2000-01-01', '2099-02-22', ''
        from distinct_cn lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.common_name, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 958

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        with distinct_cn as (select distinct common_name, latin_binomial, pt from scratch_feb2022_np_vocab.lb_to_common_name_and_pt)
		select c2.concept_id, c1.concept_id, 'napdi_is_pt_of', '2000-01-01', '2099-02-22', ''
        from distinct_cn lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.common_name, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 958

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
		with distinct_lb as (select distinct latin_binomial, pt from scratch_feb2022_np_vocab.lb_to_common_name_and_pt)
		select c1.concept_id,  c2.concept_id,  'napdi_pt', '2000-01-01', '2099-02-22', ''
        from distinct_lb lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.latin_binomial, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 303 

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
		with distinct_lb as (select distinct latin_binomial, pt from scratch_feb2022_np_vocab.lb_to_common_name_and_pt)        
		select c2.concept_id, c1.concept_id, 'napdi_is_pt_of', '2000-01-01', '2099-02-22', ''
        from distinct_lb lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.latin_binomial, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 303 

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        select c1.concept_id, c2.concept_id, 'napdi_has_const', '2000-01-01', '2099-02-22', ''
        from scratch_feb2022_np_vocab.lb_to_constituent lbconst 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbconst.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on lbconst.constituent_name  = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id != 'NaPDI NP Spelling Variation' 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Constituent'
        ;
end;
-- 4916


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        select c2.concept_id, c1.concept_id,  'napdi_is_const_of', '2000-01-01', '2099-02-22', ''
        from scratch_feb2022_np_vocab.lb_to_constituent lbconst 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbconst.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on lbconst.constituent_name  = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id != 'NaPDI NP Spelling Variation' 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Constituent'
        ;
end;
-- 4916

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c1.concept_id, c2.concept_id, 'napdi_spell_vr', '2000-01-01', '2099-02-22', ''
        from scratch_feb2022_np_vocab.lb_to_spelling_var lbspell 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbspell.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on left(lbspell.spelling_variation, 255) = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Constituent')
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Spelling Variation'        
        ;
end;
-- 11815


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c2.concept_id, c1.concept_id,  'napdi_is_spell_vr_of', '2000-01-01', '2099-02-22', ''
        from scratch_feb2022_np_vocab.lb_to_spelling_var lbspell 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbspell.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on left(lbspell.spelling_variation, 255) = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Constituent')
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Spelling Variation'        
        ;
end;
-- 11815

----------------- SCRATCH -------------------

-- Test using Kratom 
select * from staging_vocabulary.concept c where concept_class_id = 'Kratom';
/*
concept_id	concept_name	domain_id	vocabulary_id	concept_class_id	standard_concept	concept_code	valid_start_date	valid_end_date	invalid_reason
-7000583	Kratom[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		d469b67d-e9a6-459f-b209-c59451936336	2000-01-01	2099-02-22	
-7000584	Kratum[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		d469b67d-e9a6-459f-b209-c59451936336	2000-01-01	2099-02-22	
-7001354	Mitragyna speciosa[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		d469b67d-e9a6-459f-b209-c59451936336	2000-01-01	2099-02-22	
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7000583'
;
/*
Kratom[Mitragyna speciosa]	-7000583	Kratom	-7001114
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
   and c1.concept_id = '-7001114'
;
/*
Kratom	-7001114	Mitragyna speciosa[Mitragyna speciosa]	-7001354
Kratom	-7001114	Kratum[Mitragyna speciosa]	-7000584
Kratom	-7001114	Kratom[Mitragyna speciosa]	-7000583
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7001354'
;
/*
Mitragyna speciosa[Mitragyna speciosa]	-7001354	SPECIOGYNINE	-7002260
Mitragyna speciosa[Mitragyna speciosa]	-7001354	SPECIOCILIATIN	-7002004
Mitragyna speciosa[Mitragyna speciosa]	-7001354	PAYNANTHEINE	-7001982
Mitragyna speciosa[Mitragyna speciosa]	-7001354	MITRAGYNINE	-7001824
Mitragyna speciosa[Mitragyna speciosa]	-7001354	7-HYDROXYMITRAGYNINE	-7001808
Mitragyna speciosa[Mitragyna speciosa]	-7001354	MITRAPHYLLINE	-7001567
*/

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = '-7001114'
;
/*
Kratom	-7001114	7-HYDROXYMITRAGYNINE	-7001808
Kratom	-7001114	MITRAGYNINE	-7001824
Kratom	-7001114	MITRAPHYLLINE	-7001567
Kratom	-7001114	PAYNANTHEINE	-7001982
Kratom	-7001114	SPECIOCILIATIN	-7002004
Kratom	-7001114	SPECIOGYNINE	-7002260 
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = '-7001824'
;
/*
MITRAGYNINE	-7001824	Mitragyna speciosa[Mitragyna speciosa]	-7001354
MITRAGYNINE	-7001824	Kratum[Mitragyna speciosa]	-7000584
MITRAGYNINE	-7001824	Kratom[Mitragyna speciosa]	-7000583
 */

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = '-7001824'
;
/*
MITRAGYNINE	-7001824	Kratom	-7001114
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = '-7000583'
;
/*
Kratom[Mitragyna speciosa]	-7000583	WHOLE HERBS PREMIUM MAENG DA KRATOM	-7003825
Kratom[Mitragyna speciosa]	-7000583	WHITE MAENG DA KRATOM 250G	-7003824
Kratom[Mitragyna speciosa]	-7000583	WHITE MAENG DA HERBAL TEA KRATOM	-7003823
Kratom[Mitragyna speciosa]	-7000583	VIVA ZEN KRATOM	-7003822
Kratom[Mitragyna speciosa]	-7000583	VIVAZEN BOTANICALS MAENG DA KRATOM	-7003821
Kratom[Mitragyna speciosa]	-7000583	UNICORN DUST STRAIN OF KRATOM	-7003820
Kratom[Mitragyna speciosa]	-7000583	TRAIN WRECK KRATOM	-7003819
Kratom[Mitragyna speciosa]	-7000583	TAUNTON BAY SOAP COMPANY RED VEIN TEA-1LB. PACKAGE (KRATOM)	-7003818
Kratom[Mitragyna speciosa]	-7000583	SUPERIOR RED DRAGON KRATOM	-7003817
Kratom[Mitragyna speciosa]	-7000583	SUPER GREEN KRATOM POWDER	-7003816
Kratom[Mitragyna speciosa]	-7000583	SUPER GREEN HORN KRATOM	-7003815
Kratom[Mitragyna speciosa]	-7000583	SLOW-MO HIPPO KRATOM	-7003814
Kratom[Mitragyna speciosa]	-7000583	R.H. NATURAL PRODUCTS KRATOM	-7003813
Kratom[Mitragyna speciosa]	-7000583	RED VEIN MAENG DA (KRATOM)	-7003812
Kratom[Mitragyna speciosa]	-7000583	RED VEIN KRATOM	-7003811
Kratom[Mitragyna speciosa]	-7000583	RED VEIN BORNEO KRATOM	-7003810
Kratom[Mitragyna speciosa]	-7000583	RED THAI KRATOM	-7003809
Kratom[Mitragyna speciosa]	-7000583	RED MAENG DA KRATOM (MITRAGYNA SPECIOSA)	-7003808
Kratom[Mitragyna speciosa]	-7000583	RED DEVIL KRATOM WATER SOLUBLE CBD	-7003807
Kratom[Mitragyna speciosa]	-7000583	RED BORNEO KRATOM BUMBLE BEE	-7003806
Kratom[Mitragyna speciosa]	-7000583	RED BALI KRATOM	-7003805
Kratom[Mitragyna speciosa]	-7000583	RAW FORM ORGANICS MAENG DA 150 RUBY CAPSULES KRATOM	-7003804
Kratom[Mitragyna speciosa]	-7000583	PREMIUM RED MAENG DA KRATOM	-7003803
Kratom[Mitragyna speciosa]	-7000583	PREMIUM RED MAENG DA CRAZY KRATOM	-7003802
Kratom[Mitragyna speciosa]	-7000583	PREMIUM KRATOM PHOENIX RED VEIN BALI	-7003801
Kratom[Mitragyna speciosa]	-7000583	POWDERED KRATOM	-7003800
Kratom[Mitragyna speciosa]	-7000583	O.P.M.S. LIQUID KRATOM	-7003799
Kratom[Mitragyna speciosa]	-7000583	O.P.M.S. KRATOM	-7003798
Kratom[Mitragyna speciosa]	-7000583	NUTRIZONE KRATOM PAIN OUT MAENG DA	-7003797
Kratom[Mitragyna speciosa]	-7000583	NATURE'S REMEDY KRATOM	-7003796
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNINE (KRATOM)	-7003795
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNINE KRATOM	-7003794
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNA SPECIOSA (MITRAGYNINE)	-7003793
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNA SPECIOSA LEAF	-7003792
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME) (KRATOM)	-7003791
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME)	-7003790
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNA SPECIOSA KORTHALS	-7003789
Kratom[Mitragyna speciosa]	-7000583	MITRAGYNA SPECIOSA	-7003788
Kratom[Mitragyna speciosa]	-7000583	MAENG DA POWDER KRATOM HERBAL DIETARY SUPPLEMENT	-7003787
Kratom[Mitragyna speciosa]	-7000583	MAENG DA KRATOM MITROGYNA SPECIOSA	-7003786
Kratom[Mitragyna speciosa]	-7000583	MAENG DA KRATOM	-7003785
Kratom[Mitragyna speciosa]	-7000583	LYFT PREMIUM BALI KRATOM HERBAL SUPPLEMENT POWDER	-7003784
Kratom[Mitragyna speciosa]	-7000583	LUCKY SEVEN KRATOM	-7003783
Kratom[Mitragyna speciosa]	-7000583	KRAVE KRATOM	-7003782
Kratom[Mitragyna speciosa]	-7000583	KRAVE, BLUE MAGIC KRATOM	-7003781
Kratom[Mitragyna speciosa]	-7000583	KRATOM- USED SUPER GREEN, GREEN BALI, RED MAGNEA DA	-7003780
Kratom[Mitragyna speciosa]	-7000583	KRATOM SUPPLEMENT	-7003779
Kratom[Mitragyna speciosa]	-7000583	KRATOM - SPECIFICALLY ^GREEN MALAYSIAN^	-7003778
Kratom[Mitragyna speciosa]	-7000583	KRATOM SILVER THAI	-7003777
Kratom[Mitragyna speciosa]	-7000583	KRATOM RED DRAGON	-7003776
Kratom[Mitragyna speciosa]	-7000583	KRATOM POWDER	-7003775
Kratom[Mitragyna speciosa]	-7000583	KRATOM (MITRAGYNINE)	-7003774
Kratom[Mitragyna speciosa]	-7000583	KRATOM (MITRAGYNA SPECIOSA LEAF)	-7003773
Kratom[Mitragyna speciosa]	-7000583	KRATOM MITRAGYNA SPECIOSA	-7003772
Kratom[Mitragyna speciosa]	-7000583	KRATOM (MITRAGYNA SPECIOSA)	-7003771
Kratom[Mitragyna speciosa]	-7000583	KRATOM (MITRAGYNA) (MITRAGYNINE)	-7003770
Kratom[Mitragyna speciosa]	-7000583	KRATOM (MITRAGYNA)	-7003769
Kratom[Mitragyna speciosa]	-7000583	KRATOM MAGNA RED	-7003768
Kratom[Mitragyna speciosa]	-7000583	(KRATOM) KRAOMA.COM TRANQUIL KRAOMA	-7003767
Kratom[Mitragyna speciosa]	-7000583	KRATOM INDO	-7003766
Kratom[Mitragyna speciosa]	-7000583	KRATOM IN A UNMARKED BAG	-7003765
Kratom[Mitragyna speciosa]	-7000583	KRATOM HERBAL SUPPLEMENT	-7003764
Kratom[Mitragyna speciosa]	-7000583	KRATOM GREEN MAGNA DA	-7003763
Kratom[Mitragyna speciosa]	-7000583	KRATOM EXTRACT	-7003762
Kratom[Mitragyna speciosa]	-7000583	KRATOM ELEPHANT WHITE THAI	-7003761
Kratom[Mitragyna speciosa]	-7000583	KRATOM CAPSULES	-7003760
Kratom[Mitragyna speciosa]	-7000583	KRATOM 3 OZ.	-7003759
Kratom[Mitragyna speciosa]	-7000583	KRATOM	-7003758
Kratom[Mitragyna speciosa]	-7000583	KRAKEN KRATOM	-7003757
Kratom[Mitragyna speciosa]	-7000583	KRABOT KRATOM FINELY GROUND POWDER	-7003756
Kratom[Mitragyna speciosa]	-7000583	KLARITY KRATOM: MAENG DA CAPSULES	-7003755
Kratom[Mitragyna speciosa]	-7000583	INDO KRATOM	-7003754
Kratom[Mitragyna speciosa]	-7000583	HERBAL SUBSTANCE KRATOM	-7003753
Kratom[Mitragyna speciosa]	-7000583	HERBAL SALVATION KRATOM	-7003752
Kratom[Mitragyna speciosa]	-7000583	GREEN STRAIN TROPICAL KRATOM	-7003751
Kratom[Mitragyna speciosa]	-7000583	GREEN M BATIK AND RED BATIK KRATOM	-7003750
Kratom[Mitragyna speciosa]	-7000583	GREEN MALAY KRATOM	-7003749
Kratom[Mitragyna speciosa]	-7000583	GREEN BORNEO KRATOM	-7003748
Kratom[Mitragyna speciosa]	-7000583	FEELIN' GROOVY KRATOM	-7003747
Kratom[Mitragyna speciosa]	-7000583	EMERALD LEAF BALI KRATOM (HERBALSMITRAGYNINE)	-7003746
Kratom[Mitragyna speciosa]	-7000583	EMERALD KRATOM POWDER	-7003745
Kratom[Mitragyna speciosa]	-7000583	EARTH KRATOM ORGANIC RED MAENG DA	-7003744
Kratom[Mitragyna speciosa]	-7000583	CLUB 13 KRATOM MAENG DA RED 90GM	-7003743
Kratom[Mitragyna speciosa]	-7000583	CAROLINA KRATOM RED JONGKONG 100 GRAM POWDER	-7003742
Kratom[Mitragyna speciosa]	-7000583	CALCIUM KRATOMOS	-7003741
Kratom[Mitragyna speciosa]	-7000583	BRILLIANT ELIXIR, CHOCOLATE LOVER W/ KRATOM	-7003740
Kratom[Mitragyna speciosa]	-7000583	BLUE MAGIC KRAVE KRATOM	-7003739
Kratom[Mitragyna speciosa]	-7000583	BALI KRATOM	-7003738
*/


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
   and c1.concept_id = '-7003743'
;
/*
CLUB 13 KRATOM MAENG DA RED 90GM	-7003743	Mitragyna speciosa[Mitragyna speciosa]	-7001354
CLUB 13 KRATOM MAENG DA RED 90GM	-7003743	Kratum[Mitragyna speciosa]	-7000584
CLUB 13 KRATOM MAENG DA RED 90GM	-7003743	Kratom[Mitragyna speciosa]	-7000583
 */

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = '-7001114'
;
/*
Kratom	-7001114	BALI KRATOM	-7003738
Kratom	-7001114	BLUE MAGIC KRAVE KRATOM	-7003739
Kratom	-7001114	BRILLIANT ELIXIR, CHOCOLATE LOVER W/ KRATOM	-7003740
Kratom	-7001114	CALCIUM KRATOMOS	-7003741
Kratom	-7001114	CAROLINA KRATOM RED JONGKONG 100 GRAM POWDER	-7003742
Kratom	-7001114	CLUB 13 KRATOM MAENG DA RED 90GM	-7003743
Kratom	-7001114	EARTH KRATOM ORGANIC RED MAENG DA	-7003744
Kratom	-7001114	EMERALD KRATOM POWDER	-7003745
Kratom	-7001114	EMERALD LEAF BALI KRATOM (HERBALSMITRAGYNINE)	-7003746
Kratom	-7001114	FEELIN' GROOVY KRATOM	-7003747
Kratom	-7001114	GREEN BORNEO KRATOM	-7003748
Kratom	-7001114	GREEN MALAY KRATOM	-7003749
Kratom	-7001114	GREEN M BATIK AND RED BATIK KRATOM	-7003750
Kratom	-7001114	GREEN STRAIN TROPICAL KRATOM	-7003751
Kratom	-7001114	HERBAL SALVATION KRATOM	-7003752
Kratom	-7001114	HERBAL SUBSTANCE KRATOM	-7003753
Kratom	-7001114	INDO KRATOM	-7003754
Kratom	-7001114	KLARITY KRATOM: MAENG DA CAPSULES	-7003755
Kratom	-7001114	KRABOT KRATOM FINELY GROUND POWDER	-7003756
Kratom	-7001114	KRAKEN KRATOM	-7003757
Kratom	-7001114	KRATOM	-7003758
Kratom	-7001114	KRATOM 3 OZ.	-7003759
Kratom	-7001114	KRATOM CAPSULES	-7003760
Kratom	-7001114	KRATOM ELEPHANT WHITE THAI	-7003761
Kratom	-7001114	KRATOM EXTRACT	-7003762
Kratom	-7001114	KRATOM GREEN MAGNA DA	-7003763
Kratom	-7001114	KRATOM HERBAL SUPPLEMENT	-7003764
Kratom	-7001114	KRATOM IN A UNMARKED BAG	-7003765
Kratom	-7001114	KRATOM INDO	-7003766
Kratom	-7001114	(KRATOM) KRAOMA.COM TRANQUIL KRAOMA	-7003767
Kratom	-7001114	KRATOM MAGNA RED	-7003768
Kratom	-7001114	KRATOM (MITRAGYNA)	-7003769
Kratom	-7001114	KRATOM (MITRAGYNA) (MITRAGYNINE)	-7003770
Kratom	-7001114	KRATOM (MITRAGYNA SPECIOSA)	-7003771
Kratom	-7001114	KRATOM MITRAGYNA SPECIOSA	-7003772
Kratom	-7001114	KRATOM (MITRAGYNA SPECIOSA LEAF)	-7003773
Kratom	-7001114	KRATOM (MITRAGYNINE)	-7003774
Kratom	-7001114	KRATOM POWDER	-7003775
Kratom	-7001114	KRATOM RED DRAGON	-7003776
Kratom	-7001114	KRATOM SILVER THAI	-7003777
Kratom	-7001114	KRATOM - SPECIFICALLY ^GREEN MALAYSIAN^	-7003778
Kratom	-7001114	KRATOM SUPPLEMENT	-7003779
Kratom	-7001114	KRATOM- USED SUPER GREEN, GREEN BALI, RED MAGNEA DA	-7003780
Kratom	-7001114	KRAVE, BLUE MAGIC KRATOM	-7003781
Kratom	-7001114	KRAVE KRATOM	-7003782
Kratom	-7001114	LUCKY SEVEN KRATOM	-7003783
Kratom	-7001114	LYFT PREMIUM BALI KRATOM HERBAL SUPPLEMENT POWDER	-7003784
Kratom	-7001114	MAENG DA KRATOM	-7003785
Kratom	-7001114	MAENG DA KRATOM MITROGYNA SPECIOSA	-7003786
Kratom	-7001114	MAENG DA POWDER KRATOM HERBAL DIETARY SUPPLEMENT	-7003787
Kratom	-7001114	MITRAGYNA SPECIOSA	-7003788
Kratom	-7001114	MITRAGYNA SPECIOSA KORTHALS	-7003789
Kratom	-7001114	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME)	-7003790
Kratom	-7001114	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME) (KRATOM)	-7003791
Kratom	-7001114	MITRAGYNA SPECIOSA LEAF	-7003792
Kratom	-7001114	MITRAGYNA SPECIOSA (MITRAGYNINE)	-7003793
Kratom	-7001114	MITRAGYNINE KRATOM	-7003794
Kratom	-7001114	MITRAGYNINE (KRATOM)	-7003795
Kratom	-7001114	NATURE'S REMEDY KRATOM	-7003796
Kratom	-7001114	NUTRIZONE KRATOM PAIN OUT MAENG DA	-7003797
Kratom	-7001114	O.P.M.S. KRATOM	-7003798
Kratom	-7001114	O.P.M.S. LIQUID KRATOM	-7003799
Kratom	-7001114	POWDERED KRATOM	-7003800
Kratom	-7001114	PREMIUM KRATOM PHOENIX RED VEIN BALI	-7003801
Kratom	-7001114	PREMIUM RED MAENG DA CRAZY KRATOM	-7003802
Kratom	-7001114	PREMIUM RED MAENG DA KRATOM	-7003803
Kratom	-7001114	RAW FORM ORGANICS MAENG DA 150 RUBY CAPSULES KRATOM	-7003804
Kratom	-7001114	RED BALI KRATOM	-7003805
Kratom	-7001114	RED BORNEO KRATOM BUMBLE BEE	-7003806
Kratom	-7001114	RED DEVIL KRATOM WATER SOLUBLE CBD	-7003807
Kratom	-7001114	RED MAENG DA KRATOM (MITRAGYNA SPECIOSA)	-7003808
Kratom	-7001114	RED THAI KRATOM	-7003809
Kratom	-7001114	RED VEIN BORNEO KRATOM	-7003810
Kratom	-7001114	RED VEIN KRATOM	-7003811
Kratom	-7001114	RED VEIN MAENG DA (KRATOM)	-7003812
Kratom	-7001114	R.H. NATURAL PRODUCTS KRATOM	-7003813
Kratom	-7001114	SLOW-MO HIPPO KRATOM	-7003814
Kratom	-7001114	SUPER GREEN HORN KRATOM	-7003815
Kratom	-7001114	SUPER GREEN KRATOM POWDER	-7003816
Kratom	-7001114	SUPERIOR RED DRAGON KRATOM	-7003817
Kratom	-7001114	TAUNTON BAY SOAP COMPANY RED VEIN TEA-1LB. PACKAGE (KRATOM)	-7003818
Kratom	-7001114	TRAIN WRECK KRATOM	-7003819
Kratom	-7001114	UNICORN DUST STRAIN OF KRATOM	-7003820
Kratom	-7001114	VIVAZEN BOTANICALS MAENG DA KRATOM	-7003821
Kratom	-7001114	VIVA ZEN KRATOM	-7003822
Kratom	-7001114	WHITE MAENG DA HERBAL TEA KRATOM	-7003823
Kratom	-7001114	WHITE MAENG DA KRATOM 250G	-7003824
Kratom	-7001114	WHOLE HERBS PREMIUM MAENG DA KRATOM	-7003825
 */

