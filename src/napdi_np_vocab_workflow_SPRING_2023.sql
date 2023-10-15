-- napdi_np_vocab_workflow_SPRING_2023.sql 
--
-- Queries ran to update the vocabulary to have NP latin binomials, common names, preferred common names, constituents
-- spelling variations, RxNorm mappings, combination products, and relationships 

-- Search path for this workflow
set search_path to scratch_spring2023_np_vocab;

-- Step 1) Obtaining NP latin binomials to common name mapping

-- Obtain the table of latin binomials to common names that Sanya created from G-SRS and 
-- mark preferred names using either ones we identified from manual mapping or by selecting
-- the first common name

/*
cem=# drop  EXTENSION dblink; 
DROP EXTENSION
cem=# CREATE EXTENSION dblink schema scratch_spring2023_np_vocab;
CREATE EXTENSION
*/
-- SELECT dblink_connect('g_substance_reg', 'dbname=g_substance_reg user=rw_grp password=rw_grp');  -- NOTE: edit username and pword
SELECT dblink_connect('g_substance_reg', 'host=localhost port=5432 dbname=g_substance_reg user=rw_grp password=rw_grp');  -- NOTE: edit username and pword

/* -- delete later
   						'select distinct related_latin_binomial, common_name, cast(null as integer) preferred, test_srs_np.substance_uuid concept_code 
						 from scratch_sanya_2023.test_srs_np left join scratch_sanya_2023.lb_to_common_names_tsv  
						 on lb_to_common_names_tsv.latin_binomial = test_srs_np.related_latin_binomial')
 
  select distinct latin_binomial, common_name,
         case when nfrs.lookup_value is not null then 1
              else null 
         end preferred
  from remote_lb left outer join scratch_sanya_2023.np_faers_reference_set nfrs on upper(remote_lb.common_name) = upper(nfrs.lookup_value)
  
),
 
 */

-- drop table if exists scratch_spring2023_np_vocab.lb_to_common_name_and_pt;
with remote_lb_partial as (
   SELECT * FROM dblink('g_substance_reg', 
   						'select distinct test_srs_np.related_latin_binomial, test_srs_np.substance_uuid concept_code 
						 from scratch_sanya_2023.test_srs_np') 
   						AS lb_to_common_names(latin_binomial varchar, concept_code varchar)
), remote_lb as (
   select l.latin_binomial, l.concept_code, r.common_name, cast(null as integer) preferred
   from remote_lb_partial l  
      left join scratch_spring2023_np_vocab.lb_to_common_names_tsv r on l.latin_binomial = r.latin_binomial
), curated_pt as (   
   select distinct latin_binomial, pt common_name, 1 preferred
   from scratch_spring2023_np_vocab.combined_lb_to_spelling_var cltsv 
   union
   select distinct latin_binomial, pt common_name, 1 preferred
   from scratch_spring2023_np_vocab.faers_herbal_strings_annotated
   union 
   select latin_binomial, common_name, preferred
   from remote_lb   
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
select remote_lb.latin_binomial, remote_lb.common_name, all_pt.pt, max(remote_lb.concept_code) concept_code
into scratch_spring2023_np_vocab.lb_to_common_name_and_pt
from remote_lb inner join all_pt on remote_lb.latin_binomial = all_pt.latin_binomial 
group by remote_lb.latin_binomial, remote_lb.common_name, all_pt.pt
order by latin_binomial 
;
-- Updated Rows	11115

-- make  fixes to cinnamomum burmani
select *
from scratch_spring2023_np_vocab.lb_to_common_name_and_pt p
where p.latin_binomial like '%Cinnamomum burman%'
;
/*
latin_binomial	common_name	pt	concept_code
Cinnamomum burmanni	Bataviacinnamon	Bataviacinnamon	eade8733-1819-4816-a05a-86d60639c181
Cinnamomum burmannii		Cinnamon	652df5f1-853d-4e5b-be97-8c2881d210e0
*/

update scratch_spring2023_np_vocab.lb_to_common_name_and_pt
set latin_binomial = 'Cinnamomum burmanni', common_name = 'Cinnamon'
where latin_binomial = 'Cinnamomum burmannii'
;

update scratch_spring2023_np_vocab.lb_to_common_name_and_pt
set pt  = 'Cinnamon'
where common_name  = 'Bataviacinnamon'
;

update scratch_spring2023_np_vocab.lb_to_common_name_and_pt
set concept_code  = 'eade8733-1819-4816-a05a-86d60639c181'
where concept_code  = '652df5f1-853d-4e5b-be97-8c2881d210e0'
;

select *
from scratch_spring2023_np_vocab.lb_to_common_name_and_pt p
where p.latin_binomial like '%Cinnamomum burman%'
;
/*
Cinnamomum burmanni	Cinnamon	Cinnamon	eade8733-1819-4816-a05a-86d60639c181
Cinnamomum burmanni	Bataviacinnamon	Cinnamon	eade8733-1819-4816-a05a-86d60639c181
*/

--- NOTE: we also notice that cannabis has CBD and cannabidiol as common names but these should be constituents. 
--  For now, we will keep these in but later remove them.

drop table if exists scratch_spring2023_np_vocab.lb_to_constituent;
with remote_const as (
   SELECT * FROM dblink('g_substance_reg', 
   						'select distinct related_latin_binomial, constituent_name, constituent_uuid concept_code 
						 from scratch_sanya_2023.test_srs_np_constituent') 
   						AS lb_to_common_names(latin_binomial varchar, concept_name varchar, concept_code varchar)
)
select *
into scratch_spring2023_np_vocab.lb_to_constituent
from remote_const
;
-- 1683

-- Step 3) Obtaining manually curated spelling variations
select distinct latin_binomial, pt, spelling_variation, combination_product
into scratch_spring2023_np_vocab.lb_to_spelling_var
from
(
 select distinct latin_binomial, pt, spelling_variation, combination_product
 from scratch_spring2023_np_vocab.combined_lb_to_spelling_var cltsv 
 union
 select distinct latin_binomial, pt, spelling_variation, combination_product
 from scratch_spring2023_np_vocab.faers_herbal_strings_annotated fhsa 
) t
order by latin_binomial 
;
-- 3952 -- up from 3460 in December
-- Adds the wierd HERBAL\* strings that started in late 2021


/* 
 -- Step 4) adding relationships, concepts, and concept relationship mappings
 
 concept table: 
 - add concepts napdi_has_const, napdi_is_const_of, napdi_pt, napdi_is_pt_of, napdi_spell_vr, napdi_is_spell_vr_of
 
 relationship table: 
 - add relationships: napdi_has_const/napdi_is_const_of; napdi_pt/napdi_is_pt_of; napdi_spell_vr/napdi_is_spell_vr_of; napdi_np_maps_to; napdi_const_maps_to
 
 concept table:
 - add all of the common names from scratch_spring2023_np_vocab.lb_to_common_name_and_pt
 - add all of the LBs from scratch_spring2023_np_vocab.lb_to_common_name_and_pt
 - add all of the constituents from scratch_spring2023_np_vocab.lb_to_constituent
 - add all of the spelling variations from scratch_spring2023_np_vocab.lb_to_spelling_var

 concept_relationship: 
 -- from scratch_spring2023_np_vocab.lb_to_common_name_and_pt to napdi_pt/napdi_is_pt_of
 -- from scratch_spring2023_np_vocab.lb_to_constituent to napdi_has_const/napdi_is_const_of
 -- from scratch_spring2023_np_vocab.lb_to_spelling_var to napdi_spell_vr/napdi_is_spell_vr_of
 

 */
-- drop sequence napdi_concept_sequence; 
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
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
									valid_start_date, valid_end_date, invalid_reason) 
								VALUES (-9990004, 'NaPDI NP Combination Product', 'Metadata', 'Concept Class', 'Concept Class', '', 'OMOP generated', 
									'2000-01-01', '2099-02-22', '');
end;
-- 7

-- add concepts napdi_has_const, napdi_is_const_of, napdi_pt, napdi_is_pt_of, napdi_spell_vr, napdi_is_spell_vr_of, napdi_const_maps_to, napdi_np_maps_to,
--              napdi_is_combo, napdi_used_in_combo
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
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_pt_to_combo', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');									
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason) 
									VALUES (nextval('napdi_concept_sequence'), 'napdi_combo_to_pt', 'Metadata', 'Relationship', 'Relationship', '', 'OMOP generated', 
										'2000-01-01', '2099-02-22', '');
end;
-- 10


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
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_combo_to_pt'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_pt_to_combo')
                            select 'napdi_pt_to_combo', 'NaPDI NP preferred term mapped to containing combination product', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;                           
insert into staging_vocabulary.relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id)
							with inv_rel as (select c.concept_id inv_rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_pt_to_combo'),
  							     rel as (select c.concept_id rel_id from staging_vocabulary.concept c where c.concept_name = 'napdi_combo_to_pt')
                            select 'napdi_combo_to_pt', 'NaPDI combination product mapped to preferred term', 0, 0, inv_rel_id, rel_id 
                            from inv_rel cross join rel 
                            ;                          
end;
-- 8

/*
concept table:
 - add all of the common names from scratch_spring2023_np_vocab.lb_to_common_name_and_pt
 - add all of the LBs from scratch_spring2023_np_vocab.lb_to_common_name_and_pt
 - add all of the constituents from scratch_spring2023_np_vocab.lb_to_constituent
 - add all of the spelling variations from scratch_spring2023_np_vocab.lb_to_spelling_var
 - add all of the combo products from np_to_rxnorm_annotated and combined_lb_to_spelling_var and faers_herbal_strings_annotated
*/
START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), concat(lbpt.common_name, '[', lbpt.latin_binomial, ']'), 'NaPDI research', 'NAPDI', lbpt.pt, '', lbpt.concept_code, 
										'2000-01-01', '2099-02-22', ''
	   from scratch_spring2023_np_vocab.lb_to_common_name_and_pt lbpt
	   where lbpt.common_name is not null
	   ;
end;
-- 909 

START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with pt as (select distinct lbpt.pt from scratch_spring2023_np_vocab.lb_to_common_name_and_pt lbpt)
	   select nextval('napdi_concept_sequence'), pt.pt, 'NaPDI research', 'NAPDI', 'NaPDI Preferred Term', '', pt.pt, 
										'2000-01-01', '2099-02-22', ''
	   from pt
	   ;
end;
-- 285


START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with distinct_lbs as (select distinct lbpt.latin_binomial, lbpt.pt, lbpt.concept_code from scratch_spring2023_np_vocab.lb_to_common_name_and_pt lbpt)
  	     select nextval('napdi_concept_sequence'), concat(distinct_lbs.latin_binomial, '[', distinct_lbs.latin_binomial, ']'), 'NaPDI research', 'NAPDI', distinct_lbs.pt, '', distinct_lbs.concept_code, 
										'2000-01-01', '2099-02-22', ''
	     from distinct_lbs
	     where distinct_lbs.pt is not null 
	   ;
end;
-- 909
-- 10206 : correct because the sum of this and 909 = 11115 which is the total number we started with


START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       with distinct_const as (select distinct lbconst.concept_name, lbconst.concept_code from scratch_spring2023_np_vocab.lb_to_constituent lbconst)
	   select nextval('napdi_concept_sequence'), distinct_const.concept_name, 'NaPDI research', 'NAPDI', 'NaPDI NP Constituent', '', distinct_const.concept_code, 
										'2000-01-01', '2099-02-22', ''
	   from distinct_const
	   ;
end;
-- 1171 


START TRANSACTION;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), left(lbspell.spelling_variation, 255), 'NaPDI research', 'NAPDI', 'NaPDI NP Spelling Variation', '', lbspell.pt, 
										'2000-01-01', '2099-02-22', ''
	   from scratch_spring2023_np_vocab.lb_to_spelling_var lbspell
	   ;
end;
-- 3952

START TRANSACTION;
with combos as (
 select distinct left(t.combo, 255) combo  -- NOTE: truncated to 255 characters -- might need to change that later!
 from 
 (
  select cltsv.spelling_variation combo
  from scratch_spring2023_np_vocab.combined_lb_to_spelling_var cltsv 
  where cltsv.combination_product = 'yes'
  union 
  select fhsa.spelling_variation combo
  from scratch_spring2023_np_vocab.faers_herbal_strings_annotated fhsa 
  where fhsa.combination_product = 'yes'
  union 
  select ntra.concept_name_rxnorm combo
  from np_to_rxnorm_annotated ntra
  where ntra.combination_product = 'yes'
 ) t
)
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), combos.combo, 'NaPDI research', 'NAPDI', 'NaPDI NP Combination Product', '', '', 
										'2000-01-01', '2099-02-22', ''
	   from combos
	   ;
end;
-- 1318

/*
relationship and concept_relationship: 
 -- from scratch_spring2023_np_vocab.lb_to_common_name_and_pt to napdi_pt/napdi_is_pt_of
 -- from scratch_spring2023_np_vocab.lb_to_constituent to napdi_has_const/napdi_is_const_of 
 -- from scratch_spring2023_np_vocab.lb_to_spelling_var to napdi_spell_vr/napdi_is_spell_vr_of
 -- from scratch_spring2023_np_vocab.faers_herbal_strings_annotated to napdi_combo_to_pt/napdi_pt_to_combo
*/
start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        with distinct_cn as (select distinct common_name, latin_binomial, pt from scratch_spring2023_np_vocab.lb_to_common_name_and_pt)
		select c1.concept_id,  c2.concept_id,  'napdi_pt', '2000-01-01', '2099-02-22', ''
        from distinct_cn lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.common_name, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 912

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        with distinct_cn as (select distinct common_name, latin_binomial, pt from scratch_spring2023_np_vocab.lb_to_common_name_and_pt)
		select c2.concept_id, c1.concept_id, 'napdi_is_pt_of', '2000-01-01', '2099-02-22', ''
        from distinct_cn lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.common_name, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 912


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
		with distinct_lb as (select distinct latin_binomial, pt from scratch_spring2023_np_vocab.lb_to_common_name_and_pt)
		select c1.concept_id,  c2.concept_id,  'napdi_pt', '2000-01-01', '2099-02-22', ''
        from distinct_lb lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.latin_binomial, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 296


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
		with distinct_lb as (select distinct latin_binomial, pt from scratch_spring2023_np_vocab.lb_to_common_name_and_pt)        
		select c2.concept_id, c1.concept_id, 'napdi_is_pt_of', '2000-01-01', '2099-02-22', ''
        from distinct_lb lbpt 
               inner join staging_vocabulary.concept c1 on concat(lbpt.latin_binomial, '[', lbpt.latin_binomial, ']') = c1.concept_name
               inner join staging_vocabulary.concept c2 on lbpt.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = lbpt.pt 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'
        ;
end;
-- 296


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        select c1.concept_id, c2.concept_id, 'napdi_has_const', '2000-01-01', '2099-02-22', ''
        from scratch_spring2023_np_vocab.lb_to_constituent lbconst 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbconst.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on lbconst.concept_name  = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id != 'NaPDI NP Spelling Variation' 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Constituent'
        ;
end;
-- 4990


start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
        select c2.concept_id, c1.concept_id,  'napdi_is_const_of', '2000-01-01', '2099-02-22', ''
        from scratch_spring2023_np_vocab.lb_to_constituent lbconst 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbconst.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on lbconst.concept_name  = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id != 'NaPDI NP Spelling Variation' 
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Constituent'
        ;
end;
-- 4990

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c1.concept_id, c2.concept_id, 'napdi_spell_vr', '2000-01-01', '2099-02-22', ''
        from scratch_spring2023_np_vocab.lb_to_spelling_var lbspell 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbspell.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on left(lbspell.spelling_variation, 255) = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Constituent')
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Spelling Variation'        
        ;
end;
-- 26558
-- (OLD - 14137 in Dec 2022 and 10805 in spring 2022 

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c2.concept_id, c1.concept_id,  'napdi_is_spell_vr_of', '2000-01-01', '2099-02-22', ''
        from scratch_spring2023_np_vocab.lb_to_spelling_var lbspell 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbspell.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on left(lbspell.spelling_variation, 255) = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Constituent')
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Spelling Variation'        
        ;
end;
-- 26558
-- (OLD - 14137 in Dec 2022 and 10805 in spring 2022

start transaction;
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c1.concept_id, c2.concept_id, '', '2000-01-01', '2099-02-22', ''
        from scratch_spring2023_np_vocab.lb_to_spelling_var lbspell 
               inner join staging_vocabulary.concept c1 on c1.concept_name like concat('%',lbspell.latin_binomial,'%')
               inner join staging_vocabulary.concept c2 on left(lbspell.spelling_variation, 255) = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Constituent')
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI NP Spelling Variation'        
        ;
end;
-- 26558
-- (OLD - 14137 in Dec 2022 and 10805 in spring 2022 

-- napdi_combo_to_pt/napdi_pt_to_combo
start transaction;
with combos as (
 select distinct left(t.combo, 255) combo, pt  -- NOTE: truncated to 255 characters -- might need to change that later!
 from 
 (
  select cltsv.spelling_variation combo, cltsv.pt
  from scratch_spring2023_np_vocab.combined_lb_to_spelling_var cltsv 
  where cltsv.combination_product = 'yes'
  union 
  select fhsa.spelling_variation combo, fhsa.pt
  from scratch_spring2023_np_vocab.faers_herbal_strings_annotated fhsa 
  where fhsa.combination_product = 'yes'
  union 
  select ntra.concept_name_rxnorm combo, ntra.concept_name_napdi pt
  from scratch_spring2023_np_vocab.np_to_rxnorm_annotated ntra
  where ntra.combination_product = 'yes'
 ) t
)
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c1.concept_id, c2.concept_id, 'napdi_combo_to_pt', '2000-01-01', '2099-02-22', ''
        from combos 
           inner join staging_vocabulary.concept c1 on upper(combos.combo) = upper(left(c1.concept_name, 255))
           inner join staging_vocabulary.concept c2 on combos.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = 'NaPDI NP Combination Product'
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'        
        ;
end;
-- 723


start transaction;
with combos as (
 select distinct left(t.combo, 255) combo, pt  -- NOTE: truncated to 255 characters -- might need to change that later!
 from 
 (
  select cltsv.spelling_variation combo, cltsv.pt
  from scratch_spring2023_np_vocab.combined_lb_to_spelling_var cltsv 
  where cltsv.combination_product = 'yes'
  union 
  select fhsa.spelling_variation combo, fhsa.pt
  from scratch_spring2023_np_vocab.faers_herbal_strings_annotated fhsa 
  where fhsa.combination_product = 'yes'
  union 
  select ntra.concept_name_rxnorm combo, ntra.concept_name_napdi pt
  from scratch_spring2023_np_vocab.np_to_rxnorm_annotated ntra
  where ntra.combination_product = 'yes'
 ) t
)
insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason)
    	select c2.concept_id, c1.concept_id, 'napdi_pt_to_combo', '2000-01-01', '2099-02-22', ''
        from combos 
           inner join staging_vocabulary.concept c1 on upper(combos.combo) = upper(left(c1.concept_name, 255))
           inner join staging_vocabulary.concept c2 on combos.pt = c2.concept_name
        where c1.vocabulary_id = 'NAPDI' and c1.concept_class_id = 'NaPDI NP Combination Product'
          and c2.vocabulary_id = 'NAPDI' and c2.concept_class_id = 'NaPDI Preferred Term'        
        ;
end;
-- 723


--
----- RxNorm NP mappings -------------------
--
drop table if exists scratch_spring2023_np_vocab.np_to_rxnorm;
create table if not exists scratch_spring2023_np_vocab.np_to_rxnorm (
concept_id_rxnorm int4 not null,
concept_id_napdi int4 not null,
concept_name_rxnorm varchar,
concept_name_napdi varchar,
concept_class_id_rxnorm varchar,
concept_code varchar,
valid_start_date date,
valid_end_date date
);

drop table if exists scratch_spring2023_np_vocab.np_const_to_rxnorm;
create table if not exists scratch_spring2023_np_vocab.np_const_to_rxnorm (
concept_id_rxnorm int4 not null,
concept_id_napdi int4 not null,
concept_name_rxnorm varchar,
concept_name_napdi varchar,
concept_class_id_rxnorm varchar,
concept_code varchar,
valid_start_date date,
valid_end_date date
);

drop table if exists scratch_spring2023_np_vocab.np_rxnorm_substring_temp;
create table if not exists scratch_spring2023_np_vocab.np_rxnorm_substring_temp (
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
-- truncate scratch_spring2023_np_vocab.np_const_to_rxnorm;
with nps as ( 
 select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
 concept_class_id np_concept_class_id
 from staging_vocabulary.concept c 
 where c.vocabulary_id ='NAPDI'
) 
insert into scratch_spring2023_np_vocab.np_const_to_rxnorm
  (concept_id_rxnorm, concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
   concept_code, valid_start_date, valid_end_date)
select c2.concept_id concept_id_rxnorm, upper(c2.concept_name) concept_name_rxnorm, nps.concept_id_napdi, 
	   nps.concept_name_napdi, c2.concept_class_id concept_class_id_rxnorm, nps.concept_code, 
	   c2.valid_start_date, c2.valid_end_date
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = upper(nps.concept_name_napdi)
where vocabulary_id ='RxNorm' and nps.np_concept_class_id = 'NaPDI NP Constituent'
;
-- 217

--rxnorm np name matches
-- truncate table scratch_spring2023_np_vocab.np_to_rxnorm;
with nps as ( 
 select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
     concept_class_id np_concept_class_id
 from staging_vocabulary.concept c 
 where c.vocabulary_id ='NAPDI'
) 
insert into scratch_spring2023_np_vocab.np_to_rxnorm 
(concept_id_rxnorm, concept_name_rxnorm, concept_id_napdi, concept_name_napdi, concept_class_id_rxnorm,
 concept_code, valid_start_date, valid_end_date)
select c2.concept_id concept_id_rxnorm, upper(c2.concept_name) concept_name_rxnorm, nps.concept_id_napdi, 
	   nps.concept_name_napdi, c2.concept_class_id concept_class_id_rxnorm, nps.concept_code, 
       c2.valid_start_date, c2.valid_end_date
from staging_vocabulary.concept c2 inner join nps on upper(c2.concept_name) = upper(nps.concept_name_napdi)
where vocabulary_id ='RxNorm' 
   and nps.np_concept_class_id in ('NaPDI NP Combination Product','NaPDI Preferred Term')
;
-- 581 (includes RxNorm combination product names already added to vocabulary based on manual review of the prior load)
-- (Dec 2022 : 16 ; Spring 2022 : 14 


--for all NPs - substring match
-- truncate table scratch_spring2023_np_vocab.np_rxnorm_substring_temp;
with nps as ( 
select distinct upper(regexp_replace(c.concept_name, '\[.*\]','' )) concept_name_napdi, concept_id concept_id_napdi, concept_code,
   concept_class_id concept_class_id_napdi
from staging_vocabulary.concept c 
where c.vocabulary_id ='NAPDI'
and c.concept_class_id not in ('NaPDI NP Spelling Variation', 'NaPDI NP Combination Product','NaPDI NP Constituent')
), rxns as (
 select distinct c2.concept_id concept_id_rxnorm, c2.vocabulary_id, c2.standard_concept, 
    upper(c2.concept_name) concept_name_rxnorm, c2.concept_class_id concept_class_id_rxnorm,
    c2.valid_start_date, c2.valid_end_date
 from staging_vocabulary.concept c2 
 where c2.vocabulary_id = 'RxNorm'
)
insert into scratch_spring2023_np_vocab.np_rxnorm_substring_temp (concept_id_rxnorm, 
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
-- 11646

-- Get just the NP mappings
-- NOTE: do not truncate this table here -- do it up above because you will have to rerun the prior query that created this table 
insert into scratch_spring2023_np_vocab.np_to_rxnorm (concept_id_rxnorm, concept_id_napdi, concept_name_rxnorm,
	  concept_name_napdi, concept_class_id_rxnorm, concept_code, valid_start_date, valid_end_date)
select npx.concept_id_rxnorm, npx.concept_id_napdi, upper(npx.concept_name_rxnorm) concept_name_rxnorm, upper(npx.concept_name_napdi) concept_name_napdi,
	   npx.concept_class_id_rxnorm, npx.concept_code, npx.valid_start_date, npx.valid_end_date 
from scratch_spring2023_np_vocab.np_rxnorm_substring_temp npx
   left join scratch_spring2023_np_vocab.np_to_rxnorm nprx on upper(npx.concept_name_rxnorm) = upper(nprx.concept_name_rxnorm) 
where npx.concept_class_id_napdi = 'NaPDI Preferred Term'
   and nprx.concept_id_rxnorm is null
-- 2555

   
-- LEFT OFF HERE
   
-- After manual review in a prior run, these were issues that needed fixed (napdi CONCEPT IDS are from the older run)
/*
 * Deletions
19102962	-7001929	EXCEDRIN QUICK TAB SPEARMINT	SPEARMINT	Brand Name	Spearmint	1970-01-01	2018-08-05
19048945	-7001718	TAO BRAND OF TROLEANDOMYCIN	TAO	Brand Name	Tao	1970-01-01	2099-12-31
36229133	-7001718	TAO BRAND OF TROLEANDOMYCIN ORAL PRODUCT	TAO	Branded Dose Group	Tao	2016-08-01	2099-12-31
36229134	-7001718	TAO BRAND OF TROLEANDOMYCIN PILL	TAO	Branded Dose Group	Tao	2016-08-01	2099-12-31
*/
delete from scratch_spring2023_np_vocab.np_to_rxnorm np where np.concept_id_rxnorm in (19102962,19048945,36229133,36229134);
-- 4


--- TODO: needs to update this approach to use the np_to_rxnorm_annotated table. 
-- IF NEEDED : drop table scratch_spring2023_np_vocab.temp_combos_not_yet_added; 
with t as (  -- distinct entries mis-mapped to NaPDI preferred terms -- 359
 select distinct *
 from scratch_spring2023_np_vocab.np_to_rxnorm ntr 
 where ntr.concept_name_rxnorm like '% / %'
   and ntr.concept_code != ''
), s as ( -- 53 which are present in the vocabulary having been brought in from the various manually annoted files
 select *
 from staging_vocabulary.concept c inner join t 
     on (
         upper(left(c.concept_name, 255)) = upper(left(t.concept_name_rxnorm, 255))
         or 
         upper(left(c.concept_name, 255)) = upper(left(t.concept_name_napdi, 255))
        )
 where c.vocabulary_id = 'NAPDI'
 and c.concept_class_id = 'NaPDI NP Combination Product'
)
select distinct t.*
into scratch_spring2023_np_vocab.temp_combos_not_yet_added
from t  
where upper(left(t.concept_name_rxnorm, 255)) not in (select concept_name from s)
; 
-- these 306 need to be added to the vocabulary and then edited in the np_to_rxnorm table


select *
from scratch_spring2023_np_vocab.np_to_rxnorm_annotated ntra 
  inner join scratch_spring2023_np_vocab.temp_combos_not_yet_added tcnya on upper(left(ntra.concept_name_rxnorm, 255)) = upper(left(tcnya.concept_name_rxnorm, 255))
; -- 43 that were in the manually identified RxNorm combo products. 
  -- Some of these were missed as combination products and some actually are not combination products (false positives from the '% / %' pattern
/* 
--- 'x' at the front means keep in the temp_combos_not_yet_added table prior. Remove the others prior to updating the vocabulary

19103427	-7012472	CAFFEINE 40 MG / GUARANA PREPARATION 60 MG ORAL CAPSULE	GUARANA		Clinical Drug	Guarana	1/1/70	3/31/19	19103427	-7001071	CAFFEINE 40 MG / GUARANA PREPARATION 60 MG ORAL CAPSULE	GUARANA	Clinical Drug	Guarana	1970-01-01	2019-03-31
36028487	-7012472	CAFFEINE / GUARANA PREPARATION	GUARANA		Multiple Ingredients	Guarana	8/30/10	12/31/99	36028487	-7001071	CAFFEINE / GUARANA PREPARATION	GUARANA	Multiple Ingredients	Guarana	2010-08-30	2099-12-31
40017308	-7012472	CAFFEINE / GUARANA PREPARATION ORAL CAPSULE	GUARANA		Clinical Drug Form	Guarana	1/1/70	3/31/19	40017308	-7001071	CAFFEINE / GUARANA PREPARATION ORAL CAPSULE	GUARANA	Clinical Drug Form	Guarana	1970-01-01	2019-03-31
36218108	-7012472	CAFFEINE / GUARANA PREPARATION ORAL PRODUCT	GUARANA		Clinical Dose Group	Guarana	8/1/16	3/31/19	36218108	-7001071	CAFFEINE / GUARANA PREPARATION ORAL PRODUCT	GUARANA	Clinical Dose Group	Guarana	2016-08-01	2019-03-31
36218109	-7012472	CAFFEINE / GUARANA PREPARATION PILL	GUARANA		Clinical Dose Group	Guarana	8/1/16	3/31/19	36218109	-7001071	CAFFEINE / GUARANA PREPARATION PILL	GUARANA	Clinical Dose Group	Guarana	2016-08-01	2019-03-31
x 1304234	-7012382	ECHINACEA ROOT EXTRACT 250 MG / ECHINACEA, AERIAL PARTS 250 MG ORAL CAPSULE	ECHINACEA		Clinical Drug	Echinacea	1/1/70	2/3/19	1304234	-7000980	ECHINACEA ROOT EXTRACT 250 MG / ECHINACEA, AERIAL PARTS 250 MG ORAL CAPSULE	ECHINACEA	Clinical Drug	Echinacea	1970-01-01	2019-02-03
x 36027502	-7012382	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS	ECHINACEA		Multiple Ingredients	Echinacea	8/30/10	12/31/99	36027502	-7000980	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS	ECHINACEA	Multiple Ingredients	Echinacea	2010-08-30	2099-12-31
x 40034357	-7012382	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS ORAL CAPSULE	ECHINACEA		Clinical Drug Form	Echinacea	1/1/70	2/3/19	40034357	-7000980	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS ORAL CAPSULE	ECHINACEA	Clinical Drug Form	Echinacea	1970-01-01	2019-02-03
x 36226678	-7012382	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS ORAL PRODUCT	ECHINACEA		Clinical Dose Group	Echinacea	8/1/16	2/3/19	36226678	-7000980	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS ORAL PRODUCT	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
x 36226679	-7012382	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS PILL	ECHINACEA		Clinical Dose Group	Echinacea	8/1/16	2/3/19	36226679	-7000980	ECHINACEA ROOT EXTRACT / ECHINACEA, AERIAL PARTS PILL	ECHINACEA	Clinical Dose Group	Echinacea	2016-08-01	2019-02-03
x 19121492	-7012447	GINKGO BILOBA EXTRACT 60 MG / GINKGO BILOBA LEAF EXTRACT 120 MG ORAL CAPSULE	GINKGO		Clinical Drug	Ginkgo	11/13/05	2/3/19	19121492	-7001047	GINKGO BILOBA EXTRACT 60 MG / GINKGO BILOBA LEAF EXTRACT 120 MG ORAL CAPSULE	GINKGO	Clinical Drug	Ginkgo	2005-11-13	2019-02-03
x 36028565	-7012447	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF	GINKGO		Multiple Ingredients	Ginkgo	8/30/10	12/31/99	36028565	-7001047	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF	GINKGO	Multiple Ingredients	Ginkgo	2010-08-30	2099-12-31
x 40126179	-7012447	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF EXTRACT ORAL CAPSULE	GINKGO		Clinical Drug Form	Ginkgo	11/13/05	2/3/19	40126179	-7001047	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF EXTRACT ORAL CAPSULE	GINKGO	Clinical Drug Form	Ginkgo	2005-11-13	2019-02-03
x 36220514	-7012447	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF EXTRACT ORAL PRODUCT	GINKGO		Clinical Dose Group	Ginkgo	8/1/16	2/3/19	36220514	-7001047	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF EXTRACT ORAL PRODUCT	GINKGO	Clinical Dose Group	Ginkgo	2016-08-01	2019-02-03
x 36220515	-7012447	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF EXTRACT PILL	GINKGO		Clinical Dose Group	Ginkgo	8/1/16	2/3/19	36220515	-7001047	GINKGO BILOBA EXTRACT / GINKGO BILOBA LEAF EXTRACT PILL	GINKGO	Clinical Dose Group	Ginkgo	2016-08-01	2019-02-03
x 19066368	-7012447	GINKGO BILOBA LEAF EXTRACT 120 MG / GINKGO BILOBA LEAF EXTRACT 60 MG ORAL CAPSULE	GINKGO		Clinical Drug	Ginkgo	1/1/70	2/3/19	19066368	-7001047	GINKGO BILOBA LEAF EXTRACT 120 MG / GINKGO BILOBA LEAF EXTRACT 60 MG ORAL CAPSULE	GINKGO	Clinical Drug	Ginkgo	1970-01-01	2019-02-03
x 36028692	-7012447	GINKGO BILOBA LEAF / GINKGO BILOBA LEAF EXTRACT	GINKGO		Multiple Ingredients	Ginkgo	8/30/10	12/31/99	36028692	-7001047	GINKGO BILOBA LEAF / GINKGO BILOBA LEAF EXTRACT	GINKGO	Multiple Ingredients	Ginkgo	2010-08-30	2099-12-31
x 40040770	-7012447	GINKGO BILOBA LEAF / GINKGO BILOBA LEAF EXTRACT ORAL CAPSULE	GINKGO		Clinical Drug Form	Ginkgo	1/1/70	2/3/19	40040770	-7001047	GINKGO BILOBA LEAF / GINKGO BILOBA LEAF EXTRACT ORAL CAPSULE	GINKGO	Clinical Drug Form	Ginkgo	1970-01-01	2019-02-03
19121507	-7012363	GREEN TEA EXTRACT 250 MG / GREEN TEA LEAF EXTRACT 150 MG ORAL CAPSULE	GREEN TEA		Clinical Drug	Green tea	11/13/05	2/3/19	19121507	-7000965	GREEN TEA EXTRACT 250 MG / GREEN TEA LEAF EXTRACT 150 MG ORAL CAPSULE	GREEN TEA	Clinical Drug	Green tea	2005-11-13	2019-02-03
x 19121503	-7012363	GREEN TEA EXTRACT 375 MG / THEAFLAVIN 84 MG ORAL CAPSULE	GREEN TEA		Clinical Drug	Green tea	11/13/05	2/3/19	19121503	-7000965	GREEN TEA EXTRACT 375 MG / THEAFLAVIN 84 MG ORAL CAPSULE	GREEN TEA	Clinical Drug	Green tea	2005-11-13	2019-02-03
40126245	-7012363	GREEN TEA EXTRACT / GREEN TEA LEAF EXTRACT ORAL CAPSULE	GREEN TEA		Clinical Drug Form	Green tea	11/13/05	2/3/19	40126245	-7000965	GREEN TEA EXTRACT / GREEN TEA LEAF EXTRACT ORAL CAPSULE	GREEN TEA	Clinical Drug Form	Green tea	2005-11-13	2019-02-03
36224332	-7012363	GREEN TEA EXTRACT / GREEN TEA LEAF EXTRACT ORAL PRODUCT	GREEN TEA		Clinical Dose Group	Green tea	8/1/16	2/3/19	36224332	-7000965	GREEN TEA EXTRACT / GREEN TEA LEAF EXTRACT ORAL PRODUCT	GREEN TEA	Clinical Dose Group	Green tea	2016-08-01	2019-02-03
36224333	-7012363	GREEN TEA EXTRACT / GREEN TEA LEAF EXTRACT PILL	GREEN TEA		Clinical Dose Group	Green tea	8/1/16	2/3/19	36224333	-7000965	GREEN TEA EXTRACT / GREEN TEA LEAF EXTRACT PILL	GREEN TEA	Clinical Dose Group	Green tea	2016-08-01	2019-02-03
x 40126160	-7012363	GREEN TEA EXTRACT / THEAFLAVIN ORAL CAPSULE	GREEN TEA		Clinical Drug Form	Green tea	11/13/05	2/3/19	40126160	-7000965	GREEN TEA EXTRACT / THEAFLAVIN ORAL CAPSULE	GREEN TEA	Clinical Drug Form	Green tea	2005-11-13	2019-02-03
x 36224334	-7012363	GREEN TEA EXTRACT / THEAFLAVIN ORAL PRODUCT	GREEN TEA		Clinical Dose Group	Green tea	8/1/16	2/3/19	36224334	-7000965	GREEN TEA EXTRACT / THEAFLAVIN ORAL PRODUCT	GREEN TEA	Clinical Dose Group	Green tea	2016-08-01	2019-02-03
x 36224335	-7012363	GREEN TEA EXTRACT / THEAFLAVIN PILL	GREEN TEA		Clinical Dose Group	Green tea	8/1/16	2/3/19	36224335	-7000965	GREEN TEA EXTRACT / THEAFLAVIN PILL	GREEN TEA	Clinical Dose Group	Green tea	2016-08-01	2019-02-03
x 19121505	-7012363	GREEN TEA LEAF EXTRACT 375 MG / THEAFLAVIN 84 MG ORAL CAPSULE	GREEN TEA		Clinical Drug	Green tea	11/13/05	2/3/19	19121505	-7000965	GREEN TEA LEAF EXTRACT 375 MG / THEAFLAVIN 84 MG ORAL CAPSULE	GREEN TEA	Clinical Drug	Green tea	2005-11-13	2019-02-03
36028903	-7012363	GREEN TEA LEAF EXTRACT / GREEN TEA PREPARATION	GREEN TEA		Multiple Ingredients	Green tea	8/30/10	12/31/99	36028903	-7000965	GREEN TEA LEAF EXTRACT / GREEN TEA PREPARATION	GREEN TEA	Multiple Ingredients	Green tea	2010-08-30	2099-12-31
x 36027178	-7012363	GREEN TEA LEAF EXTRACT / THEAFLAVIN	GREEN TEA		Multiple Ingredients	Green tea	8/30/10	12/31/99	36027178	-7000965	GREEN TEA LEAF EXTRACT / THEAFLAVIN	GREEN TEA	Multiple Ingredients	Green tea	2010-08-30	2099-12-31
x 40126246	-7012363	GREEN TEA LEAF EXTRACT / THEAFLAVIN ORAL CAPSULE	GREEN TEA		Clinical Drug Form	Green tea	11/13/05	2/3/19	40126246	-7000965	GREEN TEA LEAF EXTRACT / THEAFLAVIN ORAL CAPSULE	GREEN TEA	Clinical Drug Form	Green tea	2005-11-13	2019-02-03
x 36220445	-7012363	GREEN TEA LEAF EXTRACT / THEAFLAVIN ORAL PRODUCT	GREEN TEA		Clinical Dose Group	Green tea	8/1/16	2/3/19	36220445	-7000965	GREEN TEA LEAF EXTRACT / THEAFLAVIN ORAL PRODUCT	GREEN TEA	Clinical Dose Group	Green tea	2016-08-01	2019-02-03
x 36220446	-7012363	GREEN TEA LEAF EXTRACT / THEAFLAVIN PILL	GREEN TEA		Clinical Dose Group	Green tea	8/1/16	2/3/19	36220446	-7000965	GREEN TEA LEAF EXTRACT / THEAFLAVIN PILL	GREEN TEA	Clinical Dose Group	Green tea	2016-08-01	2019-02-03
x 36029160	-7012363	GREEN TEA PREPARATION / THEAFLAVIN	GREEN TEA		Multiple Ingredients	Green tea	8/30/10	12/31/99	36029160	-7000965	GREEN TEA PREPARATION / THEAFLAVIN	GREEN TEA	Multiple Ingredients	Green tea	2010-08-30	2099-12-31
19071233	-7012473	MILK THISTLE FRUIT 500 MG / MILK THISTLE FRUIT EXTRACT 140 MG ORAL CAPSULE	MILK THISTLE		Clinical Drug	Milk thistle	1/1/70	2/3/19	19071233	-7001072	MILK THISTLE FRUIT 500 MG / MILK THISTLE FRUIT EXTRACT 140 MG ORAL CAPSULE	MILK THISTLE	Clinical Drug	Milk thistle	1970-01-01	2019-02-03
36029077	-7012473	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT	MILK THISTLE		Multiple Ingredients	Milk thistle	8/30/10	12/31/99	36029077	-7001072	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT	MILK THISTLE	Multiple Ingredients	Milk thistle	2010-08-30	2099-12-31
40066341	-7012473	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT ORAL CAPSULE	MILK THISTLE		Clinical Drug Form	Milk thistle	1/1/70	2/3/19	40066341	-7001072	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT ORAL CAPSULE	MILK THISTLE	Clinical Drug Form	Milk thistle	1970-01-01	2019-02-03
36212353	-7012473	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT ORAL PRODUCT	MILK THISTLE		Clinical Dose Group	Milk thistle	8/1/16	2/3/19	36212353	-7001072	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT ORAL PRODUCT	MILK THISTLE	Clinical Dose Group	Milk thistle	2016-08-01	2019-02-03
36212354	-7012473	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT PILL	MILK THISTLE		Clinical Dose Group	Milk thistle	8/1/16	2/3/19	36212354	-7001072	MILK THISTLE FRUIT / MILK THISTLE FRUIT EXTRACT PILL	MILK THISTLE	Clinical Dose Group	Milk thistle	2016-08-01	2019-02-03
19038535	-7012561	SOY PROTEIN ISOLATE 65 MG / SOYBEAN PREPARATION 325 MG ORAL TABLET	SOYBEAN		Clinical Drug	Soybean	1/1/70	3/31/19	19038535	-7001159	SOY PROTEIN ISOLATE 65 MG / SOYBEAN PREPARATION 325 MG ORAL TABLET	SOYBEAN	Clinical Drug	Soybean	1970-01-01	2019-03-31
36028936	-7012561	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION	SOYBEAN		Multiple Ingredients	Soybean	8/30/10	12/31/99	36028936	-7001159	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION	SOYBEAN	Multiple Ingredients	Soybean	2010-08-30	2099-12-31
36224437	-7012561	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION ORAL PRODUCT	SOYBEAN		Clinical Dose Group	Soybean	8/1/16	3/31/19	36224437	-7001159	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION ORAL PRODUCT	SOYBEAN	Clinical Dose Group	Soybean	2016-08-01	2019-03-31
40101405	-7012561	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION ORAL TABLET	SOYBEAN		Clinical Drug Form	Soybean	1/1/70	3/31/19	40101405	-7001159	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION ORAL TABLET	SOYBEAN	Clinical Drug Form	Soybean	1970-01-01	2019-03-31
36224438	-7012561	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION PILL	SOYBEAN		Clinical Dose Group	Soybean	8/1/16	3/31/19	36224438	-7001159	SOY PROTEIN ISOLATE / SOYBEAN PREPARATION PILL	SOYBEAN	Clinical Dose Group	Soybean	2016-08-01	2019-03-31
*/

delete from scratch_spring2023_np_vocab.temp_combos_not_yet_added 
where concept_id_rxnorm in (19038535,19071233,19103427,19121507,36028487,36028903,36028936,36029077,36212353,36212354,36218108,36218109,36224332,36224333,36224437,36224438,40017308,40066341,40101405,40126245)
;
-- 20

start transaction;
INSERT INTO staging_vocabulary.concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, 
										valid_start_date, valid_end_date, invalid_reason)
       select nextval('napdi_concept_sequence'), tny.concept_name_rxnorm, 'NaPDI research', 'NAPDI', 'NaPDI NP Combination Product', '', '', 
										'2000-01-01', '2099-02-22', ''
	   from scratch_spring2023_np_vocab.temp_combos_not_yet_added tny
	   ;
end;
-- 296  (306 - 20 from above)

start transaction;
UPDATE scratch_spring2023_np_vocab.np_to_rxnorm 
 SET concept_id_napdi = t2.concept_id, 
    concept_code = null, 
    concept_name_napdi = t2.concept_name  
 FROM scratch_spring2023_np_vocab.temp_combos_not_yet_added t1   
   INNER JOIN staging_vocabulary.concept t2 ON t1.concept_name_rxnorm = t2.concept_name 
 where  np_to_rxnorm.concept_name_rxnorm  = t2.concept_name 
;
end;
-- updated rows 286
--- confirmed using several examples from the temp_combos_not_added table that the np_to_rxnorm tabke was correctly updated

--
-- Model for combination products
--   napdi PT to single NP products in RxNorm using np_maps_to
--   napdi concepts for combos to RxNorm combination products using np_maps_to
--   napdi PT to napdi concepts for combos using napdi_pt_to_combo (include RxNorm mapped and the rest mapped in from spelling variations and herbal strings)
--   napdi concepts for combos to PT using napdi_combo_to_pt
--  
-- Supported use cases: 
--   Query for all single NP products (RxNorm), common names, LB, and spelling variations and exclude anything noted as a combination product 
--   Query for all combination NP products (RxNorm and other) and spelling variations and exclude anything noted as a single NP product 
--   Query for all NPs PT/LB/common name that are mentioned in a combination products

-- Implementation of the model
--
--- napdi PT to single NP products in RxNorm using np_maps_to
-- Those rows with a non-null concept_code map from NP PT to the RxNorm product. Rows with a NULL concept_code map from the NP combination product to RxNorm
-- There is no need to qualify this because the table concept_id_napdi and concept_id_rxnorm are already set up correctly
start transaction;
 insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
		valid_start_date, valid_end_date, invalid_reason)
 select concept_id_napdi, concept_id_rxnorm,  'napdi_np_maps_to', valid_start_date, valid_end_date, ''
 from scratch_spring2023_np_vocab.np_to_rxnorm
 where concept_code is not null -- these are single NP ingredient products
 ;
end;
-- 2846
-- (Dec 2022 2369 ; Spring 2022 2353)
select *
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
 and c1.concept_class_id = 'NaPDI Preferred Term'
;
--
--
-- napdi concepts for combos to RxNorm combination products using np_maps_to
start transaction;
 insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
		valid_start_date, valid_end_date, invalid_reason)
 select concept_id_napdi, concept_id_rxnorm,  'napdi_np_maps_to', valid_start_date, valid_end_date, ''
 from scratch_spring2023_np_vocab.np_to_rxnorm
 where concept_code is null  -- these are combo NP products 
 ;
end;
-- 286 
--
select *
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
 and c1.concept_class_id = 'NaPDI NP Combination Product'
;
--
--
--
--
-- napdi PT to napdi concepts for combos using napdi_pt_to_combo (include RxNorm mapped and the rest mapped in from spelling variations and herbal strings)
---- currently, this is only partially completed. 
---- TODO: when we can connect these products to a database like the dietary supplement label database, we should be able to create the correct mappings
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt_to_combo'
;
--
--
-- napdi concepts for combos to PT using napdi_combo_to_pt
---- currently, this is only partially completed. 
---- TODO: when we can connect these products to a database like the dietary supplement label database, we should be able to create the correct mappings
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_combo_to_pt'
;


-----------

-- NOTE: -  constituent mappings need review!!!
-- 

--N= (exact+substring match of constituents)
start transaction;
 insert into staging_vocabulary.concept_relationship (concept_id_1, concept_id_2, relationship_id, 
		valid_start_date, valid_end_date, invalid_reason)
 select concept_id_napdi, concept_id_rxnorm,  'napdi_const_maps_to', valid_start_date, valid_end_date, ''
 from scratch_spring2023_np_vocab.np_const_to_rxnorm
 ;
end;
-- 217



-----------------------------------------------------------------------------

----- queries for sanity checks

-- obtaining the NP preferred terms
select *
from staging_vocabulary.concept c 
where vocabulary_id = 'NAPDI'
 and c.concept_class_id = 'NaPDI Preferred Term'
;

-- take the concept_id of the preferred term for the NP and use the napdi_is_pt_of relationship to get the common names/L.B
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
order by c1.concept_name, c2.concept_name 
;


-- going from the many-to-one mapped common name/L.B. to the preferred term concept_id using napdi_pt
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
order by c1.concept_name, c2.concept_name
;

-- Obtain the constituents of NPs using common name or LB 
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
order by c1.concept_name, c2.concept_name
;

-- going from PT to constituents associated with any of the common names/LB that the PT could represent
select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
order by c1.concept_name, c3.concept_name
;


-- Going from a constituent assocated with a common name or LB using the napdi_is_const_of
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
order by c1.concept_name, c2.concept_name
;


-- Going from a constituent assocated with a common name or LB using the napdi_is_const_of to the preferred term that represents that common or LB
select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
order by c1.concept_name, c3.concept_name
;
   

-- Model for combination products
--   napdi PT to single NP products in RxNorm using np_maps_to
--   napdi concepts for combos to RxNorm combination products using np_maps_to
--   napdi PT to napdi concepts for combos using napdi_pt_to_combo (include RxNorm mapped and the rest mapped in from spelling variations and herbal strings)
--   napdi concepts for combos to PT using napdi_combo_to_pt
--  

select *
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
 and c1.concept_class_id = 'NaPDI Preferred Term'
;
--

--
select *
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
 and c1.concept_class_id = 'NaPDI NP Combination Product'
;

-- napdi PT to napdi concepts for combos using napdi_pt_to_combo (include RxNorm mapped and the rest mapped in from spelling variations and herbal strings)
---- currently, this is only partially completed. 
---- TODO: when we can connect these products to a database like the dietary supplement label database, we should be able to create the correct mappings
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt_to_combo'
;
--


-- napdi concepts for combos to PT using napdi_combo_to_pt
---- currently, this is only partially completed. 
---- TODO: when we can connect these products to a database like the dietary supplement label database, we should be able to create the correct mappings
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_combo_to_pt'
;

-- Supported use cases (TODO - show examples of these - Sanya): 
--   Query for all single NP products (RxNorm), common names, LB, and spelling variations and exclude anything noted as a combination product 
--   Query for all combination NP products (RxNorm and other) and spelling variations and exclude anything noted as a single NP product 
--   Query for all NPs PT/LB/common name that are mentioned in a combination products


---------------------------------------------------------------------
-- Kratom test

select * from staging_vocabulary.concept c where concept_class_id = 'Kratom';


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = -7057580
;

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
  and c1.concept_id =  -7058095
;


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7057580
;

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7058095
;

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = -7059333
;


select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = -7059333
;



select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
   and c1.concept_id = -7062105
;

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7058095
;


select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7058280
;


--- Cannabis examples
select * from staging_vocabulary.concept c where concept_class_id = 'Cannabis';

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = -7000192
;

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7000192
;   
;

-- Cinnamon examples

-- select c1.concept_name, c1.concept_id, c2.concept_name, c2.concept_id 
select distinct c1.concept_name 
from staging_vocabulary.concept c1 
   inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
   inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_has_const' 
  and c1.concept_name ilike '%cinnamom%'
;

-- Note that burmannii and camphor do not have constituents which might be b/c of the lb_to_constituent table
