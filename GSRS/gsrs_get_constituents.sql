drop table if exists test_srs_np_constituent
create table test_srs_np_constituent (
	uuid varchar(40),
	substance_uuid varchar(40),
	constituent_uuid varchar(40),
	related_common_name varchar(255),
	related_latin_binomial varchar(255),
	relation_type varchar(255),
	constituent_name varchar(255),
	constituent_type varchar(20)
)
--count old = 993
select count(*) from scratch_sanya.test_srs_np_constituent_old tsnc 
--count = 1091
select count(*) from scratch_sanya.test_srs_np_constituent

insert into test_srs_np_constituent (substance_uuid, related_common_name, related_latin_binomial, uuid, relation_type, 
constituent_name, constituent_uuid)
select tsnpr.owner_uuid as substance_uuid, tsnpr.related_common_name, tsnpr.related_latin_binomial, tsnpr.uuid, tsnpr."type" as relation_type, 
igs.ref_pname as constituent_name, igs.refuuid as constituent_uuid
from test_srs_np_part_rel tsnpr
inner join public.ix_ginas_substanceref igs on tsnpr.related_substance_uuid = igs.uuid 
and tsnpr."type" in ('ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'ACTIVE CONSTITUENT ALWAYS PRESENT', 'CONSTITUENT ALWAYS PRESENT->PARENT',
'POSSIBLE ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'CONSTITUENT MAY BE PRESENT->PARENT')

insert into test_srs_np_constituent (substance_uuid, related_common_name, related_latin_binomial, uuid, relation_type,
constituent_name, constituent_uuid)
select tsnpr.owner_uuid as substance_uuid, tsnpr.related_common_name, tsnpr.related_latin_binomial, tsnpr.uuid, tsnpr."type" as relation_type, 
igs.ref_pname as constituent_name, igs.refuuid as constituent_uuid
from test_srs_np_rel tsnpr
inner join public.ix_ginas_substanceref igs on tsnpr.related_substance_uuid = igs.uuid 
and tsnpr."type" in ('ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'ACTIVE CONSTITUENT ALWAYS PRESENT', 'CONSTITUENT ALWAYS PRESENT->PARENT',
'POSSIBLE ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'CONSTITUENT MAY BE PRESENT->PARENT')
--test constituents for 'Milk thistle'
select * from scratch_sanya.test_srs_np_constituent tsnc 
where tsnc.related_latin_binomial = 'Cannabis sativa'
 
delete from test_srs_np_constituent_mapped 
insert into test_srs_np_constituent_mapped
select substance_uuid, constituent_name, related_latin_binomial, 
max(case when related_common_name = '' then ltcnt.common_name else related_common_name end) related_common_name,
constituent_uuid
from scratch_sanya.test_srs_np_constituent tsnc inner join scratch_sanya.lb_to_common_names_tsv ltcnt on upper(tsnc.related_latin_binomial) = upper(ltcnt.latin_binomial)
group by substance_uuid, constituent_name, related_latin_binomial, constituent_uuid

select * from scratch_sanya.test_srs_np_part tsncm 
where tsncm.related_latin_binomial = 'Rhodiola rosea'

select * from scratch_sanya.test_srs_np tsncm 
where tsncm.related_latin_binomial = 'Rhodiola rosea'

---get subspecies for substances where script does not include them directly
--eg. Cannabis sativa subsp. indica, fenugreek seed (Trigonella foenum), Sedum roseum
--get whol substance from substance ID and add to test_srs_np and test_srs_np_part
--then add constituents 
--Cannabis sativa subsp. indica whole substance_uuid = f172ae99-9515-4bff-a8ed-4e68afad1b1d DONE
--TRIGONELLA FOENUM-GRAECUM WHOLE substance_uuid = 7c054d8f-9a41-44bf-ae8f-00f541e4e406 DONE
--SEDUM ROSEUM WHOLE substance_uuid = f42f059e-c9c1-46cf-a593-dcc545abb8d1
--queries taken from Python script - gsrs_get_data_without_mixture.py

--add to test_srs_np
with np_substance as (
select igs1.uuid as substance_uuid, igs1.* from ix_ginas_substance igs1
where igs1.uuid = 'f42f059e-c9c1-46cf-a593-dcc545abb8d1'
),
np_strucdiv as (
select * from ix_ginas_strucdiv igs2 
inner join np_substance on np_substance.structurally_diverse_uuid = igs2.uuid 
),
np_parent as (
select igs3.refuuid as parent_uuid from ix_ginas_substanceref igs3 
inner join np_strucdiv on np_strucdiv.parent_substance_uuid = igs3.uuid 
)
insert into scratch_sanya.test_srs_np 
select 'Rhodiola rosea' related_latin_binomial, 'Rhodiola' related_common_name, igs.dtype, igs.uuid as substance_uuid, igs.created, igs.class, igs.status, igs.modifications_uuid,
igs.approval_id, igs.structure_id, igs.structurally_diverse_uuid, 
ign.uuid as name_uuid, ign.internal_references, ign.owner_uuid, ign."name",
ign."type", ign.preferred, ign.display_name, 
ixs.uuid as structdiv_uuid, ixs.source_material_class, ixs.source_material_state, ixs.source_material_type,
ixs.organism_family, ixs.organism_author, ixs.organism_genus, ixs.organism_species, ixs.part_location,
ixs.part, ixs.parent_substance_uuid 
from ix_ginas_substance igs 
inner join ix_ginas_name as ign on ign.owner_uuid = igs.uuid 
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid 
where igs.uuid in 
(select substance_uuid from np_substance) or 
igs.uuid in
(select parent_uuid from np_parent)

--add to test_srs_np_part
with np_substance_part as (
select igss.uuid as part_uuid from ix_ginas_substanceref igss
where igss.refuuid = 'f42f059e-c9c1-46cf-a593-dcc545abb8d1'
) 
insert into scratch_sanya.test_srs_np_part
select 'Rhodiola rosea' related_latin_binomial, 'Rhodiola' related_common_name, igs.dtype, igs.uuid as substance_uuid, igs.created, igs.class, igs.status, igs.modifications_uuid,
igs.approval_id, igs.structure_id, igs.structurally_diverse_uuid, 
ign.uuid as name_uuid, ign.internal_references, ign.owner_uuid, ign."name",
ign."type", ign.preferred, ign.display_name, 
ixs.uuid as structdiv_uuid, ixs.source_material_class, ixs.source_material_state, ixs.source_material_type,
ixs.organism_family, ixs.organism_author, ixs.organism_genus, ixs.organism_species, ixs.part_location,
ixs.part, ixs.parent_substance_uuid 
from ix_ginas_substance igs 
inner join ix_ginas_name as ign on ign.owner_uuid = igs.uuid 
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid 
where ixs.parent_substance_uuid in (select part_uuid from np_substance_part)
and igs.dtype = 'DIV'

--add to test_srs_np_part_rel test_srs_np_rel
with np_substance_part as (
select igss.uuid as part_uuid from ix_ginas_substanceref igss
where igss.refuuid = 'f42f059e-c9c1-46cf-a593-dcc545abb8d1'
),
np_substance as (
select igs.uuid as substance_uuid, igs.dtype from ix_ginas_substance igs
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid
where ixs.parent_substance_uuid in (select part_uuid from np_substance_part)
and igs.dtype = 'DIV'
)
insert into scratch_sanya.test_srs_np_part_rel
select 'Rhodiola rosea' related_latin_binomial, 'Rhodiola' related_common_name, igr.*  
from ix_ginas_relationship igr
where igr.owner_uuid in (select substance_uuid from np_substance)

insert into scratch_sanya.test_srs_np_rel 
select 'Rhodiola rosea' related_latin_binomial, 'Rhodiola' related_common_name, igr.*  
from ix_ginas_relationship igr
where igr.owner_uuid = 'f42f059e-c9c1-46cf-a593-dcc545abb8d1'

--add to test_srs_np_dsld: for all uuids
drop table if exists scratch_sanya.test_srs_np_dsld
CREATE TABLE scratch_sanya.test_srs_np_dsld (
        related_latin_binomial varchar(255) NOT NULL,
        related_common_name varchar(40) NULL,
        uuid varchar(40) NULL,
        organism_family varchar(255) NULL,
        organism_genus varchar(255) NULL,
        organism_species varchar(255) NULL,
        dsld_code varchar(50) NOT NULL,
        dsld_text varchar(255) NULL
)

insert into scratch_sanya.test_srs_np_dsld
select distinct tsn.related_latin_binomial, tsn.related_common_name, tsn.substance_uuid, tsn.organism_family, tsn.organism_genus, tsn.organism_species, 
igc.code as dsld_code, regexp_replace(igc.comments, '^.*\|','') as dsld_text
from ix_ginas_code igc inner join scratch_sanya.test_srs_np tsn on igc.owner_uuid = tsn.substance_uuid 
where igc.code_system = 'DSLD' 
union 
select distinct tsnp.related_latin_binomial, tsnp.related_common_name, tsnp.substance_uuid, tsnp.organism_family, tsnp.organism_genus, tsnp.organism_species, 
igc.code as dsld_code, regexp_replace(igc.comments, '^.*\|','') as dsld_text
from ix_ginas_code igc inner join scratch_sanya.test_srs_np_part tsnp on igc.owner_uuid = tsnp.substance_uuid 
where igc.code_system = 'DSLD'

--ran constituents again after this




