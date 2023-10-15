--run gsrs_add_missing_substances.sql first
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

--count = 1149
select count(*) from scratch_sanya.test_srs_np_constituent

--N=1100 (g_substance_reg)
insert into test_srs_np_constituent (substance_uuid, related_common_name, related_latin_binomial, uuid, relation_type, 
constituent_name, constituent_uuid)
select tsnpr.owner_uuid as substance_uuid, tsnpr.related_common_name, tsnpr.related_latin_binomial, tsnpr.uuid, tsnpr."type" as relation_type, 
igs.ref_pname as constituent_name, igs.refuuid as constituent_uuid
from test_srs_np_part_rel tsnpr
inner join public.ix_ginas_substanceref igs on tsnpr.related_substance_uuid = igs.uuid 
and tsnpr."type" in ('ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'ACTIVE CONSTITUENT ALWAYS PRESENT', 'CONSTITUENT ALWAYS PRESENT->PARENT',
'POSSIBLE ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'CONSTITUENT MAY BE PRESENT->PARENT')

--N=49 (g_substance_reg)
insert into test_srs_np_constituent (substance_uuid, related_common_name, related_latin_binomial, uuid, relation_type,
constituent_name, constituent_uuid)
select tsnpr.owner_uuid as substance_uuid, tsnpr.related_common_name, tsnpr.related_latin_binomial, tsnpr.uuid, tsnpr."type" as relation_type, 
igs.ref_pname as constituent_name, igs.refuuid as constituent_uuid
from test_srs_np_rel tsnpr
inner join public.ix_ginas_substanceref igs on tsnpr.related_substance_uuid = igs.uuid 
and tsnpr."type" in ('ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'ACTIVE CONSTITUENT ALWAYS PRESENT', 'CONSTITUENT ALWAYS PRESENT->PARENT',
'POSSIBLE ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'CONSTITUENT MAY BE PRESENT->PARENT')

--test constituents for 'Cannabis sativa'
select * from scratch_sanya.test_srs_np_constituent tsnc 
where tsnc.related_latin_binomial = 'Cannabis sativa'

--test constituents for 'Milk thistle'
select * from scratch_sanya.test_srs_np_constituent tsnc 
where tsnc.related_latin_binomial = 'Silybum marianum'
 
select * from scratch_sanya.test_srs_np_constituent tsnc 
where tsnc.related_common_name  = 'Green tea'

--N = 1149 (g_substance_reg)
select count(*) from scratch_sanya.test_srs_np_constituent tsnc 

--Do we need to run DSLD again??
--add to test_srs_np_dsld: for all uuids
select * from scratch_sanya.test_srs_np_dsld tsnd 
where tsnd.related_latin_binomial = 'Camellia sinensis'

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

--N=191
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

--Constituent queries for sample constituent annotations
select tsnc.related_latin_binomial, tsnc.related_common_name, tsnc.constituent_name, tsnc.relation_type  from scratch_sanya.test_srs_np_constituent tsnc 
where related_latin_binomial in ('Allium sativum', 'Gentiana lutea', 'Zingiber officinale', 'Camellia sinensis',
'Paullinia cupana', 'Humulus lupulus', 'Paspalum distichum', 'Aloysia citrodora', 'Origanum majorana', 'Silybum marianum')

--extraction
select * from scratch_sanya.test_srs_np_part tsn 
where tsn.related_latin_binomial = 'Camellia sinensis'

select * from scratch_sanya_2023.test_srs_np tsn 
where tsn.substance_uuid = '0e564403-14be-4537-8f3e-583022cbb33b'

select * from scratch_sanya.test_srs_np_constituent tsnc 
where tsnc.related_latin_binomial in ('Withania somnifera', 'Actaea racemosa', 'Piper nigrum', 
'Cinnamomum cassia', 'Cinnamomum verum', 'Vaccinium macrocarpon', 'Echinacea purpurea', 
'Trigonella foenum', 'Tanacetum parthenium', 'Linum usitatissimum', 'Allium sativum', 
'Zingiber officinale', 'Ginkgo biloba', 'Hydrastis canadensis', 'Citrus paradisi', 
'Camellia sinensis', 'Paullinia cupana', 'Cannabis sativa', 'Aesculus hippocastanum', 
'Mitragyna speciosa', 'Glycyrrhiza glabra', 'Glycyrrhiza inflata', 'Glycyrrhiza uralensis', 
'Taraxacum officinale', 'Silybum marianum', 'Origanum vulgare', 'Panax ginseng', 'Rhodiola rosea', 
'Rosmarinus officinalis', 'Serenoa repens', 'Glycine max', 'Curcuma longa', 
'Valeriana officinalis', 'Crataegus laevigata')
order by related_latin_binomial 

drop table scratch_sanya.test_srs_np_constituent_mapped 

--Map constituents
create table scratch_sanya.test_srs_np_constituent_mapped (
	uuid varchar(40),
	substance_uuid varchar(40),
	constituent_uuid varchar(40),
	related_common_name varchar(255),
	related_latin_binomial varchar(255),
	relation_type varchar(255),
	constituent_name varchar(255),
	constituent_code_type varchar(20),
	constituent_code varchar (20)
)

--N=736
select count(distinct constituent_uuid) from scratch_sanya.test_srs_np_constituent tsnc 

select count(*), igc.code_system from ix_ginas_code igc 
group by igc.code_system 

--check catechin mappings
select * from ix_ginas_code igc 
where igc.owner_uuid = '5e3251fe-d591-4ac6-a8ad-679a3fe17e21'

select * from scratch_sanya.test_srs_np_constituent tsnc 
where tsnc.constituent_name = 'CIANIDANOL'

--get identifiers of constituents (CHEBI, PUBCHEM, RXCUI)
--N=1521
INSERT INTO scratch_sanya.test_srs_np_constituent_mapped (uuid, substance_uuid, constituent_uuid, 
related_common_name, related_latin_binomial, relation_type, constituent_name, constituent_code_type,
constituent_code)
select tsnc.uuid, tsnc.substance_uuid, tsnc.constituent_uuid, tsnc.related_common_name,
tsnc.related_latin_binomial, tsnc.relation_type, tsnc.constituent_name, igc.code_system, igc.code 
from scratch_sanya.test_srs_np_constituent tsnc 
left join ix_ginas_code igc on tsnc.constituent_uuid = igc.owner_uuid
and igc.code_system in ('PUBCHEM', 'RXCUI')

select * from scratch_sanya.test_srs_np_constituent_mapped tsnc 
where tsnc.constituent_code is NULL

--N=736
select count(distinct constituent_uuid) from scratch_sanya.test_srs_np_constituent_mapped tsnc 

--extract mapped constituents for 30 NPs
select * from scratch_sanya.test_srs_np_constituent_mapped tsnc 
where tsnc.related_latin_binomial in ('Withania somnifera', 'Actaea racemosa', 'Piper nigrum', 
'Cinnamomum cassia', 'Cinnamomum verum', 'Vaccinium macrocarpon', 'Echinacea purpurea', 
'Trigonella foenum', 'Tanacetum parthenium', 'Linum usitatissimum', 'Allium sativum', 
'Zingiber officinale', 'Ginkgo biloba', 'Hydrastis canadensis', 'Citrus paradisi', 
'Camellia sinensis', 'Paullinia cupana', 'Cannabis sativa', 'Aesculus hippocastanum', 
'Mitragyna speciosa', 'Glycyrrhiza glabra', 'Glycyrrhiza inflata', 'Glycyrrhiza uralensis', 
'Taraxacum officinale', 'Silybum marianum', 'Origanum vulgare', 'Panax ginseng', 'Rhodiola rosea', 
'Rosmarinus officinalis', 'Serenoa repens', 'Glycine max', 'Curcuma longa', 
'Valeriana officinalis', 'Crataegus laevigata')
order by related_latin_binomial 



