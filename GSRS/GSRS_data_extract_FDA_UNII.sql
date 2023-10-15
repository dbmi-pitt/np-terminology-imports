--use FDA UNII list to get details for all 26k substances (scratch_sanya_2023)

CREATE TABLE scratch_sanya_2023.fda_unii_struc_diverse (
UNII varchar,
PT varchar,
RN varchar,
EC varchar,
NCIT varchar,
RXCUI varchar,
PUBCHEM varchar,
ITIS varchar,
NCBI varchar,
PLANTS varchar,
GRIN varchar,
MPNS varchar,
INN_ID varchar,
USAN_ID varchar,
MF varchar,
INCHIKEY varchar,
SMILES varchar,
INGREDIENT_TYPE varchar,
UTF8_DISPLAY_NAME varchar,
SUBSTANCE_TYPE varchar,
UUID varchar,
DAILYMED varchar
)

--import data from https://precision.fda.gov/uniisearch/archive - filter by substanceType = structurally diverse
--N=26903 (indexed on uuid)
select count(*) from scratch_sanya_2023.fda_unii_struc_diverse fusd 

--N=26903
select count(distinct uuid) from scratch_sanya_2023.fda_unii_struc_diverse fusd 

--Create tables for data

drop table scratch_sanya_2023.test_srs_np 

drop table scratch_sanya_2023.test_srs_np_rel 

drop table scratch_sanya_2023.test_srs_np_dsld 

CREATE TABLE scratch_sanya_2023.test_srs_np (
        related_latin_binomial varchar(255) NULL,
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
)

CREATE TABLE scratch_sanya_2023.test_srs_np_rel (
        related_latin_binomial varchar(255) NULL,
        related_common_name varchar(40) NULL,   
        uuid varchar(40) NULL,
        current_version int4 NULL,
        created timestamp NULL,
        created_by_id int8 NULL,
        last_edited timestamp NULL,
        last_edited_by_id int8 NULL,
        deprecated bool NULL,
        record_access bytea NULL,
        internal_references text NULL,
        owner_uuid varchar(40) NULL,
        amount_uuid varchar(40) NULL,
        "comments" text NULL,
        interaction_type varchar(255) NULL,
        qualification varchar(255) NULL,
        related_substance_uuid varchar(40) NULL,
        mediator_substance_uuid varchar(40) NULL,
        originator_uuid varchar(255) NULL,
        "type" varchar(255) NULL,
        internal_version int8 NULL
)

CREATE TABLE scratch_sanya_2023.test_srs_np_dsld (
        related_latin_binomial varchar(255) NULL,
        related_common_name varchar(40) NULL,
        uuid varchar(40) NULL,
        organism_family varchar(255) NULL,
        organism_genus varchar(255) NULL,
        organism_species varchar(255) NULL,
        dsld_code varchar(50) NOT NULL,
        dsld_text varchar(255) NULL
)

create table scratch_sanya_2023.test_srs_np_constituent (
	uuid varchar(40),
	substance_uuid varchar(40),
	constituent_uuid varchar(40),
	related_common_name varchar(255),
	related_latin_binomial varchar(255),
	relation_type varchar(255),
	constituent_name varchar(255),
	constituent_type varchar(20)
)

CREATE TABLE scratch_sanya_2023.test_srs_np_part (
        related_latin_binomial varchar(255) NULL,
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
)

CREATE TABLE scratch_sanya_2023.test_srs_np_part_rel (
        related_latin_binomial varchar(255) NULL,
        related_common_name varchar(40) NULL,
        uuid varchar(40) NULL,
        current_version int4 NULL,
        created timestamp NULL,
        created_by_id int8 NULL,
        last_edited timestamp NULL,
        last_edited_by_id int8 NULL,
        deprecated bool NULL,
        record_access bytea NULL,
        internal_references text NULL,
        owner_uuid varchar(40) NULL,
        amount_uuid varchar(40) NULL,
        "comments" text NULL,
        interaction_type varchar(255) NULL,
        qualification varchar(255) NULL,
        related_substance_uuid varchar(40) NULL,
        mediator_substance_uuid varchar(40) NULL,
        originator_uuid varchar(255) NULL,
        "type" varchar(255) NULL,
        internal_version int8 NULL
)

--Extract NP data to test_srs_np for all uuids in fda_unii_struc_diverse
insert into scratch_sanya_2023.test_srs_np (dtype, substance_uuid, created, "class", status, modifications_uuid, 
approval_id, structure_id, structurally_diverse_uuid, 
name_uuid, internal_references, owner_uuid, "name", 
"type", preferred, display_name, 
structdiv_uuid, source_material_class, source_material_state, source_material_type, 
organism_family, organism_author, organism_genus, organism_species, part_location, part, parent_substance_uuid)
select igs.dtype, igs.uuid as substance_uuid, igs.created, igs.class, igs.status, igs.modifications_uuid,
igs.approval_id, igs.structure_id, igs.structurally_diverse_uuid, 
ign.uuid as name_uuid, ign.internal_references, ign.owner_uuid, ign."name",
ign."type", ign.preferred, ign.display_name, 
ixs.uuid as structdiv_uuid, ixs.source_material_class, ixs.source_material_state, ixs.source_material_type,
ixs.organism_family, ixs.organism_author, ixs.organism_genus, ixs.organism_species, ixs.part_location,
ixs.part, ixs.parent_substance_uuid 
from ix_ginas_substance igs 
inner join ix_ginas_name as ign on ign.owner_uuid = igs.uuid 
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid 
inner join scratch_sanya_2023.fda_unii_struc_diverse fusd on fusd.uuid = igs.uuid
where ixs.organism_genus is not null and ixs.organism_species is not null
and ixs.source_material_type = 'PLANT'

--N=110,859
select count(*) from scratch_sanya_2023.test_srs_np tsn 

--N=10,666
select count(distinct tsn.substance_uuid) from scratch_sanya_2023.test_srs_np tsn 

/* Filtering above query to PLANT only N=10666
Count	Source material type
1	ARACHNID
1	BACTERIA
1	CHILOPOD ARTHROPOD
1	DIATOM
1	DIATOM ALGA
1	DINOFLAGELLATE
1	HAPTOPHYTE
1	JAWLESS FISH
1	MAMMALIAN
1	PRIMATE
1	SEGMENTED WORM
1	XIPHOSURAN ARTHROPOD
2	MICROALGA
2	RETROVIRAL VECTOR
2	TUNICATE
3	SPONGE
4	AMPHIBIAN
4	CHIMERIC VIRUS
5	CNIDARIAN
6	CHROMALVEOLATE
6	FUNGI
7	CYANOBACTERIUM
8	HUMAN
11	HUMAN CELL LINE
14	BIRD
14	ECHINODERM
23	ARACHNID ARTHROPOD
23	GREEN ALGA
29	PROTIST
32	FLATWORM
34	RECOMBINANT VIRUS
40	NEMATODE
40	REPTILE
44	BROWN ALGA
56	RED ALGA
65	MAMMAL
90	CARTILAGINOUS FISH
93	INSECT ARTHROPOD
157	CRUSTACEAN ARTHROPOD
228	MOLLUSC
241	VIRUS
338	BACTERIUM
389	FUNGUS
1350	BONY FISH
10666	PLANT
 */
select distinct count(*), ixs.source_material_type
from ix_ginas_substance igs 
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid 
inner join scratch_sanya_2023.fda_unii_struc_diverse fusd on fusd.uuid = igs.uuid
where ixs.organism_genus is not null and ixs.organism_species is not null
group by ixs.source_material_type 

--Add related latin binomial and common name
update scratch_sanya_2023.test_srs_np tsn
set related_latin_binomial = concat_ws(' ', INITCAP(tsn.organism_genus), LOWER(tsn.organism_species))

select * from scratch_sanya_2023.test_srs_np tsn 
where tsn.related_latin_binomial = 'Mitragyna speciosa'

--N=0
select * from scratch_sanya_2023.test_srs_np tsn 
where tsn.related_latin_binomial is NULL

--Add common name from lb_to_common_name tsv where available
update scratch_sanya_2023.test_srs_np 
set related_common_name = tsnp.pt  
from scratch_sanya_2023.lb_to_common_name_and_pt tsnp
where tsnp.latin_binomial = related_latin_binomial 

--Extract parts (test_srs_np only contains whole substances)
--get all parts of substances in test_srs_np
with np_substance_part as (
select igss.uuid as part_uuid, tsn.related_latin_binomial as lb, tsn.related_common_name as cn 
from ix_ginas_substanceref igss
inner join scratch_sanya_2023.test_srs_np tsn on tsn.substance_uuid = igss.refuuid
) 
insert into scratch_sanya_2023.test_srs_np_part (related_latin_binomial, related_common_name, dtype, substance_uuid, created, "class", status, modifications_uuid, 
approval_id, structure_id, structurally_diverse_uuid, 
name_uuid, internal_references, owner_uuid, "name", 
"type", preferred, display_name, 
structdiv_uuid, source_material_class, source_material_state, source_material_type, 
organism_family, organism_author, organism_genus, organism_species, part_location, part, parent_substance_uuid)
select np_substance_part.lb, np_substance_part.cn, igs.dtype, igs.uuid as substance_uuid, igs.created, igs.class, igs.status, igs.modifications_uuid,
igs.approval_id, igs.structure_id, igs.structurally_diverse_uuid, 
ign.uuid as name_uuid, ign.internal_references, ign.owner_uuid, ign."name",
ign."type", ign.preferred, ign.display_name, 
ixs.uuid as structdiv_uuid, ixs.source_material_class, ixs.source_material_state, ixs.source_material_type,
ixs.organism_family, ixs.organism_author, ixs.organism_genus, ixs.organism_species, ixs.part_location,
ixs.part, ixs.parent_substance_uuid 
from ix_ginas_substance igs
inner join ix_ginas_name as ign on ign.owner_uuid = igs.uuid 
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid 
inner join np_substance_part on np_substance_part.part_uuid = ixs.parent_substance_uuid 
and igs.dtype = 'DIV' and ixs.source_material_type = 'PLANT'

--N=1,558,809
select count(*) from scratch_sanya_2023.test_srs_np_part tsnp 

--N=6,565
select count(distinct tsnp.substance_uuid) from scratch_sanya_2023.test_srs_np_part tsnp 

select * from scratch_sanya_2023.test_srs_np_part tsn 
where tsn.related_latin_binomial = 'Mitragyna speciosa'

--Extract NP relationships data to test_srs_np for all uuids in fda_unii_struc_diverse
--N=11617
insert into scratch_sanya_2023.test_srs_np_rel (related_latin_binomial,
related_common_name, uuid, current_version,created,created_by_id,last_edited,last_edited_by_id,deprecated,record_access,internal_references,
owner_uuid,amount_uuid,"comments",interaction_type,qualification,related_substance_uuid,mediator_substance_uuid,
originator_uuid,"type",internal_version)
select tsn.related_latin_binomial, tsn.related_common_name, 
igr.uuid, igr.current_version, igr.created,igr.created_by_id,igr.last_edited,igr.last_edited_by_id,igr.deprecated,
igr.record_access, igr.internal_references, igr.owner_uuid,igr.amount_uuid, igr."comments", igr.interaction_type, igr.qualification,
igr.related_substance_uuid, igr.mediator_substance_uuid, igr.originator_uuid, igr."type", igr.internal_version
from ix_ginas_relationship igr
inner join scratch_sanya_2023.test_srs_np tsn on tsn.substance_uuid = igr.owner_uuid 

--N=2161219
insert into scratch_sanya_2023.test_srs_np_part_rel (related_latin_binomial,
related_common_name, uuid, current_version,created,created_by_id,last_edited,last_edited_by_id,deprecated,record_access,internal_references,
owner_uuid,amount_uuid,"comments",interaction_type,qualification,related_substance_uuid,mediator_substance_uuid,
originator_uuid,"type",internal_version)
select tsnp.related_latin_binomial, tsnp.related_common_name,
igr.uuid, igr.current_version, igr.created,igr.created_by_id,igr.last_edited,igr.last_edited_by_id,igr.deprecated,
igr.record_access, igr.internal_references, igr.owner_uuid,igr.amount_uuid, igr."comments", igr.interaction_type, igr.qualification,
igr.related_substance_uuid, igr.mediator_substance_uuid, igr.originator_uuid, igr."type", igr.internal_version  
from ix_ginas_relationship igr
inner join scratch_sanya_2023.test_srs_np_part tsnp on tsnp.substance_uuid = igr.owner_uuid 

--Add DSLD codes
--N=428
insert into scratch_sanya_2023.test_srs_np_dsld
select distinct tsn.related_latin_binomial, tsn.related_common_name, tsn.substance_uuid, tsn.organism_family, tsn.organism_genus, tsn.organism_species, 
igc.code as dsld_code, regexp_replace(igc.comments, '^.*\|','') as dsld_text
from ix_ginas_code igc inner join scratch_sanya_2023.test_srs_np tsn on igc.owner_uuid = tsn.substance_uuid 
where igc.code_system = 'DSLD' 
union 
select distinct tsnp.related_latin_binomial, tsnp.related_common_name, tsnp.substance_uuid, tsnp.organism_family, tsnp.organism_genus, tsnp.organism_species, 
igc.code as dsld_code, regexp_replace(igc.comments, '^.*\|','') as dsld_text
from ix_ginas_code igc inner join scratch_sanya_2023.test_srs_np_part tsnp on igc.owner_uuid = tsnp.substance_uuid 
where igc.code_system = 'DSLD'

--Run constituents script
drop table if exists scratch_sanya_2023.test_srs_np_constituent

create table scratch_sanya_2023.test_srs_np_constituent (
	uuid varchar(40),
	substance_uuid varchar(40),
	constituent_uuid varchar(40),
	related_common_name varchar(255),
	related_latin_binomial varchar(255),
	relation_type varchar(255),
	constituent_name varchar(255),
	constituent_type varchar(20)
)

--N=581,521 (g_substance_reg)
insert into scratch_sanya_2023.test_srs_np_constituent (substance_uuid, related_common_name, related_latin_binomial, uuid, relation_type, 
constituent_name, constituent_uuid)
select tsnpr.owner_uuid as substance_uuid, tsnpr.related_common_name, tsnpr.related_latin_binomial, tsnpr.uuid, tsnpr."type" as relation_type, 
igs.ref_pname as constituent_name, igs.refuuid as constituent_uuid
from scratch_sanya_2023.test_srs_np_part_rel tsnpr
inner join public.ix_ginas_substanceref igs on tsnpr.related_substance_uuid = igs.uuid 
and tsnpr."type" in ('ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'ACTIVE CONSTITUENT ALWAYS PRESENT', 'CONSTITUENT ALWAYS PRESENT->PARENT',
'POSSIBLE ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'CONSTITUENT MAY BE PRESENT->PARENT')

--N=2156 (g_substance_reg)
insert into scratch_sanya_2023.test_srs_np_constituent (substance_uuid, related_common_name, related_latin_binomial, uuid, relation_type,
constituent_name, constituent_uuid)
select tsnpr.owner_uuid as substance_uuid, tsnpr.related_common_name, tsnpr.related_latin_binomial, tsnpr.uuid, tsnpr."type" as relation_type, 
igs.ref_pname as constituent_name, igs.refuuid as constituent_uuid
from scratch_sanya_2023.test_srs_np_rel tsnpr
inner join public.ix_ginas_substanceref igs on tsnpr.related_substance_uuid = igs.uuid 
and tsnpr."type" in ('ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'ACTIVE CONSTITUENT ALWAYS PRESENT', 'CONSTITUENT ALWAYS PRESENT->PARENT',
'POSSIBLE ACTIVE CONSTITUENT ALWAYS PRESENT->PARENT', 'CONSTITUENT MAY BE PRESENT->PARENT')

--count = 583,677
select count(*) from scratch_sanya_2023.test_srs_np_constituent

--Map constituents to PUBCHEM, RXCUI
create table scratch_sanya_2023.test_srs_np_constituent_mapped (
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

--N=1171
select count(distinct constituent_uuid) from scratch_sanya_2023.test_srs_np_constituent tsnc 

select distinct(related_latin_binomial) from scratch_sanya_2023.test_srs_np_constituent tsnc 
where tsnc.constituent_name = 'CIANIDANOL'

--get identifiers of constituents (CHEBI, PUBCHEM, RXCUI)
--N=805,819
INSERT INTO scratch_sanya_2023.test_srs_np_constituent_mapped (uuid, substance_uuid, constituent_uuid, 
related_common_name, related_latin_binomial, relation_type, constituent_name, constituent_code_type,
constituent_code)
select tsnc.uuid, tsnc.substance_uuid, tsnc.constituent_uuid, tsnc.related_common_name,
tsnc.related_latin_binomial, tsnc.relation_type, tsnc.constituent_name, igc.code_system, igc.code 
from scratch_sanya_2023.test_srs_np_constituent tsnc 
left join ix_ginas_code igc on tsnc.constituent_uuid = igc.owner_uuid
and igc.code_system in ('PUBCHEM', 'RXCUI')

--N=1171
select count(distinct constituent_uuid) from scratch_sanya_2023.test_srs_np_constituent_mapped tsnc 

--extract mapped constituents for 30 NPs
select distinct * from scratch_sanya_2023.test_srs_np_constituent tsnc 
where tsnc.related_latin_binomial in ('Withania somnifera', 'Actaea racemosa', 'Piper nigrum', 
'Cinnamomum cassia', 'Cinnamomum verum', 'Vaccinium macrocarpon', 'Echinacea purpurea', 
'Trigonella foenum', 'Tanacetum parthenium', 'Linum usitatissimum', 'Allium sativum', 
'Zingiber officinale', 'Ginkgo biloba', 'Hydrastis canadensis', 'Citrus paradisi', 
'Camellia sinensis', 'Paullinia cupana', 'Cannabis sativa', 'Aesculus hippocastanum', 
'Mitragyna speciosa', 'Glycyrrhiza glabra', 'Glycyrrhiza inflata', 'Glycyrrhiza uralensis', 
'Taraxacum officinale', 'Silybum marianum', 'Origanum vulgare', 'Panax ginseng', 'Rhodiola rosea', 
'Rosmarinus officinalis', 'Serenoa repens', 'Glycine max', 'Curcuma longa', 
'Valeriana officinalis', 'Crataegus laevigata', 'Salvia rosmarinus', 'Sedum roseum', 'Trigonella foenum-graecum')
order by related_latin_binomial 

select distinct * from scratch_sanya_2023.test_srs_np_constituent tsnc 
where tsnc.substance_uuid = 'c908e8d3-b594-494f-9a8f-fce4d730be66'

select * from scratch_sanya_2023.test_srs_np_part tsn 
where tsn.substance_uuid = 'c908e8d3-b594-494f-9a8f-fce4d730be66'

