--workflow: https://github.com/rkboyce/NaPDI-pv/blob/master/np-terminology-imports/napdi_np_vocab_workflow_SPRING_2023.sql


-- N=285
--- queries for sanity checks
-- obtaining the NP preferred terms 
select *
from staging_vocabulary.concept c 
where vocabulary_id = 'NAPDI' 
and c.concept_class_id = 'NaPDI Preferred Term'
;

-- take the concept_id of the preferred term for the NP and use the napdi_is_pt_of relationship to get the common names/L.B
select c1.concept_name, c1.concept_id, c2.concept_name, c2.concept_id 
from staging_vocabulary.concept c1 
inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_is_pt_of'
order by c1.concept_name, c2.concept_name 
;

-- going from the many-to-one mapped common name/L.B. to the preferred term concept_id using napdi_pt
select c1.concept_name, c1.concept_id, c2.concept_name, c2.concept_id 
from staging_vocabulary.concept c1 
inner join staging_vocabulary.concept_relationship cr 
on c1.concept_id = cr.concept_id_1 
inner join staging_vocabulary.concept c2 
on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_pt'
order by c1.concept_name, c2.concept_name 
;

-- Obtain the constituents of NPs using common name or LB 
select c1.concept_name, c1.concept_id, c2.concept_name, c2.concept_id 
from staging_vocabulary.concept c1 
inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_has_const'
order by c1.concept_name, c2.concept_name 
;

-- going from PT to constituents associated with any of the common names/LB that the PT could represent
select distinct c1.concept_name, c1.concept_id, c3.concept_name, c3.concept_id 
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
select c1.concept_name, c1.concept_id, c2.concept_name, c2.concept_id 
from staging_vocabulary.concept c1 
inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_is_const_of'
order by c1.concept_name, c2.concept_name 
;

-- Going from a constituent assocated with a common name or LB using the napdi_is_const_of to the preferred term that represents that common or LB
select distinct c1.concept_name, c1.concept_id, c3.concept_name, c3.concept_id 
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
-- napdi PT to single NP products in RxNorm using np_maps_to
-- napdi concepts for combos to RxNorm combination products using np_maps_to
-- napdi PT to napdi concepts for combos using napdi_pt_to_combo (include RxNorm mapped and the rest mapped in from spelling variations and herbal strings)
-- napdi concepts for combos to PT using napdi_combo_to_pt
-- ***should these have combinations?
select * from staging_vocabulary.concept_relationship cr 
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
--
-- currently, this is only partially completed. 
--
-- TODO: when we can connect these products to a database like the dietary supplement label database, we should be able to create the correct mappings
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept_relationship cr 
inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_pt_to_combo'
;

--
-- napdi concepts for combos to PT using napdi_combo_to_pt
--
-- currently, this is only partially completed. 
--
-- TODO: when we can connect these products to a database like the dietary supplement label database, we should be able to create the correct mappings
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept_relationship cr 
inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_combo_to_pt'
;

--TESTING WITH SPECIFIC NP EXAMPLES - also added to wiki

--Test using kratom
-- obtaining the common name and L.B. that has a concept_class_id of the NP of interest
-- This works becaseu for common name and L.B., the concept_class_id is the preferred term
select * from staging_vocabulary.concept c where concept_class_id = 'Kratom';
/*
 * -7000553	Kratom[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		d469b67d-e9a6-459f-b209-c59451936336	2000-01-01	2099-02-22
-7000554	Kratum[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		d469b67d-e9a6-459f-b209-c59451936336	2000-01-01	2099-02-22
-7001253	Mitragyna speciosa[Mitragyna speciosa]	NaPDI research	NAPDI	Kratom		d469b67d-e9a6-459f-b209-c59451936336	2000-01-01	2099-02-22
 */

-- going from the many-to-one mapped common name/L.B. to the preferred term concept_id using napdi_pt
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = -7000553
;
/*
 * Kratom[Mitragyna speciosa]	-7000553	Kratom	-7001068
 */

-- take the concept_id of the preferred term for the NP and use the napdi_is_pt_of relationship to get the common names/L.B
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
  and c1.concept_id =  -7001068
;
/*
 * Kratom	-7001068	Mitragyna speciosa[Mitragyna speciosa]	-7001253
Kratom	-7001068	Kratum[Mitragyna speciosa]	-7000554
Kratom	-7001068	Kratom[Mitragyna speciosa]	-7000553
 */

-- Obtain the constituents of one of the NPs by common name
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7000554
;
/*
Kratum[Mitragyna speciosa]	-7000554	SPECIOCILIATIN	-7002452
Kratum[Mitragyna speciosa]	-7000554	MITRAGYNINE	-7002306
Kratum[Mitragyna speciosa]	-7000554	7-HYDROXYMITRAGYNINE	-7002295
Kratum[Mitragyna speciosa]	-7000554	SPECIOGYNINE	-7002075
Kratum[Mitragyna speciosa]	-7000554	PAYNANTHEINE	-7001833
Kratum[Mitragyna speciosa]	-7000554	MITRAPHYLLINE	-7001502
*/

-- going from PT to constintuents associated with any of the common names/LB that the PT could represent
select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7001068
;
/*
 Kratom	-7001068	7-HYDROXYMITRAGYNINE	-7002295
Kratom	-7001068	MITRAGYNINE	-7002306
Kratom	-7001068	MITRAPHYLLINE	-7001502
Kratom	-7001068	PAYNANTHEINE	-7001833
Kratom	-7001068	SPECIOCILIATIN	-7002452
Kratom	-7001068	SPECIOGYNINE	-7002075
 */

-- Going from a constituent assocated with a common name or LB using the napdi_is_const_of
select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = -7002306
;
/*
 MITRAGYNINE	-7002306	Mitragyna speciosa[Mitragyna speciosa]	-7001253
MITRAGYNINE	-7002306	Kratum[Mitragyna speciosa]	-7000554
MITRAGYNINE	-7002306	Kratom[Mitragyna speciosa]	-7000553
 */

-- Going from a constituent assocated with a common name or LB using the napdi_is_const_of to the preferred term that represents that common or LB
select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = -7002306
;
/*
MITRAGYNINE	-7002306	Kratom	-7001068
 */

-- going from PT to spelling variations associated with any of the common names/LB that the PT could represent
select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7001068
;
/*
Kratom	-7001068	BALI KRATOM	-7005078
Kratom	-7001068	BLUE MAGIC KRAVE KRATOM	-7005095
Kratom	-7001068	BRILLIANT ELIXIR, CHOCOLATE LOVER W/ KRATOM	-7005094
Kratom	-7001068	CALCIUM KRATOMOS	-7005082
Kratom	-7001068	CAROLINA KRATOM RED JONGKONG 100 GRAM POWDER	-7005064
Kratom	-7001068	CLUB 13 KRATOM MAENG DA RED 90GM	-7005035
Kratom	-7001068	EARTH KRATOM ORGANIC RED MAENG DA	-7005077
Kratom	-7001068	EMERALD KRATOM POWDER	-7005097
Kratom	-7001068	EMERALD LEAF BALI KRATOM (HERBALSMITRAGYNINE)	-7005068
Kratom	-7001068	FEELIN' GROOVY KRATOM	-7005118
Kratom	-7001068	GREEN BORNEO KRATOM	-7005061
Kratom	-7001068	GREEN KRATOM	-7005072
Kratom	-7001068	GREEN MALAY KRATOM	-7005091
Kratom	-7001068	GREEN M BATIK AND RED BATIK KRATOM	-7005051
Kratom	-7001068	GREEN STRAIN TROPICAL KRATOM	-7005110
Kratom	-7001068	HERBAL SALVATION KRATOM	-7005114
Kratom	-7001068	HERBALS\MITRAGYNINE	-7005124
Kratom	-7001068	HERBAL SUBSTANCE KRATOM	-7005055
Kratom	-7001068	INDO KRATOM	-7005102
Kratom	-7001068	KATROM MITRAGYNA SPECIOSA	-7005033
Kratom	-7001068	KLARITY KRATOM: MAENG DA CAPSULES	-7005057
Kratom	-7001068	KRABOT KRATOM FINELY GROUND POWDER	-7005069
Kratom	-7001068	KRAKEN KRATOM	-7005065
Kratom	-7001068	KRATOM	-7005117
Kratom	-7001068	KRATOM 3 OZ.	-7005116
Kratom	-7001068	KRATOM CAPSULES	-7005036
Kratom	-7001068	KRATOM ELEPHANT WHITE THAI	-7005120
Kratom	-7001068	KRATOM EXTRACT	-7005056
Kratom	-7001068	KRATOM GREEN MAGNA DA	-7005099
Kratom	-7001068	KRATOM HERBAL SUPPLEMENT	-7005098
Kratom	-7001068	KRATOM IN A UNMARKED BAG	-7005081
Kratom	-7001068	KRATOM INDO	-7005063
Kratom	-7001068	(KRATOM) KRAOMA.COM TRANQUIL KRAOMA	-7005052
Kratom	-7001068	KRATOM MAGNA RED	-7005038
Kratom	-7001068	KRATOM MG	-7005112
Kratom	-7001068	KRATOM (MITRAGYNA)	-7005090
Kratom	-7001068	KRATOM (MITRAGYNA) (MITRAGYNINE)	-7005106
Kratom	-7001068	KRATOM (MITRAGYNA SPECIOSA)	-7005104
Kratom	-7001068	KRATOM MITRAGYNA SPECIOSA	-7005047
Kratom	-7001068	KRATOM (MITRAGYNA SPECIOSA LEAF)	-7005046
Kratom	-7001068	KRATOM (MITRAGYNINE)	-7005059
Kratom	-7001068	KRATOM POWDER	-7005096
Kratom	-7001068	KRATOM RED DRAGON	-7005107
Kratom	-7001068	KRATOM SILVER THAI	-7005058
Kratom	-7001068	KRATOM - SPECIFICALLY ^GREEN MALAYSIAN^	-7005123
Kratom	-7001068	KRATOM SUPPLEMENT	-7005075
Kratom	-7001068	KRATOM- USED SUPER GREEN, GREEN BALI, RED MAGNEA DA	-7005088
Kratom	-7001068	KRAVE, BLUE MAGIC KRATOM	-7005045
Kratom	-7001068	KRAVE KRATOM	-7005034
Kratom	-7001068	LUCKY SEVEN KRATOM	-7005113
Kratom	-7001068	LYFT PREMIUM BALI KRATOM HERBAL SUPPLEMENT POWDER	-7005084
Kratom	-7001068	MAENG DA KRATOM	-7005037
Kratom	-7001068	MAENG DA KRATOM MITROGYNA SPECIOSA	-7005108
Kratom	-7001068	MAENG DA POWDER KRATOM HERBAL DIETARY SUPPLEMENT	-7005093
Kratom	-7001068	MITRAGYNA SPECIOSA	-7005105
Kratom	-7001068	MITRAGYNA SPECIOSA KORTHALS	-7005039
Kratom	-7001068	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME)	-7005041
Kratom	-7001068	MITRAGYNA SPECIOSA KORTHALS (BOTANIC NAME) (KRATOM)	-7005054
Kratom	-7001068	MITRAGYNA SPECIOSA KRATOM WVB CAPSULES	-7005060
Kratom	-7001068	MITRAGYNA SPECIOSA LEAF	-7005042
Kratom	-7001068	MITRAGYNA SPECIOSA (MITRAGYNINE)	-7005043
Kratom	-7001068	MITRAGYNINE (KRATOM)	-7005087
Kratom	-7001068	MITRAGYNINE KRATOM	-7005111
Kratom	-7001068	NATURE'S REMEDY KRATOM	-7005109
Kratom	-7001068	NUTRIZONE KRATOM PAIN OUT MAENG DA	-7005083
Kratom	-7001068	O.P.M.S. KRATOM	-7005089
Kratom	-7001068	OPMS KRATOM	-7005076
Kratom	-7001068	O.P.M.S. LIQUID KRATOM	-7005070
Kratom	-7001068	POWDERED KRATOM	-7005115
Kratom	-7001068	PREMIUM KRATOM PHOENIX RED VEIN BALI	-7005119
Kratom	-7001068	PREMIUM RED MAENG DA CRAZY KRATOM	-7005122
Kratom	-7001068	PREMIUM RED MAENG DA KRATOM	-7005050
Kratom	-7001068	RAW FORM ORGANICS MAENG DA 150 RUBY CAPSULES KRATOM	-7005100
Kratom	-7001068	RED BALI KRATOM	-7005085
Kratom	-7001068	RED BORNEO KRATOM BUMBLE BEE	-7005103
Kratom	-7001068	RED DEVIL KRATOM WATER SOLUBLE CBD	-7005053
Kratom	-7001068	RED MAENG DA KRATOM (MITRAGYNA SPECIOSA)	-7005062
Kratom	-7001068	RED THAI KRATOM	-7005121
Kratom	-7001068	RED VEIN BORNEO KRATOM	-7005067
Kratom	-7001068	RED VEIN KRATOM	-7005125
Kratom	-7001068	RED VEIN MAENG DA (KRATOM)	-7005079
Kratom	-7001068	R.H. NATURAL PRODUCTS KRATOM	-7005049
Kratom	-7001068	SLOW-MO HIPPO KRATOM	-7005032
Kratom	-7001068	SUPER GREEN HORN KRATOM	-7005044
Kratom	-7001068	SUPER GREEN KRATOM POWDER	-7005074
Kratom	-7001068	SUPERIOR RED DRAGON KRATOM	-7005048
Kratom	-7001068	TAUNTON BAY SOAP COMPANY RED VEIN TEA-1LB. PACKAGE (KRATOM)	-7005040
Kratom	-7001068	TRAIN WRECK KRATOM	-7005086
Kratom	-7001068	UNICORN DUST STRAIN OF KRATOM	-7005080
Kratom	-7001068	VIVAZEN BOTANICALS MAENG DA KRATOM	-7005071
Kratom	-7001068	VIVA ZEN KRATOM	-7005066
Kratom	-7001068	WHITE MAENG DA HERBAL TEA KRATOM	-7005073
Kratom	-7001068	WHITE MAENG DA KRATOM 250G	-7005101
Kratom	-7001068	WHOLE HERBS PREMIUM MAENG DA KRATOM	-7005092
 */

-- going from PT to mapped concepts associated with any of the common names/LB that the PT could represent
select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_np_maps_to'
   and c1.concept_id = -7001068
;
--no results

---- going from PT to combos associated with any of the common names/LB that the PT could represent
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt_to_combo'
and c1.concept_id = -7001068
;
--no results

--Queries to confirm relationships with 'Cinnamon'

select * from staging_vocabulary.concept c where concept_class_id = 'Cinnamon';
/*
 * -7000233	Camphor[Cinnamomum camphora]	NaPDI research	NAPDI	Cinnamon
-7000234	Camphor laurel[Cinnamomum camphora]	NaPDI research	NAPDI	Cinnamon
-7000235	Camphor tree[Cinnamomum camphora]	NaPDI research	NAPDI	Cinnamon
-7000236	Camphortree[Cinnamomum camphora]	NaPDI research	NAPDI	Cinnamon
-7000237	Japanese camphor[Cinnamomum camphora]	NaPDI research	NAPDI	Cinnamon
-7000238	Kuso-no-ki[Cinnamomum camphora]	NaPDI research	NAPDI	Cinnamon
-7000239	Cassia[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon
-7000240	Cassia cinnamon[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon
-7000241	Chinese cinnamon[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon
-7000242	Chinese cinnamon tree[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon
-7000243	Cinnamon[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon
-7000244	Rou gui[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon
-7000245	Ceylon cinnamon[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon
-7000246	Cinnamon[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon
-7000247	True cinnamon[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon
-7000248	Tvak[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon
-7000917	Bataviacinnamon[Cinnamomum burmanni]	NaPDI research	NAPDI	Cinnamon
-7000918	Cinnamon[Cinnamomum burmanni]	NaPDI research	NAPDI	Cinnamon
-7001305	Cinnamomum burmanni[Cinnamomum burmanni]	NaPDI research	NAPDI	Cinnamon
-7001343	Cinnamomum verum[Cinnamomum verum]	NaPDI research	NAPDI	Cinnamon
-7001471	Cinnamomum cassia[Cinnamomum cassia]	NaPDI research	NAPDI	Cinnamon
-7001479	Cinnamomum camphora[Cinnamomum camphora]	NaPDI research	NAPDI	Cinnamon
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = -7000917
;
/*
 Bataviacinnamon[Cinnamomum burmanni]	-7000917	Cinnamon	-7000933
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
   and c1.concept_id = -7000933
;
/*
Cinnamon	-7000933	Cinnamomum camphora[Cinnamomum camphora]	-7001479
Cinnamon	-7000933	Cinnamomum cassia[Cinnamomum cassia]	-7001471
Cinnamon	-7000933	Cinnamomum verum[Cinnamomum verum]	-7001343
Cinnamon	-7000933	Cinnamomum burmanni[Cinnamomum burmanni]	-7001305
Cinnamon	-7000933	Cinnamon[Cinnamomum burmanni]	-7000918
Cinnamon	-7000933	Bataviacinnamon[Cinnamomum burmanni]	-7000917
Cinnamon	-7000933	Tvak[Cinnamomum verum]	-7000248
Cinnamon	-7000933	True cinnamon[Cinnamomum verum]	-7000247
Cinnamon	-7000933	Cinnamon[Cinnamomum verum]	-7000246
Cinnamon	-7000933	Ceylon cinnamon[Cinnamomum verum]	-7000245
Cinnamon	-7000933	Rou gui[Cinnamomum cassia]	-7000244
Cinnamon	-7000933	Cinnamon[Cinnamomum cassia]	-7000243
Cinnamon	-7000933	Chinese cinnamon tree[Cinnamomum cassia]	-7000242
Cinnamon	-7000933	Chinese cinnamon[Cinnamomum cassia]	-7000241
Cinnamon	-7000933	Cassia cinnamon[Cinnamomum cassia]	-7000240
Cinnamon	-7000933	Cassia[Cinnamomum cassia]	-7000239
Cinnamon	-7000933	Kuso-no-ki[Cinnamomum camphora]	-7000238
Cinnamon	-7000933	Japanese camphor[Cinnamomum camphora]	-7000237
Cinnamon	-7000933	Camphortree[Cinnamomum camphora]	-7000236
Cinnamon	-7000933	Camphor tree[Cinnamomum camphora]	-7000235
Cinnamon	-7000933	Camphor laurel[Cinnamomum camphora]	-7000234
Cinnamon	-7000933	Camphor[Cinnamomum camphora]	-7000233
 */

select c1.concept_name, c1.concept_id, c2.concept_name, c2.concept_id 
from staging_vocabulary.concept c1 
	inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
	inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_has_const' 
and c1.concept_id in (-7000243, -7001479)
;

select * from staging_vocabulary.concept c 
where c.concept_id = -7001479

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7001479
;
/*
Cassia[Cinnamomum cassia]	-7000239	O-METHOXYCINNAMALDEHYDE, (E)-	-7002639
Cassia[Cinnamomum cassia]	-7000239	CHINESE CINNAMON OIL	-7002618
Cassia[Cinnamomum cassia]	-7000239	O-METHOXYCINNAMALDEHYDE	-7002510
Cassia[Cinnamomum cassia]	-7000239	SALICYLALDEHYDE	-7002380
Cassia[Cinnamomum cassia]	-7000239	2-METHOXYCINNAMIC ACID	-7002113
Cassia[Cinnamomum cassia]	-7000239	CINNAMTANNIN B2	-7001955
Cassia[Cinnamomum cassia]	-7000239	CINNAMIC ACID	-7001900
Cassia[Cinnamomum cassia]	-7000239	METHYL EUGENOL	-7001875
Cassia[Cinnamomum cassia]	-7000239	CINNAMYL ACETATE	-7001703
Cassia[Cinnamomum cassia]	-7000239	CINNAMYL ALCOHOL	-7001658
Cassia[Cinnamomum cassia]	-7000239	EUGENOL	-7001575
Cassia[Cinnamomum cassia]	-7000239	CINNAMALDEHYDE	-7001529
Cassia[Cinnamomum cassia]	-7000239	COUMARIN	-7001522
 */

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7000933
;
/*
Cinnamon	-7000933	2-METHOXYCINNAMIC ACID	-7002113
Cinnamon	-7000933	BENZALDEHYDE	-7001978
Cinnamon	-7000933	BENZYL BENZOATE	-7002460
Cinnamon	-7000933	CARYOPHYLLENE	-7001565
Cinnamon	-7000933	CHINESE CINNAMON OIL	-7002618
Cinnamon	-7000933	CINNAMALDEHYDE	-7001529
Cinnamon	-7000933	CINNAMIC ACID	-7001900
Cinnamon	-7000933	CINNAMTANNIN B1	-7001762
Cinnamon	-7000933	CINNAMTANNIN B2	-7001955
Cinnamon	-7000933	CINNAMYL ACETATE	-7001703
Cinnamon	-7000933	CINNAMYL ALCOHOL	-7001658
Cinnamon	-7000933	COUMARIN	-7001522
Cinnamon	-7000933	EUCALYPTOL	-7001612
Cinnamon	-7000933	EUGENOL	-7001575
Cinnamon	-7000933	LINALOOL, (+/-)-	-7002586
Cinnamon	-7000933	M-CYMENE	-7001592
Cinnamon	-7000933	METHYL EUGENOL	-7001875
Cinnamon	-7000933	O-CYMENE	-7002102
Cinnamon	-7000933	O-METHOXYCINNAMALDEHYDE	-7002510
Cinnamon	-7000933	O-METHOXYCINNAMALDEHYDE, (E)-	-7002639
Cinnamon	-7000933	P-CYMENE	-7002327
Cinnamon	-7000933	PHELLANDRENE	-7001826
Cinnamon	-7000933	PINENE	-7001777
Cinnamon	-7000933	SAFROLE	-7002658
Cinnamon	-7000933	SALICYLALDEHYDE	-7002380
 */

select c1.concept_name, c1.concept_id, c2.concept_name, c2.concept_id 
from staging_vocabulary.concept c1 
inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2 
where cr.relationship_id = 'napdi_is_pt_of' 
and c1.concept_id = -7000933;

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = -7001777
;
/*
PINENE	-7001777	Cinnamomum verum[Cinnamomum verum]	-7001343
PINENE	-7001777	Tvak[Cinnamomum verum]	-7000248
PINENE	-7001777	True cinnamon[Cinnamomum verum]	-7000247
PINENE	-7001777	Cinnamon[Cinnamomum verum]	-7000246
PINENE	-7001777	Ceylon cinnamon[Cinnamomum verum]	-7000245
 */

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = -7001777
;
/*
PINENE	-7001777	Cinnamon	-7000933
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7000239
;
--569 results

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7001305
;
--no results (no spelling variations for this cinnamon variety)

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
    and c1.concept_id = -7006546  
;
/*
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Cinnamomum camphora[Cinnamomum camphora]	-7001479
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Cinnamomum cassia[Cinnamomum cassia]	-7001471
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Cinnamomum verum[Cinnamomum verum]	-7001343
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Zingiber officinale[Zingiber officinale]	-7001223
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Jiang[Zingiber officinale]	-7000916
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Ginger[Zingiber officinale]	-7000915
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Tvak[Cinnamomum verum]	-7000248
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	True cinnamon[Cinnamomum verum]	-7000247
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Cinnamon[Cinnamomum verum]	-7000246
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Ceylon cinnamon[Cinnamomum verum]	-7000245
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Rou gui[Cinnamomum cassia]	-7000244
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Cinnamon[Cinnamomum cassia]	-7000243
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Chinese cinnamon tree[Cinnamomum cassia]	-7000242
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Chinese cinnamon[Cinnamomum cassia]	-7000241
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Cassia cinnamon[Cinnamomum cassia]	-7000240
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Cassia[Cinnamomum cassia]	-7000239
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Kuso-no-ki[Cinnamomum camphora]	-7000238
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Japanese camphor[Cinnamomum camphora]	-7000237
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Camphortree[Cinnamomum camphora]	-7000236
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Camphor tree[Cinnamomum camphora]	-7000235
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Camphor laurel[Cinnamomum camphora]	-7000234
CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	-7006546	Camphor[Cinnamomum camphora]	-7000233
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
    and c1.concept_id = -7000933
;
/*
Cinnamon	-7000933	cinnamon preparation	1307310
Cinnamon	-7000933	Cinnamon Preparation 500 MG	1307311
Cinnamon	-7000933	Cinnamon Preparation 500 MG Oral Capsule	1307352
Cinnamon	-7000933	cinnamon bark	1359143
Cinnamon	-7000933	cinnamon bark 500 MG Oral Capsule	19112600
Cinnamon	-7000933	cinnamon bark 500 MG	19112601
Cinnamon	-7000933	cinnamon bark Oral Product	36216559
Cinnamon	-7000933	cinnamon bark Pill	36216560
Cinnamon	-7000933	Cinnamon Preparation Oral Product	36216561
Cinnamon	-7000933	Cinnamon Preparation Pill	36216562
Cinnamon	-7000933	cinnamon allergenic extract Injectable Product	36219824
Cinnamon	-7000933	cinnamon bark Oral Capsule	40028295
Cinnamon	-7000933	Cinnamon Preparation Oral Capsule	40133415
Cinnamon	-7000933	Cinnamon Preparation 500 MG Oral Tablet	40163080
Cinnamon	-7000933	Cinnamon Preparation Oral Tablet	40163081
Cinnamon	-7000933	cinnamon allergenic extract	40172100
Cinnamon	-7000933	cinnamon allergenic extract 50 MG/ML	40172101
Cinnamon	-7000933	cinnamon allergenic extract 50 MG/ML Injectable Solution	40172102
Cinnamon	-7000933	cinnamon allergenic extract Injectable Solution	40172103
Cinnamon	-7000933	cinnamon allergenic extract 100 MG/ML	40174181
Cinnamon	-7000933	cinnamon allergenic extract 100 MG/ML Injectable Solution	40174182
Cinnamon	-7000933	Chinese cinnamon leaf oil	42898601
Cinnamon	-7000933	Chinese cinnamon oil	42898727
Cinnamon	-7000933	Chinese cinnamon extract	42898728
Cinnamon	-7000933	cinnamon oil	42898867
Cinnamon	-7000933	cinnamon oil, bark	42900310
Cinnamon	-7000933	cinnamon oil, leaf	42900311
Cinnamon	-7000933	cinnamon bark 1000 MG	43525468
Cinnamon	-7000933	cinnamon bark 1000 MG Oral Capsule	43525717
 */

---- going from PT to combos associated with any of the common names/LB that the PT could represent
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept_relationship cr 
  inner join staging_vocabulary.concept c1 on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt_to_combo'
and c1.concept_id = -7000933
;
/*
-7000933	Cinnamon	-7007930	CINNAMON BARK W/CHROMIMUN	napdi_pt_to_combo
-7000933	Cinnamon	-7007912	SHOSEIRYUTO [ASARUM SPP. ROOT;CINNAMOMUM CASSIA BARK;EPHEDRA SPP.	napdi_pt_to_combo
-7000933	Cinnamon	-7007904	CINNAMON 1000/PLUS CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007890	CINNAMON WITH CHROMIUM (CINNAMOMUM VERUM)	napdi_pt_to_combo
-7000933	Cinnamon	-7007871	CHINESE LICORICE (+) CHINESE PEONY (+) CINNAMON (+) EPHEDRA (+) GINGER	napdi_pt_to_combo
-7000933	Cinnamon	-7007868	CINNAMON W/ CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007867	CINNAMON CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007866	MISTURA CARMINATIVA (CARAWAY (+) CARDAMOM SEED (+) CINNAMON (+) GLYCER	napdi_pt_to_combo
-7000933	Cinnamon	-7007853	CINNAMON WITH CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007847	CINNAMOMUM CASSIA BARK/EPHEDRA SPP. HERB/GLYCYRRHIZA SPP. ROOT/PAEONIA LACTIFLORA ROOT/PUERARIA LOBA	napdi_pt_to_combo
-7000933	Cinnamon	-7007817	CHROMIUM W/CINNAMOMUM VERUM/ZINC	napdi_pt_to_combo
-7000933	Cinnamon	-7007805	CINNAMON/CHRONDROITIN	napdi_pt_to_combo
-7000933	Cinnamon	-7007795	VITAMIN C-CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007788	CINNAMON AND GREEN TEA	napdi_pt_to_combo
-7000933	Cinnamon	-7007722	CINSULIN (CINNAMON AND CHROMIUM PICOLINATE)	napdi_pt_to_combo
-7000933	Cinnamon	-7007582	CINNAMON/CHROME	napdi_pt_to_combo
-7000933	Cinnamon	-7007577	KEISHIKASHAKUYAKUTO [CINNAMOMUM CASSIA BARK;GLYCYRRHIZA SPP. ROOT;PAEO	napdi_pt_to_combo
-7000933	Cinnamon	-7007573	CINNAMON WITH HONEY AND TEA COMBINATION	napdi_pt_to_combo
-7000933	Cinnamon	-7007566	GOSHAJINKIGAN (ACHYRANTHES BIDENTATA ROOT, ACONITUM SPP. PROCESSED ROOT, ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, CINNAMOMUM CASSIA BARK, CORNUS OFFICINALIS FRUIT, DIOSCOREA SPP. RHIZOME, PAEONIA X SUFFRUTICOSA ROOT BARK, PLANTAGO ASIATICA SEED, POR	napdi_pt_to_combo
-7000933	Cinnamon	-7007560	CINNAMOMUM CASSIA BARK, PAEONIA LACTIFLORA ROOT, PAEONIA X SUFFRUTICOS	napdi_pt_to_combo
-7000933	Cinnamon	-7007551	MAOTO (CINNAMOMUM CASSIA BARK, EPHEDRA SPP. HERB, GLYCYRRHIZA SPP. ROOT, PRUNIUS SPP. SEED)	napdi_pt_to_combo
-7000933	Cinnamon	-7007549	ASARUM SPP. ROOT, CINNAMOMUM CASSIA BARK, EPHEDRA SPP. HERB, GLYCYRRHI	napdi_pt_to_combo
-7000933	Cinnamon	-7007532	S.M. (CALCIUM CARBONATE, CINNAMOMUM VERUM POWDER, COPTIS TRIFOLIA, DIA	napdi_pt_to_combo
-7000933	Cinnamon	-7007514	CINNAMON+CHROM	napdi_pt_to_combo
-7000933	Cinnamon	-7007510	CHROMIUM/CINNAMOMUM VERUM/ZINC	napdi_pt_to_combo
-7000933	Cinnamon	-7007498	CINNAMON PLUS CHROMIUM METABOLISM SUPPORT	napdi_pt_to_combo
-7000933	Cinnamon	-7007494	HACHIMIJIOGAN (ACONITUM SPP. PROCESSED ROOT, ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, CINNAMOMUM CASSIA BARK, CORNUS OFFICINALIS FRUIT, DIOSCOREA SPP. RHIZOME, PAEONIA X SUFFRUTICOSA ROOT BARK, PORIA COCOS SCLEROTIUM, REHMANNIA GLUTINOSA ROOT)	napdi_pt_to_combo
-7000933	Cinnamon	-7007456	COQ WITH CINAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007438	ALPHA LIPSIS SOID.CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007428	CINNAMON 500 MG PLUS CHROMIUM METABOLISM SUPPORT	napdi_pt_to_combo
-7000933	Cinnamon	-7007419	CIMMAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007400	CINNAMON + CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007376	KAKKONTOKASENKYUSHIN'I [CINNAMOMUM CASSIA BARK;CNIDIUM OFFICINALE RHIZ	napdi_pt_to_combo
-7000933	Cinnamon	-7007365	CINNAMON W/CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007359	CINNAMON 1000 MG WITH CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007328	CHROMIUM;CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007315	KAKKONTO [CINNAMOMUM CASSIA BARK;EPHEDRA SPP.	napdi_pt_to_combo
-7000933	Cinnamon	-7007268	PURITAN'S PRIDE CINNAMON WITH HIGH POTENCY CHROMIUM COMPLEX	napdi_pt_to_combo
-7000933	Cinnamon	-7007246	VITAMIN E CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007234	GOREISAN /08015901/ (ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, ATRACTYLODES SPP. RHIZOME, CINNAMOMIUM CASSIA BARK, POLYPORUS UMBELLATUS SCLEROTIUM, PORIA COCOS SCLEROTIUM)	napdi_pt_to_combo
-7000933	Cinnamon	-7007224	CINNAMON AND HONEY	napdi_pt_to_combo
-7000933	Cinnamon	-7007214	CO-Q 10 W/CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007188	CINNAMON GARLIC	napdi_pt_to_combo
-7000933	Cinnamon	-7007180	^GOOD HERBS ORIGINAL^ TEA - ROOBOIS, ROSEHIPS, CHAMOMILE, CINNAMON, LE	napdi_pt_to_combo
-7000933	Cinnamon	-7007169	COQ10 WITH CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007158	MENTHOL/THYMOL/SYZYGIUM AROMATICUM OIL/CINNAMOMUM VERUM BARK OIL/EUCALYPTUS GLOBULUS OIL/FOENICULUM	napdi_pt_to_combo
-7000933	Cinnamon	-7007130	CINSULILN (CINNAMON, VITAMIN D3, 500IU)	napdi_pt_to_combo
-7000933	Cinnamon	-7007128	CINNAMONCHROMIUM SUPPLEMENT	napdi_pt_to_combo
-7000933	Cinnamon	-7007104	ATRACTYLODES LANCEA RHIZOME/ALISMA RHIZOME/POLYPORUS SCLEROTIUM/PORIA SCLEROTIUM/CINNAMON BARK	napdi_pt_to_combo
-7000933	Cinnamon	-7007098	CHROMIUM/CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7007090	BERBERTINE + CINNAMON BARK	napdi_pt_to_combo
-7000933	Cinnamon	-7007076	SAIREITO /08000901/ (ALISMA PLANTAGO-AQUATICA VAR. ORIENTALE TUBER, ATRACTYLODES LANCEA RHIZOME, BUPLEURUM FALCATUM ROOT, CINNAMOMUM CASSIA BARK, GLYCYRRHIZA SPP. ROOT, PANAX GINSENG ROOT, PINELLIA TERNATA TUBER, POLYPORUS UMBELLATUS SCLEROTIUM, PORIA COC	napdi_pt_to_combo
-7000933	Cinnamon	-7007073	LOOSE LEAF TEA (YERBA MATE) W/DANDELION ROOT+TUMERIC+CINNAMON+OREGAN+THYME	napdi_pt_to_combo
-7000933	Cinnamon	-7007004	CINNAMON/CHRM	napdi_pt_to_combo
-7000933	Cinnamon	-7006947	CABAGIN (CINNAMON, POWDERED, DIASTASE, LIQURICE, MENTHOL, NISIN, SCOPO	napdi_pt_to_combo
-7000933	Cinnamon	-7006914	KAKKONTO [CINNAMOMUM CASSIA BARK;EPHEDRA SPP. HERB;GLYCYRRHIZA SPP. RO	napdi_pt_to_combo
-7000933	Cinnamon	-7006802	CINNAMON/CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7006774	SHOSEIRYUTO [ASARUM SPP. ROOT;CINNAMOMUM CASSIA BARK;EPHEDRA SPP. HERB	napdi_pt_to_combo
-7000933	Cinnamon	-7006773	CINNAMON CITRACAL + D CLOTRIMAZOLE	napdi_pt_to_combo
-7000933	Cinnamon	-7006768	HONEY + CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7006743	CINNAMON PLUS CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7006726	RYOKEIJUTSUKANTO [ATRACTYLODES LANCEA RHIZOME;CINNAMOMUM CASSIA BARK;G	napdi_pt_to_combo
-7000933	Cinnamon	-7006725	CINNAMON AND CHROMIUM	napdi_pt_to_combo
-7000933	Cinnamon	-7006709	HERBAL SUPPLEMENT WITH CINNAMON	napdi_pt_to_combo
-7000933	Cinnamon	-7006648	CHROMIUM, CINNAMOMUM VERUM	napdi_pt_to_combo
-7000933	Cinnamon	-7006625	KAIGEN (CAFFEINE, CINNAMON, GLYCYRRHIZA EXTRACT, METHYLEPHEDRINE HYDRO	napdi_pt_to_combo
 */

--test combo products with cinnamon
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt_to_combo'
and c1.concept_id = -7000933
;
--same results as above

select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_combo_to_pt'
and c1.concept_id = -7006625
;
/*
-7006625	KAIGEN (CAFFEINE, CINNAMON, GLYCYRRHIZA EXTRACT, METHYLEPHEDRINE HYDRO	-7000933	Cinnamon	napdi_combo_to_pt
 */

--Test using cannabis
select * from staging_vocabulary.concept c where concept_class_id = 'Cannabis';
/*
-7000192	Cannibidiol[Cannabis sativa]	NaPDI research	NAPDI	Cannabis		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22
-7000193	CBD[Cannabis sativa]	NaPDI research	NAPDI	Cannabis		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22
-7000194	Da ma[Cannabis sativa]	NaPDI research	NAPDI	Cannabis		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22
-7000195	Hemp[Cannabis sativa]	NaPDI research	NAPDI	Cannabis		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22
-7000196	Hemp extract[Cannabis sativa]	NaPDI research	NAPDI	Cannabis		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22
-7000197	Marijuana[Cannabis sativa]	NaPDI research	NAPDI	Cannabis		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22
-7001260	Cannabis sativa[Cannabis sativa]	NaPDI research	NAPDI	Cannabis		f172ae99-9515-4bff-a8ed-4e68afad1b1d	2000-01-01	2099-02-22
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
   and c1.concept_id = -7000197
;
/*
Marijuana[Cannabis sativa]	-7000197	Cannabis	-7001094
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_pt_of'
   and c1.concept_id = -7001094
;
/*
Cannabis	-7001094	Cannabis sativa[Cannabis sativa]	-7001260
Cannabis	-7001094	Marijuana[Cannabis sativa]	-7000197
Cannabis	-7001094	Hemp extract[Cannabis sativa]	-7000196
Cannabis	-7001094	Hemp[Cannabis sativa]	-7000195
Cannabis	-7001094	Da ma[Cannabis sativa]	-7000194
Cannabis	-7001094	CBD[Cannabis sativa]	-7000193
Cannabis	-7001094	Cannibidiol[Cannabis sativa]	-7000192
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7000197
;
/*
Marijuana[Cannabis sativa]	-7000197	4-THUJANOL, CIS-(+/-)-	-7002663
Marijuana[Cannabis sativa]	-7000197	.GAMMA.-BISABOLENE, (Z)-	-7002641
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-CADINENE, (+)-	-7002628
Marijuana[Cannabis sativa]	-7000197	QUERCETIN	-7002589
Marijuana[Cannabis sativa]	-7000197	LINALOOL, (+/-)-	-7002586
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-PINENE	-7002549
Marijuana[Cannabis sativa]	-7000197	CANNABICHROMENE	-7002514
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-TERPINEOL	-7002509
Marijuana[Cannabis sativa]	-7000197	FENCHONE, (+/-)-	-7002504
Marijuana[Cannabis sativa]	-7000197	CANNABIVARIN	-7002482
Marijuana[Cannabis sativa]	-7000197	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID B	-7002469
Marijuana[Cannabis sativa]	-7000197	HUMULENE	-7002442
Marijuana[Cannabis sativa]	-7000197	TERPINOLENE	-7002421
Marijuana[Cannabis sativa]	-7000197	LINOLENIC ACID	-7002416
Marijuana[Cannabis sativa]	-7000197	.DELTA.-7-CIS-ISOTETRAHYDROCANNABIVARIN	-7002368
Marijuana[Cannabis sativa]	-7000197	.BETA.-PINENE	-7002346
Marijuana[Cannabis sativa]	-7000197	CANNABICHROMEVARIN	-7002324
Marijuana[Cannabis sativa]	-7000197	IPSDIENOL	-7002310
Marijuana[Cannabis sativa]	-7000197	APIGENIN	-7002307
Marijuana[Cannabis sativa]	-7000197	ORIENTIN	-7002303
Marijuana[Cannabis sativa]	-7000197	.DELTA.9-TETRAHYDROCANNABINOLIC ACID	-7002270
Marijuana[Cannabis sativa]	-7000197	CANNABIGEROLIC ACID MONOMETHYL ETHER	-7002254
Marijuana[Cannabis sativa]	-7000197	LUTEOLIN	-7002249
Marijuana[Cannabis sativa]	-7000197	3-CARENE	-7002243
Marijuana[Cannabis sativa]	-7000197	.DELTA.1-TETRAHYDROCANNABIORCOL	-7002215
Marijuana[Cannabis sativa]	-7000197	DRONABINOL	-7002208
Marijuana[Cannabis sativa]	-7000197	GROSSAMIDE	-7002200
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-EUDESMOL	-7002185
Marijuana[Cannabis sativa]	-7000197	CANNABIDIOLIC ACID	-7002178
Marijuana[Cannabis sativa]	-7000197	.BETA.-FENCHYL ALCOHOL	-7002169
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-THUJENE, (+/-)-	-7002164
Marijuana[Cannabis sativa]	-7000197	LIMONENE, (+/-)-	-7002154
Marijuana[Cannabis sativa]	-7000197	CAMPHENE	-7002060
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-LONGIPINENE	-7002029
Marijuana[Cannabis sativa]	-7000197	.BETA.-PHELLANDRENE	-7002021
Marijuana[Cannabis sativa]	-7000197	CANNABIDIVARIN	-7002016
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-SELINENE	-7001990
Marijuana[Cannabis sativa]	-7000197	CANNABICITRAN	-7001982
Marijuana[Cannabis sativa]	-7000197	.BETA.-EUDESMOL	-7001973
Marijuana[Cannabis sativa]	-7000197	.BETA.-OCIMENE, (3E)-	-7001937
Marijuana[Cannabis sativa]	-7000197	.BETA.-FARNESENE, (6Z)-	-7001931
Marijuana[Cannabis sativa]	-7000197	CANNABIGEROLIC ACID	-7001930
Marijuana[Cannabis sativa]	-7000197	TETRAHYDROCANNABIVARIN	-7001923
Marijuana[Cannabis sativa]	-7000197	CANNABIDIOL	-7001854
Marijuana[Cannabis sativa]	-7000197	YLANGENE	-7001850
Marijuana[Cannabis sativa]	-7000197	CANNABICHROMENIC ACID, (+)-	-7001846
Marijuana[Cannabis sativa]	-7000197	.BETA.-CARYOPHYLLENE OXIDE	-7001807
Marijuana[Cannabis sativa]	-7000197	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID A	-7001803
Marijuana[Cannabis sativa]	-7000197	CANNABINOL	-7001794
Marijuana[Cannabis sativa]	-7000197	3-BUTYL-.DELTA.9-TETRAHYDROCANNABINOL	-7001770
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-BISABOLOL, (-)-EPI-	-7001769
Marijuana[Cannabis sativa]	-7000197	N-CAFFEOYLTYRAMINE	-7001757
Marijuana[Cannabis sativa]	-7000197	BORNEOL	-7001739
Marijuana[Cannabis sativa]	-7000197	.BETA.-OCIMENE, (3Z)-	-7001731
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-FARNESENE	-7001708
Marijuana[Cannabis sativa]	-7000197	.BETA.-ELEMENE	-7001678
Marijuana[Cannabis sativa]	-7000197	OLEIC ACID	-7001668
Marijuana[Cannabis sativa]	-7000197	.ALPHA.-BERGAMOTENE, (E)-(-)-	-7001659
Marijuana[Cannabis sativa]	-7000197	.GAMMA.-BISABOLENE, (E)-	-7001655
Marijuana[Cannabis sativa]	-7000197	.DELTA.8-TETRAHYDROCANNABINOL	-7001652
Marijuana[Cannabis sativa]	-7000197	VITEXIN	-7001640
Marijuana[Cannabis sativa]	-7000197	MYRCENE	-7001636
Marijuana[Cannabis sativa]	-7000197	KAEMPFEROL	-7001594
Marijuana[Cannabis sativa]	-7000197	CARYOPHYLLENE	-7001565
Marijuana[Cannabis sativa]	-7000197	CANNABIGEROVARIN	-7001555
Marijuana[Cannabis sativa]	-7000197	LINOLEIC ACID	-7001523
Marijuana[Cannabis sativa]	-7000197	GUAIOL	-7001519
Marijuana[Cannabis sativa]	-7000197	.DELTA.-9-CIS-TETRAHYDROCANNABINOL, (-)-	-7001500
 */

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_pt_of'
   and cr2.relationship_id = 'napdi_has_const'
   and c1.concept_id = -7001094
;
/*
 Cannabis	-7001094	3-BUTYL-.DELTA.9-TETRAHYDROCANNABINOL	-7001770
Cannabis	-7001094	3-CARENE	-7002243
Cannabis	-7001094	4-THUJANOL, CIS-(+/-)-	-7002663
Cannabis	-7001094	.ALPHA.-BERGAMOTENE, (E)-(-)-	-7001659
Cannabis	-7001094	.ALPHA.-BISABOLOL, (-)-EPI-	-7001769
Cannabis	-7001094	.ALPHA.-CADINENE, (+)-	-7002628
Cannabis	-7001094	.ALPHA.-EUDESMOL	-7002185
Cannabis	-7001094	.ALPHA.-FARNESENE	-7001708
Cannabis	-7001094	.ALPHA.-LONGIPINENE	-7002029
Cannabis	-7001094	.ALPHA.-PINENE	-7002549
Cannabis	-7001094	.ALPHA.-SELINENE	-7001990
Cannabis	-7001094	.ALPHA.-TERPINEOL	-7002509
Cannabis	-7001094	.ALPHA.-THUJENE, (+/-)-	-7002164
Cannabis	-7001094	APIGENIN	-7002307
Cannabis	-7001094	.BETA.-CARYOPHYLLENE OXIDE	-7001807
Cannabis	-7001094	.BETA.-ELEMENE	-7001678
Cannabis	-7001094	.BETA.-EUDESMOL	-7001973
Cannabis	-7001094	.BETA.-FARNESENE, (6Z)-	-7001931
Cannabis	-7001094	.BETA.-FENCHYL ALCOHOL	-7002169
Cannabis	-7001094	.BETA.-OCIMENE, (3E)-	-7001937
Cannabis	-7001094	.BETA.-OCIMENE, (3Z)-	-7001731
Cannabis	-7001094	.BETA.-PHELLANDRENE	-7002021
Cannabis	-7001094	.BETA.-PINENE	-7002346
Cannabis	-7001094	BORNEOL	-7001739
Cannabis	-7001094	CAMPHENE	-7002060
Cannabis	-7001094	CANNABICHROMENE	-7002514
Cannabis	-7001094	CANNABICHROMENIC ACID, (+)-	-7001846
Cannabis	-7001094	CANNABICHROMEVARIN	-7002324
Cannabis	-7001094	CANNABICITRAN	-7001982
Cannabis	-7001094	CANNABIDIOL	-7001854
Cannabis	-7001094	CANNABIDIOLIC ACID	-7002178
Cannabis	-7001094	CANNABIDIVARIN	-7002016
Cannabis	-7001094	CANNABIGEROLIC ACID	-7001930
Cannabis	-7001094	CANNABIGEROLIC ACID MONOMETHYL ETHER	-7002254
Cannabis	-7001094	CANNABIGEROVARIN	-7001555
Cannabis	-7001094	CANNABINOL	-7001794
Cannabis	-7001094	CANNABIVARIN	-7002482
Cannabis	-7001094	CARYOPHYLLENE	-7001565
Cannabis	-7001094	.DELTA.1-TETRAHYDROCANNABIORCOL	-7002215
Cannabis	-7001094	.DELTA.-7-CIS-ISOTETRAHYDROCANNABIVARIN	-7002368
Cannabis	-7001094	.DELTA.8-TETRAHYDROCANNABINOL	-7001652
Cannabis	-7001094	.DELTA.-9-CIS-TETRAHYDROCANNABINOL, (-)-	-7001500
Cannabis	-7001094	.DELTA.9-TETRAHYDROCANNABINOLIC ACID	-7002270
Cannabis	-7001094	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID A	-7001803
Cannabis	-7001094	.DELTA.-9-TETRAHYDROCANNABIORCOLIC ACID B	-7002469
Cannabis	-7001094	DRONABINOL	-7002208
Cannabis	-7001094	FENCHONE, (+/-)-	-7002504
Cannabis	-7001094	.GAMMA.-BISABOLENE, (E)-	-7001655
Cannabis	-7001094	.GAMMA.-BISABOLENE, (Z)-	-7002641
Cannabis	-7001094	GROSSAMIDE	-7002200
Cannabis	-7001094	GUAIOL	-7001519
Cannabis	-7001094	HUMULENE	-7002442
Cannabis	-7001094	IPSDIENOL	-7002310
Cannabis	-7001094	KAEMPFEROL	-7001594
Cannabis	-7001094	LIMONENE, (+/-)-	-7002154
Cannabis	-7001094	LINALOOL, (+/-)-	-7002586
Cannabis	-7001094	LINOLEIC ACID	-7001523
Cannabis	-7001094	LINOLENIC ACID	-7002416
Cannabis	-7001094	LUTEOLIN	-7002249
Cannabis	-7001094	MYRCENE	-7001636
Cannabis	-7001094	N-CAFFEOYLTYRAMINE	-7001757
Cannabis	-7001094	OLEIC ACID	-7001668
Cannabis	-7001094	ORIENTIN	-7002303
Cannabis	-7001094	QUERCETIN	-7002589
Cannabis	-7001094	TERPINOLENE	-7002421
Cannabis	-7001094	TETRAHYDROCANNABIVARIN	-7001923
Cannabis	-7001094	VITEXIN	-7001640
Cannabis	-7001094	YLANGENE	-7001850
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_const_of'
   and c1.concept_id = -7001850
;
/*
YLANGENE	-7001850	Cannabis sativa[Cannabis sativa]	-7001260
YLANGENE	-7001850	Marijuana[Cannabis sativa]	-7000197
YLANGENE	-7001850	Hemp extract[Cannabis sativa]	-7000196
YLANGENE	-7001850	Hemp[Cannabis sativa]	-7000195
YLANGENE	-7001850	Da ma[Cannabis sativa]	-7000194
YLANGENE	-7001850	CBD[Cannabis sativa]	-7000193
YLANGENE	-7001850	Cannibidiol[Cannabis sativa]	-7000192
 */

select distinct c1.concept_name, c1.concept_id,  c3.concept_name, c3.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr1 on c1.concept_id = cr1.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr1.concept_id_2
     inner join staging_vocabulary.concept_relationship cr2 on c2.concept_id = cr2.concept_id_1
     inner join staging_vocabulary.concept c3 on c3.concept_id = cr2.concept_id_2
where cr1.relationship_id = 'napdi_is_const_of'
   and cr2.relationship_id = 'napdi_pt'
   and c1.concept_id = -7001850
;
/*
YLANGENE	-7001850	Cannabis	-7001094
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_spell_vr'
   and c1.concept_id = -7000197
;
/*
Marijuana[Cannabis sativa]	-7000197	UNSPECIFIED MEDICAL MARIJUANA	-7003343
Marijuana[Cannabis sativa]	-7000197	MARIJUANA NOS CANNABIS SATIVA	-7003342
Marijuana[Cannabis sativa]	-7000197	MARIJUANA	-7003341
Marijuana[Cannabis sativa]	-7000197	HEMP HEART	-7003340
Marijuana[Cannabis sativa]	-7000197	VAPORIZER CBD/MARIJUANA	-7003339
Marijuana[Cannabis sativa]	-7000197	MEDICAL CANNABIS CBD	-7003338
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (MEDICAL)	-7003337
Marijuana[Cannabis sativa]	-7000197	DISPENSARY MARIJUANA PLANT AND WAX CARTRIDGES.	-7003336
Marijuana[Cannabis sativa]	-7000197	EDIBLE MARIJUANA	-7003335
Marijuana[Cannabis sativa]	-7000197	HEMPSEED	-7003334
Marijuana[Cannabis sativa]	-7000197	MARIJUANA GUMMIES	-7003333
Marijuana[Cannabis sativa]	-7000197	MARIIJUANA	-7003332
Marijuana[Cannabis sativa]	-7000197	MARIJUANA VAPING PRODUCT	-7003331
Marijuana[Cannabis sativa]	-7000197	ACETAMINOPHEN/CODINE MEDICAL MARIJUANA	-7003330
Marijuana[Cannabis sativa]	-7000197	MAXIMUM STRENGTH HEMP EXTRACT OIL, 60MG/SERVING	-7003329
Marijuana[Cannabis sativa]	-7000197	^MARIJUANA CREAM^	-7003328
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA (MARIJUANA)	-7003327
Marijuana[Cannabis sativa]	-7000197	PROCANNA--HEMP OIL	-7003326
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (NO PREF. NAME)	-7003325
Marijuana[Cannabis sativa]	-7000197	MARIJUANA OIL	-7003324
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA	-7003323
Marijuana[Cannabis sativa]	-7000197	MARIJUANA/SYNTHETIC PILL FORM OF MARIJUANA (CANNABIS SATIVA)	-7003322
Marijuana[Cannabis sativa]	-7000197	MARIJANA	-7003321
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS, )	-7003320
Marijuana[Cannabis sativa]	-7000197	MARIJUANA VAPE	-7003319
Marijuana[Cannabis sativa]	-7000197	MARIJUANA WAX	-7003318
Marijuana[Cannabis sativa]	-7000197	CANNABIS RESIN	-7003317
Marijuana[Cannabis sativa]	-7000197	CBD HEMP FLOWER (CBDTHC)	-7003316
Marijuana[Cannabis sativa]	-7000197	MARIJUANNA	-7003315
Marijuana[Cannabis sativa]	-7000197	CBD COMPLEX CANNABIS SATIVA	-7003314
Marijuana[Cannabis sativa]	-7000197	MARIJUANA OIL VAPING	-7003313
Marijuana[Cannabis sativa]	-7000197	CANNABISCANNABIS CANNABIS SATIVA	-7003312
Marijuana[Cannabis sativa]	-7000197	NUTRITIONAL FRONTIERS FULL SPECTRUM HEMP EXTREME OIL (HEMP EXTRACT)	-7003311
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA TINCTURE OILS	-7003310
Marijuana[Cannabis sativa]	-7000197	MARIJUANA TBD HERB MEDICAL CENTER, 12509 OXNARD ST. N.HOLLYW	-7003309
Marijuana[Cannabis sativa]	-7000197	PEACE + WELLNESS ELEVATE (CBD/HEMP OIL INFUSED)	-7003308
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS SATIVA )	-7003307
Marijuana[Cannabis sativa]	-7000197	HASHISH	-7003306
Marijuana[Cannabis sativa]	-7000197	MARIJUANA SUPPLEMENTS (THC AND CBD)	-7003305
Marijuana[Cannabis sativa]	-7000197	CANNABINIODS	-7003304
Marijuana[Cannabis sativa]	-7000197	THC CANNABIS SATIVA	-7003303
Marijuana[Cannabis sativa]	-7000197	CANNABIS VAPING	-7003302
Marijuana[Cannabis sativa]	-7000197	HEMPLUCID CBD OIL 1000 MG VAPING	-7003301
Marijuana[Cannabis sativa]	-7000197	QUEEN CITY HEMP CBD	-7003300
Marijuana[Cannabis sativa]	-7000197	LEGAL MARIJUANA	-7003299
Marijuana[Cannabis sativa]	-7000197	MARAJUANA	-7003298
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS SATIVA) UNKNOWN	-7003297
Marijuana[Cannabis sativa]	-7000197	CANNABIS CANNIBIS SATIVA	-7003296
Marijuana[Cannabis sativa]	-7000197	MARIJUANA, N.O.S	-7003295
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNIBIS SATIVA)	-7003294
Marijuana[Cannabis sativa]	-7000197	CANNABISCANNABIS SATIVA	-7003293
Marijuana[Cannabis sativa]	-7000197	CANNABIOL	-7003292
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS, CANNABIS SATIVA)	-7003291
Marijuana[Cannabis sativa]	-7000197	ORIGINAL FORMULA HEMP EXTRACT OIL MINT CHOCOLATE FLAVOR CHARLOTTES WEB [CBD]	-7003290
Marijuana[Cannabis sativa]	-7000197	CBD OIL HEMP-DERIVED CANNABIDIOL FULL SPECTRUM HEMP SUPPL	-7003289
Marijuana[Cannabis sativa]	-7000197	MARIJUANA  (CANNABIS)	-7003288
Marijuana[Cannabis sativa]	-7000197	MARIJUANA DROPS	-7003287
Marijuana[Cannabis sativa]	-7000197	ILLEGAL MARIJUANA	-7003286
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS)	-7003285
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS SATIVA)	-7003284
Marijuana[Cannabis sativa]	-7000197	MEDICINAL MARIJUANA	-7003283
Marijuana[Cannabis sativa]	-7000197	CANNABIS BEDICA	-7003282
Marijuana[Cannabis sativa]	-7000197	VAPE PEN (MARIJUANA)	-7003281
Marijuana[Cannabis sativa]	-7000197	R.L.V. 500MG HEMP EXTRACT ISOLATE	-7003280
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA FLOWER	-7003279
Marijuana[Cannabis sativa]	-7000197	AMOS HEMPS [CBD OIL IN MCT OIL]	-7003278
Marijuana[Cannabis sativa]	-7000197	CTFO (CHANGING THE FUTURE OUTCOME) 10XPURE CBD HEMP OIL	-7003277
Marijuana[Cannabis sativa]	-7000197	RECREATIONAL MARIJUANA	-7003276
Marijuana[Cannabis sativa]	-7000197	OIL MARIJUANA	-7003275
Marijuana[Cannabis sativa]	-7000197	MARIJUANA FOR MEDICAL USE	-7003274
Marijuana[Cannabis sativa]	-7000197	MARIJUANA FLOWER	-7003273
Marijuana[Cannabis sativa]	-7000197	CBD OIL / HEMP OIL	-7003272
Marijuana[Cannabis sativa]	-7000197	MARIJUANA KUSH	-7003271
Marijuana[Cannabis sativa]	-7000197	MARIJUANA TINCTURE	-7003270
Marijuana[Cannabis sativa]	-7000197	TASTY GUMMIES FULL SPECTRUM HEMP OIL ASSORTED FRUIT FLAVORS 40 GUMMIES	-7003269
Marijuana[Cannabis sativa]	-7000197	CANNABIS, INDICA	-7003268
Marijuana[Cannabis sativa]	-7000197	MARIJUANA VAPING LIQUID	-7003267
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS SATIVA) (CANNABIS SATIVA)	-7003266
Marijuana[Cannabis sativa]	-7000197	MARIJUNA	-7003265
Marijuana[Cannabis sativa]	-7000197	CBD HEMP EXTRACT	-7003264
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA (HEMP HEARTS)	-7003263
Marijuana[Cannabis sativa]	-7000197	CALM REST AND RELAX HEMP EXTRACT FORMULA (CBG)	-7003262
Marijuana[Cannabis sativa]	-7000197	MARIJUANA    (CANNABIS SATIVA)	-7003261
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS,)	-7003260
Marijuana[Cannabis sativa]	-7000197	MED MARIJUANA	-7003259
Marijuana[Cannabis sativa]	-7000197	POT	-7003258
Marijuana[Cannabis sativa]	-7000197	MARIJUNAN	-7003257
Marijuana[Cannabis sativa]	-7000197	MARJUANA	-7003256
Marijuana[Cannabis sativa]	-7000197	THC CANNABIS STAIVA	-7003255
Marijuana[Cannabis sativa]	-7000197	CALM REST AND RELAX HEMP EXTRACT FORMULA (CBD)	-7003254
Marijuana[Cannabis sativa]	-7000197	60MG, PLANT?BASED CANNABINOIDS PER 1ML HEMP EXTRACT MINT CHOCOLATE FLA	-7003253
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS0	-7003252
Marijuana[Cannabis sativa]	-7000197	MARIJUANA                   (CANNABIS, CANNABIS SATIVA)	-7003251
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA (RSO)	-7003250
Marijuana[Cannabis sativa]	-7000197	CANNIBAL	-7003249
Marijuana[Cannabis sativa]	-7000197	CANNABIS CANNABIS SATIVA	-7003248
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA (CANNABIS SATIVA)	-7003247
Marijuana[Cannabis sativa]	-7000197	ORIGINAL FORMULA HEMP EXTRACT OLIVE OIL FLAVOR CHARLOTTES WEB [CBD]	-7003246
Marijuana[Cannabis sativa]	-7000197	MARIJUANA LIQUID	-7003245
Marijuana[Cannabis sativa]	-7000197	CANNABIS CANNABIS CANNABIS SATIVA	-7003244
Marijuana[Cannabis sativa]	-7000197	MARIJUANA  (CANNABIS	-7003243
Marijuana[Cannabis sativa]	-7000197	THC (PURE MARIJUANA IN PILL FORM) PRN	-7003242
Marijuana[Cannabis sativa]	-7000197	MEDICINAL RECREAT CANABIS HEMP F	-7003241
Marijuana[Cannabis sativa]	-7000197	MARIJUANA HERB, THC OILS	-7003240
Marijuana[Cannabis sativa]	-7000197	VAPORIZED MEDICINAL MARIJUANA	-7003239
Marijuana[Cannabis sativa]	-7000197	HEMPZ	-7003238
Marijuana[Cannabis sativa]	-7000197	CANNIBIS	-7003237
Marijuana[Cannabis sativa]	-7000197	HEMPANOL	-7003236
Marijuana[Cannabis sativa]	-7000197	CANNABIS INDICA	-7003235
Marijuana[Cannabis sativa]	-7000197	MARIJUANA HERB DEVICE USED: LOKEE BRAND VAPE^	-7003234
Marijuana[Cannabis sativa]	-7000197	METAGENICS, HEMP OIL BROAD SPECTRUM HEMP EXTRACT, 30ML BOTTLE WITH A 1	-7003233
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA OIL	-7003232
Marijuana[Cannabis sativa]	-7000197	HEMP SEED	-7003231
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA OIL	-7003230
Marijuana[Cannabis sativa]	-7000197	CANNABIS LOTIONS	-7003229
Marijuana[Cannabis sativa]	-7000197	ALLERGY INJECTIONS - CAT DANDER,DOG DANDER,MOLD MIX,HEMP	-7003228
Marijuana[Cannabis sativa]	-7000197	LEGALIZED MARIJUANA	-7003227
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNIBAS)	-7003226
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (MARIJUANA) UNKNOWN	-7003225
Marijuana[Cannabis sativa]	-7000197	CANNABIS BEDICA OLIE	-7003224
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA/CBD	-7003223
Marijuana[Cannabis sativa]	-7000197	MARIJUANA/ THC/ HEMP/HASH	-7003222
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA E SEMINIBUS	-7003221
Marijuana[Cannabis sativa]	-7000197	CANNIBIS SATIVA TINCTURE	-7003220
Marijuana[Cannabis sativa]	-7000197	HEMP MED EX: RSHO BLUE, GOLD	-7003219
Marijuana[Cannabis sativa]	-7000197	CANNABIS SMOKING	-7003218
Marijuana[Cannabis sativa]	-7000197	CANABIS LOTION	-7003217
Marijuana[Cannabis sativa]	-7000197	MARIJUANA             (MARIJUANA)	-7003216
Marijuana[Cannabis sativa]	-7000197	CYPRESS HEMP CBD OMEGAS	-7003215
Marijuana[Cannabis sativa]	-7000197	MARIJUANA EXTRACT	-7003214
Marijuana[Cannabis sativa]	-7000197	MARIJAUNA	-7003213
Marijuana[Cannabis sativa]	-7000197	CANNABIS SPRAY	-7003212
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA SUBSP. SATIVA FLOWERING TOP	-7003211
Marijuana[Cannabis sativa]	-7000197	CANNABIS CANNABIS SATIVA CON	-7003210
Marijuana[Cannabis sativa]	-7000197	QUEEN CITY HEMP 500MG (CBD)	-7003209
Marijuana[Cannabis sativa]	-7000197	CANNABISCANNABIS SATIVA L	-7003208
Marijuana[Cannabis sativa]	-7000197	PAX 3 VAPORIZER ^NOTHING WAS OBTAINED, BUT DRIED MARIJUANA WAS USED IN THE PAX 3 DEVICE	-7003207
Marijuana[Cannabis sativa]	-7000197	CBDISTILLERY 33MG CBD PER SERVING FULL SPECTRUM HEMP SUPPLEMENT	-7003206
Marijuana[Cannabis sativa]	-7000197	2 HITS OF MARIJUANA	-7003205
Marijuana[Cannabis sativa]	-7000197	MARIHUANA	-7003204
Marijuana[Cannabis sativa]	-7000197	MARIJUANA EDIBLES	-7003203
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (CANNABIS SATIVA)(CANNABIS SATIVA)	-7003202
Marijuana[Cannabis sativa]	-7000197	HEMPVANA HEEL TASTIC	-7003201
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA EXTRACT	-7003200
Marijuana[Cannabis sativa]	-7000197	COLESVAM MEDICAL MARIJUANA (CBD)	-7003199
Marijuana[Cannabis sativa]	-7000197	CBD EXTRACT	-7003198
Marijuana[Cannabis sativa]	-7000197	MARIJUA	-7003197
Marijuana[Cannabis sativa]	-7000197	MEDICINAL MARIJUANA (CANNABIS SATIVA)	-7003196
Marijuana[Cannabis sativa]	-7000197	CANNIMED OIL	-7003195
Marijuana[Cannabis sativa]	-7000197	WHITE RECLUSE MARIJUANA FLOWERS	-7003194
Marijuana[Cannabis sativa]	-7000197	THC MARIJUANA	-7003193
Marijuana[Cannabis sativa]	-7000197	HEMPWORX	-7003192
Marijuana[Cannabis sativa]	-7000197	CANNABIS TEA	-7003191
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA SUBSP. INDICA TOP	-7003190
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVACANNABIS CANNABIS SATIVA	-7003189
Marijuana[Cannabis sativa]	-7000197	HEMPTRANCE NATURAL CBD GUMMIES	-7003188
Marijuana[Cannabis sativa]	-7000197	PLUS CBD OIL HEMP DROPS PEPPERMINT EXTRA STRENGTH	-7003187
Marijuana[Cannabis sativa]	-7000197	MARIJUANA(MARIJUANA)	-7003186
Marijuana[Cannabis sativa]	-7000197	DRUG - MARIJUANA	-7003185
Marijuana[Cannabis sativa]	-7000197	VAPING MARIJUANA	-7003184
Marijuana[Cannabis sativa]	-7000197	CANNIBIDIOL OIL	-7003183
Marijuana[Cannabis sativa]	-7000197	MARIJUANA (MARIJUANA)	-7003182
Marijuana[Cannabis sativa]	-7000197	ADVANCED CBD OIL WITH TERPENES (FROM HEMP)	-7003181
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA CARD HOLDER	-7003180
Marijuana[Cannabis sativa]	-7000197	HEMP PROTEIN POWDER	-7003179
Marijuana[Cannabis sativa]	-7000197	MARIJUANA(CANNABIS)	-7003178
Marijuana[Cannabis sativa]	-7000197	MEDICAL MARIJUANA (PRESCRIPTION)	-7003177
Marijuana[Cannabis sativa]	-7000197	MEDICAL CANNABIS OIL VIOLET CBD	-7003176
Marijuana[Cannabis sativa]	-7000197	WAX, MARIJUANA	-7003175
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA CANNABIS SATIVA	-7003174
Marijuana[Cannabis sativa]	-7000197	CANNABIS INDICA SMOKE	-7003173
Marijuana[Cannabis sativa]	-7000197	TETRAHYDROCANNABINOL (MARIJUANA,HASH)	-7003172
Marijuana[Cannabis sativa]	-7000197	BLACK MAMBA CANNABIS SATIVA	-7003171
Marijuana[Cannabis sativa]	-7000197	PLATINUM AND DANK BRAND MARIJUANA CARTRIDGES (VAPE THCNICOTINE)	-7003170
Marijuana[Cannabis sativa]	-7000197	MARIJUANA                                        (CANNABIS)	-7003169
Marijuana[Cannabis sativa]	-7000197	HEMPVANA	-7003168
Marijuana[Cannabis sativa]	-7000197	K2/MARIJUANA	-7003167
Marijuana[Cannabis sativa]	-7000197	RESET BIOSCIENCE BALANCE 300MG 99%+ NANO LIPOSOMAL ORGANIC HEMP CBD	-7003166
Marijuana[Cannabis sativa]	-7000197	CANNABIS SATIVA	-7003165
Marijuana[Cannabis sativa]	-7000197	VAPORIZED MARIJUANA	-7003164
Marijuana[Cannabis sativa]	-7000197	CBD OIL (MEDICAL MARIJUANA)	-7003163
Marijuana[Cannabis sativa]	-7000197	HEMPWORX CBD OIL	-7003162
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
    and c1.concept_id = -7003162  
;
/*
HEMPWORX CBD OIL	-7003162	Cannabis sativa[Cannabis sativa]	-7001260
HEMPWORX CBD OIL	-7003162	Marijuana[Cannabis sativa]	-7000197
HEMPWORX CBD OIL	-7003162	Hemp extract[Cannabis sativa]	-7000196
HEMPWORX CBD OIL	-7003162	Hemp[Cannabis sativa]	-7000195
HEMPWORX CBD OIL	-7003162	Da ma[Cannabis sativa]	-7000194
HEMPWORX CBD OIL	-7003162	CBD[Cannabis sativa]	-7000193
HEMPWORX CBD OIL	-7003162	Cannibidiol[Cannabis sativa]	-7000192
 */

select c1.concept_name, c1.concept_id,  c2.concept_name, c2.concept_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
    and c1.concept_id = -7000197
;
--no results

--combo products
select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt_to_combo'
and c1.concept_id = -7001094
;
/*
-7001094	Cannabis	-7007507	ALLERGY INJECTIONS - CAT DANDER,DOG DANDER,MOLD MIX,HEMP	napdi_pt_to_combo
-7001094	Cannabis	-7007267	PLUS CBD OIL HEMP DROPS PEPPERMINT EXTRA STRENGTH	napdi_pt_to_combo
-7001094	Cannabis	-7006961	ACETAMINOPHEN/CODINE MEDICAL MARIJUANA	napdi_pt_to_combo
-7001094	Cannabis	-7006780	ADVANCED CBD OIL WITH TERPENES (FROM HEMP)	napdi_pt_to_combo
 */

select c1.concept_id, c1.concept_name, c2.concept_id, c2.concept_name, cr.relationship_id 
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_combo_to_pt'
and c1.concept_id = -7007507
;
/*
-7007507	ALLERGY INJECTIONS - CAT DANDER,DOG DANDER,MOLD MIX,HEMP	-7001094	Cannabis	napdi_combo_to_pt
 */


-- Supported use cases (TODO - show examples of these - Sanya): 
-- Query for all single NP products (RxNorm), common names, LB, and spelling variations and exclude anything noted as a combination product 
--get all common names and LBs 
with np_all as (
select distinct c1.concept_name np_name, c1.concept_id np_id, c1.concept_class_id np_concept_class_id
from staging_vocabulary.concept c1 
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_pt'
and c1.concept_class_id = 'Cinnamon'
union
--get all spelling variations (includes combos)
select distinct c1.concept_name np_name, c1.concept_id np_id, c1.concept_class_id np_concept_class_id
from staging_vocabulary.concept c1 
	 inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1  
     inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_is_spell_vr_of'
and c1.concept_class_id = 'NaPDI NP Spelling Variation'
and c2.concept_class_id = 'Cinnamon'
union
--get rxnorm mappings
select distinct c2.concept_name np_name, c2.concept_id np_id, c2.concept_class_id np_concept_class_id
from staging_vocabulary.concept c1
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
 and c1.concept_class_id = 'NaPDI Preferred Term'
 and c1.concept_code = 'Cinnamon'
 ),
 --get all combination terms to exclude
 np_combo as (
 select c1.concept_name np_name, c1.concept_id, c1.concept_class_id  
	from staging_vocabulary.concept c1 
	  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
	  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
	where cr.relationship_id = 'napdi_combo_to_pt'
 union
select c1.concept_name np_name, c1.concept_id, c1.concept_class_id 
	from staging_vocabulary.concept c1
	  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
	  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
	where cr.relationship_id = 'napdi_np_maps_to'
	 and c1.concept_class_id = 'NaPDI NP Combination Product'
 )
select distinct np_all.np_name
from np_all
except 
select np_combo.np_name
from np_combo

--left join np_combo on np_all.np_name = np_combo.np_name
--where np_combo.np_name is NULL
--order by np_name

select * from staging_vocabulary.concept c 
where c.concept_name = 'CINNAMON GARLIC'

select * from staging_vocabulary.concept c 
where c.concept_name = 'Chinese cinnamon extract'

select * from staging_vocabulary.concept c 
where c.concept_name = '1000 ML OLIVE OIL 160 MG/ML / SOYBEAN OIL 40 MG/ML INJECTION'

--N=1535
-- II. Query for all combination NP products (RxNorm and other) and spelling variations and exclude anything noted as a single NP product 
with np_combo_pt as (
	select c1.concept_id np_id, c1.concept_name np_name, c2.concept_id, c2.concept_name pt,
	c1.concept_class_id np_concept_class_id
	from staging_vocabulary.concept c1 
	  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
	  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
	where cr.relationship_id = 'napdi_combo_to_pt'
), np_combo_rxnorm as (
	select c1.concept_id np_id, c1.concept_name np_name, c2.concept_id, c2.concept_name,
	c1.concept_class_id np_concept_class_id
	from staging_vocabulary.concept c1
	  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
	  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
	where cr.relationship_id = 'napdi_np_maps_to'
	 and c1.concept_class_id = 'NaPDI NP Combination Product'
)
select distinct np_id, np_name, np_concept_class_id from np_combo_pt
union 
select distinct np_id, np_name, np_concept_class_id from np_combo_rxnorm
order by np_name
;


-- Query for all NPs PT/LB/common name that are mentioned in a combination products
--1. Spelling variations
with np_combo_pt as (
	select c1.concept_id, c1.concept_name np_name, c2.concept_id pt_id, c2.concept_name pt, cr.relationship_id 
	from staging_vocabulary.concept c1 
	  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
	  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
	where cr.relationship_id = 'napdi_combo_to_pt'
)
select distinct c.concept_id, c.concept_name, np_combo_pt.np_name
from staging_vocabulary.concept c
inner join staging_vocabulary.concept_relationship cr2 on cr2.concept_id_1 = c.concept_id 
inner join np_combo_pt on np_combo_pt.pt_id = cr2.concept_id_2 
where cr2.relationship_id = 'napdi_pt'

--2. RxNorm combos
--Rxnorm combos are not mapped to PTs?
select c1.concept_id, c1.concept_name np_name, c2.concept_id, c2.concept_name, cr.relationship_id,
c1.concept_class_id, c2.concept_class_id, c1.concept_code, c2.concept_code 
from staging_vocabulary.concept c1
  inner join staging_vocabulary.concept_relationship cr on c1.concept_id = cr.concept_id_1 
  inner join staging_vocabulary.concept c2 on c2.concept_id = cr.concept_id_2
where cr.relationship_id = 'napdi_np_maps_to'
 and c1.concept_class_id = 'NaPDI NP Combination Product'

select * from staging_vocabulary.concept c 
where c.concept_name = 'ALOE VERA PREPARATION / UREA'


--get concept class ids and relationships
/*
 * NaPDI NP Combination Product
NaPDI NP Constituent
NaPDI NP Spelling Variation
NaPDI Preferred Term
 */
select distinct c.concept_class_id 
from staging_vocabulary.concept c 
where c.concept_id < 0

/*
 * NaPDI NP Combination Product
NaPDI Preferred Term
NaPDI NP Spelling Variation
NaPDI NP Constituent
NaPDI Natural Product
 */
select distinct *
from staging_vocabulary.concept c 
where c.concept_class_id = 'Concept Class'
and c.concept_id < 0

select distinct * from staging_vocabulary.concept c 
where c.concept_id < 0
and c.concept_class_id = 'Relationship'

--napdi_has_const, napdi_is_const_of, napdi_pt, napdi_is_pt_of, napdi_spell_vr, napdi_is_spell_vr_of
select * from staging_vocabulary.concept_relationship cr
where cr.relationship_id  = 'napdi_has_const'
limit 100























