import sys
import json
import requests, os
import psycopg2, sys
import pickle

DEBUG = True
GET_DSLD = True
RERUN_SRS_API_CALLS = True
SRS_API_CALL_RESULTS_FILE = 'np-data-from-srs_03072023.pickle'

NP_DB_SCHEMA = 'scratch_sanya'
NP_DB_TABLE_PREFIX = 'test_srs_np'

np_to_binomial = {
        'Aloe vera':'Aloe vera',
        'Berberis aristata':'Barberry',
        'Beta vulgaris':'Beet root',
        'Boswellia serrata':'Boswellia',
        'Cannabis sativa':'Hemp extract',
        'Camellia sinensis':'Green tea',
        'Cinnamomum cassia':'Cinnamon',
        'Cinnamomum burmannii':'Cinnamon',
        'Cinnamomum loureiroi':'Cinnamon',
        'Cinnamomum verum':'Cinnamon',
        'Cinnamomum citriodorum':'Cinnamon',
        'Coleus forskholii':'Coleus',
        'Commiphora mukul':'Guggul',
        'Coptis chinensis':'Coptis',
        'Curcuma longa':'Turmeric',
        'Epimedium grandif':'Horny goat weed',
        'Epimedium orum':'Horny goat weed',
        'Equisetum hyemale':'Horsetail',
        'Garcinia gummi':'Garcinia',
        'Glycyrrhiza glabra':'Licorice',
        'Glycyrrhiza uralensis':'Licorice',
        'Glycyrrhiza inflata':'Licorice',
        'Hedera helix':'Ivy leaf',
        'Hordeum vulgare':'Barley grass',
        'Hydrastis canadensis':'Goldenseal',
        'Lepidium meyenii':'Maca',
        'Linum usitatissimum':'Flax seed',
        'Marrubium vulgare':'Horehound', 
        'Mitragyna speciosa':'Kratom', 
        'Moringa oleifera':'Moringa',
        'Origanum vulgare':'Oregano',
        'Pausinystalia johimbe':'Yohimbe',
        'Piper nigrum':'Black pepper',
        'Rhodiola rosea':'Rhodiola',
        'Sambucus nigra':'Elderberry',
        'Trigonelfa foenum':'Fenugreek',
        'Triticum aestivum':'Wheat grass',
        'Valeriana officinalis':'Valerian',
        'Withania somnifera':'Ashwaganda',
        'Zingiber officinale':'Ginger',
        'Malus domestica':'Apple cider vinegar',
        'Malus pumila':'Apple cider vinegar',
        'Chlorella vulgaris':'Chlorella',
        'Vaccinium macrocarpon':'Cranberry',
        'Oenothera biennis':'Evening primrose oil',
        'Allium sativum':'Garlic',
        'Citrus paradisi':'Grapefruit',
        'Nigella sativa':'Black cumin',
        'Oryza sativa':'Red yeast rice',
        'Senna alexandrina':'Senna',
        'Stevia rebaudiana':'Stevia',
        'Ophiocordyceps sinensis': 'Cordyceps',
        'Passiflora incarnata': 'Passion flower',
        'Strychnos ignatii': 'Ignatia',
        'Taxus baccata': 'English yew',
        'Houttuynia cordata': 'Chameleon',
        'Humulus lupulus': 'Hops',
        'Sterculia urens': 'Karaya gum',
        'Laminaria japonica': 'Kombu',
        'Lavandula angustifolia': 'Lavender',
        'Myristica fragrans': 'Nutmeg',
        'Panax notoginseng': 'Notoginseng',
        'Plantago indica': 'Black psyllium',
        'Plantago ovata': 'Blond psyllium',
        'Polygala senega': 'Senegaroot',
        'Rehmannia glutinosa': 'Rehmannia',
        'Salvia miltiorrhiza': 'Red sage',
        'Smilax china': 'Chinaroot',
        'Smilax glabra': 'Chinaroot',
        'Strychnos nux-vomica': 'Strychnine tree',
        'Helianthus annuus': 'Sunflower',
        'Papaver somniferum': 'Opium poppy',
        'Salvia officinalis': 'Sage',
        'Saccharina japonica': 'Kombu'
}

np = ['Glycyrrhiza uralensis','Glycyrrhiza inflata','Mitragyna speciosa','Abies alba','Abies borisii','Acacia brevispica','Acacia dealbata','Acacia saligna','Acanthus mollis','Acer floridanum','Acer rubrum','Achillea ageratum','Achillea falcata','Achillea millefolium','Acorus calamus','Actaea racemosa','Acuba sp','Adiantum capillus','Aesculus hippocastanum','Aesculus pavia','Aframomum angustifolium','Aframomum melegueta','Aframomum mildbraedii','Agathosma betulina','Agrimonia eupatoria','Albizia coriaria','Albizia julibrissin','Alcea rosea','Alcea setosa','Alchemilla xanthochlora','Alchornea hirtella','Allium amethystinum','Allium cepa','Allium sp','Alocasia macrorrhizos','Aloe vera','Aloysia citriodora','Alpinia officinarum','Amanita caesarea','Amaranthus spinosus','Ambrosia artemisiifolia','Amomum tsao','Amomum villosum','Amorpha fruticosa','Amsonia ciliata','Amsonia tabernaemontana','Anabasis aretioides','Anacyclus clavatus','Anchusa officinalis','Andropogon glomeratus','Andropogon virginicus','Aneilema beniniense','Aneilema nyasense','Anethum graveolens','Angelica sinensis','Anthyllis vulneraria','Apium graveolens','Aralia racemosa','Aralia spinosa','Arctium lappa','Arctostaphylos uva','Ardisia crenata','Areca catechu','Aristolochia baetica','Aristolochia fimbriata','Aristolochia rugosa','Aristotelia chilensis','Artemisia absinthium','Artemisia annua','Artemisia arborescens','Artemisia herba','Arthrocnemum macrostachyum','Artocarpus altilis','Arum italicum','Arundo donax','Asclepias curassavica','Asclepias erosa','Asclepias incarnata','Asimina angustifolia','Asimina incana','Asimina parviflora','Asparagus acutifolius','Asparagus racemosus','Asphodelus microcarpus','Astragalus monspessulanus','Astragalus propinquus','Atractylodes lancea','Atractylodes macrocephala','Atriplex portulacoides','Azadirachta indica','Bacopa monnieri','Balduina uniflora','Ballota nigra','Baptisia alba','Bauhinia guianensis','Bellardia trixago','Berberis aquifolium','Berberis aristata','Berberis lycium','Berberis vulgaris','Beta vulgaris','Bidens mitis','Bidens pilosa','Bixa orellana','Boehmeria cylindrica','Boletus aereus','Bontia daphnoides','Borago officinalis','Boswellia serrata','Brassica juncea','Brassica macrocarpa','Brassica oleracea','Brassica rapa','Bryophyllum fedtschenkoi','Bupleurum chinense','Callicarpa americana','Calluna vulgaris','Camellia sinensis','Campsis radicans','Cannabis sativa','Capparis spinosa','Capsella bursa','Carpinus caroliniana','Carthamus lanatus','Carya alba','Carya aquatica','Carya illinoinensis','Carya tomentosa','Cassine buchananii','Castanea dentata','Castanea henryi','Castanea hybrid','Castanea mollissima','Castanea neglecta','Castanea ozarkensis','Castanea pumila','Castanea sativa','Caulophyllum thalictroides','Cecropia peltata','Celtis gomphophylla','Celtis laevigata','Centaurea benedicta','Centaurium erythraea','Centaurium pulchellum','Centranthus ruber','Centrosema virginianum','Cephalanthus occidentalis','Ceratonia siliqua','Cercis canadensis','Cerinthe major','Ceterach officinarum','Chasmanthium latifolium','Chelidonium majus','Chimaphila umbellata','Chionanthus virginicus','Chrysopsis mariana','Cicer incisum','Cichorium intybus','Cinnamomum burmanni','Cinnamomum camphora','Cinnamomum cassia','Cinnamomum verum','Cirsium sp','Cistus creticus','Citrullus ecirrhosus','Citrullus lanatus','Citrus aurantium','Citrus reticulata','Citrus sinensis','Cladium mariscus','Cladonia leporina','Clematis glaucophylla','Clinopodium nepeta','Clinopodium vulgare','Clitoria mariana','Cnidoscolus urens','Codonopsis pilosula','Cola nitida','Coleus forskholii','Combretum molle','Commelina benghalensis','Commelina diffusa','Commiphora mukul','Conyza canadensis','Conyza ramosissima','Coptis chinensis','Cordia curassavica','Cordyceps militaris','Coreopsis major','Coriandrum sativum','Coriolopsis gallica','Cornus florida','Cornus officinalis','Corydalis diphylla','Corylus americana','Corylus avellana','Crassocephalum vitellinum','Crataegus aestivalis','Crataegus azarolus','Crataegus flava','Crataegus laevigata','Crataegus monogyna','Crataegus sp','Crepis neglecta','Crithmum maritimum','Crotalaria juncea','Crotalaria pallida','Croton argyranthemus','Croton echioides','Ctenium aromaticum','Cubitermes ugandensis','Cuminum cyminum','Cuphea carthagenensis','Curcuma longa','Curcuma zedoaria','Cyclamen hederifolium','Cynometra alexandri','Cyperus rotundus','Daedalea quercina','Daedaleopsis confragosa','Dalea pinnata','Daphne gnidium','Daphne oleoides','Daphne sericea','Daucus carota','Delonix regia','Delphinium fissum','Dianthus rupicola','Digitalis ferruginea','Digitaria cognata','Diodella teres','Dioscorea praehensilis','Dioscorea villosa','Diospyros virginiana','Diplotaxis erucoides','Diplotaxis tenuifolia','Dipsacus fullonum','Ditrysinia fruticosa','Dorylus sp','Drimia pancration','Drosera intermedia','Drypetes ugandensis','Ecballium elaterium','Echinacea angustifolia','Echinacea purpurea','Echium italicum','Eichhornia crassipes','Eleutherococcus senticosus','Elymus repens','Emilia sonchifolia','Encyclia tampensis','Enterolobium cyclocarpum','Epimedium grandif','Epimedium grandiflorum','Epimedium orum','Equisetum hyemale','Equisetum maximum','Erechtites hieracifolia','Erica multiflora','Erigeron strigosus','Eriobotrya japonica','Eriodictyon californicum','Eriogonum tomentosum','Erodium malacoides','Eruca vesicaria','Eryngium foetidum','Erythrina abyssinica','Erythrina herbacea','Erythrina lysistemon','Eschscholzia californica','Eupatorium capillifolium','Eupatorium compositifolium','Eupatorium fortunei','Eupatorium purpureum','Euphorbia antisyphilitica','Euphorbia characias','Euphorbia cyathophora','Euphorbia dendroides','Euphorbia pubentissima','Euphorbia segetalis','Euterpe oleracea','Fagus grandifolia','Ferula communis','Ferula elaeochytris','Ficus carica','Ficus saussureana','Filipendula ulmaria','Fistulina hepatica','Floscopa confusa','Foeniculum vulgare','Fomes fomentarius','Fomitopsis pinicola','Frangula alnus','Fraxinus americana','Fraxinus quadrangulata','Fucus vesiculosus','Fumana thymifolia','Fumaria officinalis','Fuscoporia torulosa','Galactites tomentosa','Galega officinalis','Galium aparine','Galium bermudense','Galium verum','Ganoderma lucidum','Garcinia gummi','Garcinia gummi','Gaylussacia dumosa','Gentiana lutea','Gentiana olivieri','Gentiana tianschanica','Geranium columbinum','Geranium maculatum','Ginkgo biloba','Glaucium flavum','Gloeophyllum sepiarium','Glycine max','Glycyrrhiza glabra','Glycyrrhiza glabra','Gnaphalium pensylvanicum','Gnaphalium purpureum','Gratiola virginiana','Grewia calymmatosepala','Gymnema sylvestre','Haloxylon scoparium','Hamamelis virginiana','Handroanthus heptaphyllus','Hapalopilus rutilans','Harpagophytum procumbens','Harungana madagascariensis','Hedera helix','Hedera helix','Helenium amarum','Helianthemum rosmarinifolium','Helichrysum arenarium','Helichrysum panormitanum','Helminthotheca echioides','Herpothallon rubrocinctum','Hesperaloe parviflora','Hibiscus moscheutos','Hippocrepis emerus','Hordeum vulgare','Hydrastis canadensis','Hydrastis canadensis','Hylodesmum repandum','Hylotelephium telephioides','Hymenocallis crassifolia','Hypericum gentianoides','Hypericum hypericoides','Hypericum perforatum','Hypericum piriai','Hypericum punctatum','Hypericum sp','Hyptis verticillata','Hyssopus officinalis','Ilex glabra','Ilex opaca','Ilex paraguariensis','Ilex vomitoria','Indigofera hirsuta','Infundibulicybe geotropa','Inocutis tamaricis','Inonotus obliquus','Inula helenium','Ipomoea cordatotriloba','Ipomoea pandurata','Iris virginica','Jacobaea maritima','Jatropha curcas','Juglans nigra','Juglans regia','Juncus articulatus','Juncus effusus','Juniperus communis','Juniperus oxycedrus','Juniperus virginiana','Kalanchoe mortagei','Khaya anthotheca','Knautia arvensis','Knautia lucana','Lachnanthes caroliniana','Laetiporus sulphureus','Lechea minor','Lechea mucronata','Lechea sessiliflora','Lechea tenuifolia','Leonurus cardiaca','Leopoldia comosa','Lepidium','Lepidium draba','Lepidium meyenii','Lepidium meyenii','Lepidium virginicum','Leucas calostachys','Levisticum officinale','Licania michauxii','Lilium candidum','Limbarda crithmoides','Limonium aegusae','Limonium tenuiculum','Linaria vulgaris','Linum usitatissimum','Liquidambar styraciflua','Liriodendron tulipifera','Lobaria pulmonaria','Lonicera implexa','Lonicera japonica','Lonicera webbiana','Ludwigia erecta','Ludwigia helminthorrhiza','Ludwigia leptocarpa','Ludwigia linearis','Lupinus perennis','Lycium barbarum','Lycopus americanus','Lyonia lucida','Maclura tinctoria','Maesa lanceolata','Magnolia grandiflora','Magnolia officinalis','Magnolia tripetala','Magnolia virginiana','Magydaris pastinacea','Malva sylvestris','Mammea americana','Markhamia lutea','Marrubium vulgare','Marrubium vulgare','Matelea gonocarpos','Matricaria chamomilla','Medicago polymorpha','Melia azedarach','Melilotus albus','Melissa officinalis','Mentha pulegium','Mentha spicata','Meripilus giganteus','Microgramma lycopodioides','Micromeria myrtifolia','Mikania scandens','Mimosa pudica','Mirabilis jalapa','Mitchella repens','Mitracarpus hirtus','Momordica charantia','Momordica sp','Morella cerifera','Morella kandtiana','Morella salicifolia','Morinda citrifolia','Moringa oleifera','Moringa oleifera','Morrenia odorata','Morus rubra','Myriophyllum aquaticum','Myrtus communis','Nectandra coriacea','Nelumbo lutea','Nepeta cataria','Nepeta cilicica','Nephrolepis cordifolia','Nerium oleander','Neurolaena lobata','Nicotiana glauca','Notopterygium incisum','Nuphar lutea','Nymphaea odorata','Ocimum tenuiflorum','Oecophylla longinoda','Oenothera fruticosa','Olea europaea','Ononis spinosa','Oplopanax horridus','Opuntia humifusa','Orbexilum lupinellum','Orchis anthropophora','Orchis italica','Orchis purpurea','Origanum compactum','Origanum ehrenbergii','Origanum libanoticum','Origanum majorana','Origanum syriacum','Origanum vulgare','Origanum vulgare','Oxalis priceae','Oxyria digyna','Panax ginseng','Papaver rhoeas','Papaver somniferum','Parentucellia viscosa','Parietaria judaica','Paronychia argentea','Parthenocissus quinquefolia','Paspalum notatum','Passiflora edulis','Paullinia cupana','Pausinystalia johimbe','Periploca laevigata','Persicaria hydropiperoides','Persicaria punctata','Petroselinum crispum','Peumus boldus','Phagnalon kotschyi','Phillyrea latifolia','Phlomis herba','Phlomis italica','Phlox glaberrima','Phyllanthus amarus','Phytolacca americana','Piloblephis rigida','Pinus echinata','Pinus heldreichii','Pinus mugo','Pinus nigra','Pinus palustris','Pinus peuce','Pinus sylvestris','Piper methysticum','Piper nigrum','Piptoporus betulinus','Piriqueta cistoides','Pistacia lentiscus','Pistacia terebinthus','Pistia stratiotes','Plantago aristata','Plantago major','Plantago virginica','Platanus occidentalis','Plectranthus hadiensis','Pleopeltis polypodioides','Pluchea rosea','Pogostemon cablin','Polygala grandiflora','Polygala nana','Polyporus squamosus','Pontederia cordata','Porodaedalea pini','Posidonia oceanica','Prangos asperula','Prunella vulgaris','Prunus angustifolia','Prunus armeniaca','Prunus caroliniana','Prunus persica','Prunus serotina','Prunus spinosa','Prunus umbellata','Pseudacanthotermes spiniger','Pseudarthria hookeri','Pseudognaphalium obtusifolium','Pseudoscabiosa limonifolia','Pseudotsuga menziesii','Ptelea trifoliata','Pteridium aquilinum','Pyrostegia venusta','Pyrus pashia','Quassia amara','Quercus alba','Quercus arkansana','Quercus cerris','Quercus falcata','Quercus geminata','Quercus ilex','Quercus incana','Quercus inopina','Quercus laevis','Quercus laurifolia','Quercus margaretta','Quercus marilandica','Quercus nigra','Quercus stellata','Quercus virginiana','Ramalina sp','Ranunculus acris','Raphanus raphanistrum','Reynoutria multiflora','Rhamnus lycioides','Rheum','Rheum australe','Rheum palmatum','Rhexia mariana','Rhexia virginica','Rhodiola rosea','Rhus copallinum','Rhus coriaria','Rinorea beniensis','Rivina humilis','Robinia pseudoacacia','Rosa canina','Rosa damascena','Rosa sp','Rosmarinus officinalis','Rubus allegheniensis','Rubus argutus','Rubus cuneifolius','Rubus flagellaris','Rubus laciniatus','Rubus leucodermis','Rubus parvifolius','Rubus praecox','Rubus sp','Rubus trivialis','Rubus ulmifolius','Rubus ursinus','Rudbeckia hirta','Ruellia caroliniensis','Rumex crispus','Rumex hastatulus','Ruscus aculeatus','Ruta chalepensis','Ruta graveolens','Sabal minor','Sagittaria graminea','Salix eriocephala','Salix nigra','Salix × fragilis L.','Salvia officinalis','Salvia pratensis','Salvia sclarea','Salvia sp','Salvia verbenaca','Salvia verticillata','Sambucus canadensis','Sambucus ebulus','Sambucus nigra','Sambucus nigra','Sanguinaria canadensis','Saponaria officinalis','Saposhnikovia divaricata','Sargassum pallidum','Sassafras albidum','Satureja montana','Saururus cernuus','Saussurea gossypiphora','Schinus terebinthifolia','Schisandra chinensis','Schisandra glabra','Schoenoplectus tabernaemontani','Scirpus cyperinus','Scolymus hispanicus','Scrophularia umbrosa','Scutellaria lateriflora','Securidaca longipedunculata','Senecio doriiformis','Senna obtusifolia','Senna occidentalis','Serenoa repens','Sesamum calycinum','Seseli bocconei','Sida spinosa','Sideroxylon celastrinum','Sideroxylon lanuginosum','Silene latifolia','Silene nutans','Silybum marianum','Sisymbrium officinale','Smilax auriculata','Smilax bona','Smilax glauca','Smilax laurifolia','Smilax pumila','Smilax rotundifolia','Smilax smallii','Smyrnium olusatrum','Solanum aculeastrum','Solanum americanum','Solanum anguivi','Solanum carolinense','Solanum linnaeanum','Solanum viarum','Solidago altissima','Solidago canadensis','Solidago odora','Sonchus oleraceus','Spartium junceum','Spermacoce verticillata','Sphagnum L.','Sphagnum sp','Stachys ehrenbergii','Stachys floridana','Stachys germanica','Stachys officinalis','Stachys tymphaea','Sterculia dawei','Stillingia sylvatica','Stylisma aquatica','Stylisma humistrata','Swertia chirata','Swertia petiolata','Symphytum officinale','Syngonanthus flavidulus','Tamarindus indica','Tanacetum falconeri','Tanacetum parthenium','Tanacetum vulgare','Taraxacum officinale','Tephrosia virginiana','Teucrium chamaedrys','Teucrium fruticans','Thapsia garganica','Thelypteris kunthii','Thunbergia fragrans','Thymbra capitata','Thymelaea hirsuta','Thymelaea microphylla','Thymelaea tartonraira','Thymus vulgaris','Tilia × europaea L.','Tillandsia fasciculata','Tillandsia setacea','Tillandsia usneoides','Toddalia asiatica','Tordylium apulum','Trametes versicolor','Tribulus terrestris','Trichaptum biforme','Trifolium badium','Trifolium ochroleucon','Trifolium repens','Trigonelfa foenum','Trigonella foenum','Tripsacum dactyloides','Triticum aestivum','Turnera diffusa','Tussilago farfara','Typha domingensis','Typha latifolia','Ulmus americana','Ulmus minor','Ulmus rubra','Uncaria tomentosa','Urena lobata','Urera trinervis','Urospermum dalechampii','Urtica dioica','Vaccinium arboreum','Vaccinium myrsinites','Vaccinium stamineum','Vaccinium tenellum','Valeriana officinalis','Valeriana officinalis','Vauquelinia californica','Verbascum sinuatum','Verbascum thapsus','Veronica chamaedrys','Viburnum opulus','Vicia cracca','Vicia faba','Vicia sativa','Vinca major','Vitex agnus','Vitis aestivalis','Vitis rotundifolia','Vitis vinifera','Warburgia ugandensis','Wisteria sinensis','Withania somnifera','Withania somnifera','Xanthium strumarium','Youngia japonica','Yucca filamentosa','Zanthoxylum armatum','Zanthoxylum chalybeum','Zanthoxylum clava','Zingiber officinale',
        'Smilax glabra', 'Saccharina japonica', 'Malus domestica', 'Malus pumila', 'Nigella sativa', 'Chlorella vulgaris', 'Vaccinium macrocarpon', 'Ophiocordyceps sinensis', 'Oenothera biennis', 'Allium sativum', 'Citrus paradisi', 'Oryza sativa', 'Senna alexandrina', 'Stevia rebaudiana','Passiflora incarnata', 'Strychnos ignatii', 'Taxus baccata', 'Houttuynia cordata', 'Humulus lupulus', 'Sterculia urens', 'Laminaria japonica', 'Lavandula angustifolia', 'Myristica fragrans', 'Panax notoginseng', 'Plantago indica', 'Plantago ovata', 'Polygala senega', 'Rehmannia glutinosa', 'Salvia miltiorrhiza', 'Sedum roseum', 'Smilax china', 'Strychnos nux-vomica', 'Helianthus annuus']

np_result = {}

#Function to query database for structurally diverse substance (kratom, goldenseal, green tea, cinnamon)
#Gets details from tables ix_ginas_substance, ix_ginas_name, ix_ginas_strucdiv, ix_ginas_relationship, 
#ix_ginas_code (for DSLD mapping) 
def get_initial_details(np_item):
        uri = "https://ginas.ncats.nih.gov/ginas/app/api/v1/substances/search?q=" + np_item
        response = requests.get(uri)
        result = response.json()
        if not result["content"]:
                return None

        np_result[np_item] = result
        return np_item

def get_whole_substance(np, np_result):
        search_name = np.upper()
        search_name_whole = search_name + ' WHOLE'
        result_substance = None
        result_substance_similar = None
        for content_item in np_result['content']:
                if content_item['substanceClass'] == 'structurallyDiverse':
                        if content_item['_name'] == search_name_whole:
                                result_substance = content_item
                                break
                        elif 'WHOLE' in content_item['_name']:
                                result_substance_similar = content_item
        
        if result_substance is None:
                if result_substance_similar is None:
                        for content_item in np_result['content']:
                                if content_item['substanceClass'] == 'structurallyDiverse':
                                        result_substance = content_item
                                        break
                else:
                        result_substance = result_substance_similar
                
        return result_substance

def clean_tables(conn):
        query_clean = ('DROP TABLE IF EXISTS ' + NP_DB_SCHEMA + '.' + NP_DB_TABLE_PREFIX,
                       'DROP TABLE IF EXISTS ' + NP_DB_SCHEMA + '.' + NP_DB_TABLE_PREFIX + '_parent',
                       'DROP TABLE IF EXISTS ' + NP_DB_SCHEMA + '.' + NP_DB_TABLE_PREFIX + '_rel',
                       'DROP TABLE IF EXISTS ' + NP_DB_SCHEMA + '.' + NP_DB_TABLE_PREFIX + '_part',
                       'DROP TABLE IF EXISTS ' + NP_DB_SCHEMA + '.' + NP_DB_TABLE_PREFIX + '_part_rel',
                       'DROP TABLE IF EXISTS ' + NP_DB_SCHEMA + '.' + NP_DB_TABLE_PREFIX + '_dsld')
        try:
                cur = conn.cursor()
                for query in query_clean:
                        if DEBUG:
                                print(query)
                        cur.execute(query)
                cur.close()
                conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
                cur.close()
                print(error)

def create_tables(conn):
        query_create = ("""
CREATE TABLE {}.{} (
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
)
""".format(NP_DB_SCHEMA, NP_DB_TABLE_PREFIX), """
CREATE TABLE {}.{}_parent (
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
)
""".format(NP_DB_SCHEMA ,NP_DB_TABLE_PREFIX), """
CREATE TABLE {}.{}_rel (
        related_latin_binomial varchar(255) NOT NULL,
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
""".format(NP_DB_SCHEMA,NP_DB_TABLE_PREFIX), """
CREATE TABLE {}.{}_part (
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
)
""".format(NP_DB_SCHEMA, NP_DB_TABLE_PREFIX), """
CREATE TABLE {}.{}_part_rel (
        related_latin_binomial varchar(255) NOT NULL,
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
""".format(NP_DB_SCHEMA,NP_DB_TABLE_PREFIX), """
CREATE TABLE {}.{}_dsld (
        related_latin_binomial varchar(255) NOT NULL,
        related_common_name varchar(40) NULL,
        uuid varchar(40) NULL,
        organism_family varchar(255) NULL,
        organism_genus varchar(255) NULL,
        organism_species varchar(255) NULL,
        dsld_code varchar(50) NOT NULL,
        dsld_text varchar(255) NULL
)
""".format(NP_DB_SCHEMA, NP_DB_TABLE_PREFIX))
                        
        try:
                cur = conn.cursor()
                for query in query_create:
                        if DEBUG:
                                print(query)
                        cur.execute(query)
                cur.close()
                conn.commit()

        except (Exception, psycopg2.DatabaseError) as error:
                cur.close()
                print(error)
        

def get_structurally_diverse_np(uuid, parent_uuid, conn, latin_binomial, common_name):
        query_main = """
with np_substance as (
select igs1.uuid as substance_uuid, igs1.* from ix_ginas_substance igs1
where igs1.uuid = '{}'
),
np_strucdiv as (
select * from ix_ginas_strucdiv igs2 
inner join np_substance on np_substance.structurally_diverse_uuid = igs2.uuid 
),
np_parent as (
select igs3.refuuid as parent_uuid from ix_ginas_substanceref igs3 
inner join np_strucdiv on np_strucdiv.parent_substance_uuid = igs3.uuid 
)
insert into {}.{} (related_latin_binomial, related_common_name, dtype, substance_uuid, created, "class", status, modifications_uuid, 
approval_id, structure_id, structurally_diverse_uuid, 
name_uuid, internal_references, owner_uuid, "name", 
"type", preferred, display_name, 
structdiv_uuid, source_material_class, source_material_state, source_material_type, 
organism_family, organism_author, organism_genus, organism_species, part_location, part, parent_substance_uuid)
select '{}' related_latin_binomial, '{}' related_common_name, igs.dtype, igs.uuid as substance_uuid, igs.created, igs.class, igs.status, igs.modifications_uuid,
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
""".format(uuid, NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, latin_binomial, common_name)

        if parent_uuid == '':
                query_parent = None
                query_relations = """
insert into {}.{}_rel (related_latin_binomial,
related_common_name, uuid, current_version,created,created_by_id,last_edited,last_edited_by_id,deprecated,record_access,internal_references,
owner_uuid,amount_uuid,"comments",interaction_type,qualification,related_substance_uuid,mediator_substance_uuid,
originator_uuid,"type",internal_version)
select '{}' related_latin_binomial, '{}' related_common_name, 
igr.uuid, igr.current_version, igr.created,igr.created_by_id,igr.last_edited,igr.last_edited_by_id,igr.deprecated,
igr.record_access, igr.internal_references, igr.owner_uuid,igr.amount_uuid, igr."comments", igr.interaction_type, igr.qualification,
igr.related_substance_uuid, igr.mediator_substance_uuid, igr.originator_uuid, igr."type", igr.internal_version
from ix_ginas_relationship igr
where igr.owner_uuid = '{}'
""".format(NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, latin_binomial, common_name, uuid)
        else:
                #do we want all parent synonyms or just single? Should we include a flag for parent in the main query??
                query_parent = """
with np_parent as (
select refuuid as parent_id from ix_ginas_substanceref igs2
where igs2.uuid = '{}'
)
insert into {}.{}_parent 
(related_latin_binomial, related_common_name, dtype, substance_uuid, created, "class", status, modifications_uuid, 
approval_id, structure_id, structurally_diverse_uuid, 
name_uuid, internal_references, owner_uuid, "name", 
"type", preferred, display_name, 
structdiv_uuid, source_material_class, source_material_state, source_material_type, 
organism_family, organism_author, organism_genus, organism_species, part_location, part, parent_substance_uuid)
select '{}' related_latin_binomial, '{}' related_common_name, igs.dtype, igs.uuid as substance_uuid, igs.created, igs.class, igs.status, igs.modifications_uuid,
igs.approval_id, igs.structure_id, igs.structurally_diverse_uuid, 
ign.uuid as name_uuid, ign.internal_references, ign.owner_uuid, ign."name",
ign."type", ign.preferred, ign.display_name, 
ixs.uuid as structdiv_uuid, ixs.source_material_class, ixs.source_material_state, ixs.source_material_type,
ixs.organism_family, ixs.organism_author, ixs.organism_genus, ixs.organism_species, ixs.part_location,
ixs.part, ixs.parent_substance_uuid 
from ix_ginas_substance igs 
inner join ix_ginas_name as ign on ign.owner_uuid = igs.uuid 
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid 
where igs.uuid in (select parent_id from np_parent)
""".format(parent_uuid, NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, latin_binomial, common_name)

                query_relations = """
with np_parent as (
select refuuid as parent_id from ix_ginas_substanceref igs2
where igs2.uuid = '{}'
)
insert into {}.{}_rel (related_latin_binomial,
related_common_name, uuid, current_version,created,created_by_id,last_edited,last_edited_by_id,deprecated,record_access,internal_references,
owner_uuid,amount_uuid,"comments",interaction_type,qualification,related_substance_uuid,mediator_substance_uuid,
originator_uuid,"type",internal_version)
select '{}' related_latin_binomial, '{}' related_common_name,
igr.uuid, igr.current_version, igr.created,igr.created_by_id,igr.last_edited,igr.last_edited_by_id,igr.deprecated,
igr.record_access, igr.internal_references, igr.owner_uuid,igr.amount_uuid, igr."comments", igr.interaction_type, igr.qualification,
igr.related_substance_uuid, igr.mediator_substance_uuid, igr.originator_uuid, igr."type", igr.internal_version 
from ix_ginas_relationship igr
where igr.owner_uuid = '{}' or igr.owner_uuid in (select parent_id from np_parent)
""".format(parent_uuid, NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, latin_binomial, common_name, uuid)

        query_dsld = """
select * from ix_ginas_code igc
where igc.owner_uuid = '{}' and igc.code_system = 'DSLD'
""".format(uuid)
                
        query_part = """
with np_substance_part as (
select igss.uuid as part_uuid from ix_ginas_substanceref igss
where igss.refuuid = '{}'
) 
insert into {}.{}_part (related_latin_binomial, related_common_name, dtype, substance_uuid, created, "class", status, modifications_uuid, 
approval_id, structure_id, structurally_diverse_uuid, 
name_uuid, internal_references, owner_uuid, "name", 
"type", preferred, display_name, 
structdiv_uuid, source_material_class, source_material_state, source_material_type, 
organism_family, organism_author, organism_genus, organism_species, part_location, part, parent_substance_uuid)
select '{}' related_latin_binomial, '{}' related_common_name, igs.dtype, igs.uuid as substance_uuid, igs.created, igs.class, igs.status, igs.modifications_uuid,
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
""".format(uuid, NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, latin_binomial, common_name)

#check if parts of the substance are 'structurally diverse' or 'mixture' and extract details accordingly

        query_part_rel = """
with np_substance_part as (
select igss.uuid as part_uuid from ix_ginas_substanceref igss
where igss.refuuid = '{}'
),
np_substance as (
select igs.uuid as substance_uuid, igs.dtype from ix_ginas_substance igs
inner join ix_ginas_strucdiv as ixs on ixs.uuid = igs.structurally_diverse_uuid
where ixs.parent_substance_uuid in (select part_uuid from np_substance_part)
and igs.dtype = 'DIV'
)
insert into {}.{}_part_rel (related_latin_binomial,
related_common_name, uuid, current_version,created,created_by_id,last_edited,last_edited_by_id,deprecated,record_access,internal_references,
owner_uuid,amount_uuid,"comments",interaction_type,qualification,related_substance_uuid,mediator_substance_uuid,
originator_uuid,"type",internal_version)
select '{}' related_latin_binomial, '{}' related_common_name,
igr.uuid, igr.current_version, igr.created,igr.created_by_id,igr.last_edited,igr.last_edited_by_id,igr.deprecated,
igr.record_access, igr.internal_references, igr.owner_uuid,igr.amount_uuid, igr."comments", igr.interaction_type, igr.qualification,
igr.related_substance_uuid, igr.mediator_substance_uuid, igr.originator_uuid, igr."type", igr.internal_version  
from ix_ginas_relationship igr
where igr.owner_uuid in (select substance_uuid from np_substance)
""".format(uuid, NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, latin_binomial, common_name)
        
        flag = 0
        cur = conn.cursor()
        try:       
                if DEBUG:
                        print(query_main)
                cur.execute(query_main)
                if query_parent is not None:
                        if DEBUG:
                                print(query_parent)
                        cur.execute(query_parent)
                if DEBUG:
                        print(query_relations)
                cur.execute(query_relations)
                if DEBUG:
                        print(query_part)  
                        print(query_part_rel)  
                cur.execute(query_part)
                cur.execute(query_part_rel)
                cur.close()
                conn.commit()
                flag = 1
        except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        cur.close()
        return flag

def run_dsld_query(conn):
        query_dsld = """
insert into {}.{}_dsld (
related_latin_binomial, related_common_name, uuid,organism_family, organism_genus, organism_species, dsld_code,
dsld_text
)
select distinct tsn.related_latin_binomial, tsn.related_common_name, tsn.substance_uuid, tsn.organism_family, tsn.organism_genus, tsn.organism_species, 
igc.code as dsld_code, regexp_replace(igc.comments, '^.*\|','') as dsld_text
from ix_ginas_code igc inner join {}.{} tsn on igc.owner_uuid = tsn.substance_uuid 
where igc.code_system = 'DSLD' 
union 
select distinct tsnp.related_latin_binomial, tsnp.related_common_name, tsnp.substance_uuid, tsnp.organism_family, tsnp.organism_genus, tsnp.organism_species, 
igc.code as dsld_code, regexp_replace(igc.comments, '^.*\|','') as dsld_text
from ix_ginas_code igc inner join {}.{}_part tsnp on igc.owner_uuid = tsnp.substance_uuid 
where igc.code_system = 'DSLD'
""".format(NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, NP_DB_SCHEMA, NP_DB_TABLE_PREFIX, NP_DB_SCHEMA, NP_DB_TABLE_PREFIX)

        cur = conn.cursor()
        try:       
                if DEBUG:
                        print(query_dsld)
                cur.execute(query_dsld)
                cur.close()
                conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
                print(error)
        cur.close()

if __name__ == '__main__':
        #connect to DB
        try:
                conn = psycopg2.connect("dbname='g_substance_reg' user='rw_grp' host='localhost' password='rw_grp'")
        except Exception as error:
                print(error)
                print('Unable to connect to DB')
                conn = None
        if not conn:
                sys.exit(1)

        clean_tables(conn)
        create_tables(conn)

        if RERUN_SRS_API_CALLS:
                print('INFO: rerunning SRS NP data and saving it to ' + SRS_API_CALL_RESULTS_FILE)
                for item in np:
                        t = get_initial_details(item)
                        if t == None:
                                print('No GSRS results found for: ' + item)
                                print('Trying to URL encode the space')
                                t = get_initial_details(item.replace(' ','%20'))
                                if t == None:
                                        print('Trying to URL encode did not work')
                                        continue
                        else:
                                print("Substance found: ", item)

                f = open(SRS_API_CALL_RESULTS_FILE,'wb')
                pickle.dump(np_result,f)
                f.close()

                ## now - reopen it so that we are working with python dict instead of a JSON object
                f = open(SRS_API_CALL_RESULTS_FILE,'rb')
                np_result = pickle.load(f)
                f.close()
        else:
                print('INFO: reloading SRS NP data from ' + SRS_API_CALL_RESULTS_FILE)
                try:
                        f = open(SRS_API_CALL_RESULTS_FILE,'rb')
                        np_result = pickle.load(f)
                        f.close()
                except Exception as error:
                        print('ERROR: an error occurred while loading ' + SRS_API_CALL_RESULTS_FILE + '.\n\t' + error)
                        sys.exit(1)
                        
                        
        #based on substance class from above, call function to query the database using the substance ID and parent ID
        flag = False
        for item in np:
                common_name = ''
                if np_to_binomial.get(item):
                        common_name = np_to_binomial[item]

                #get substance ID
                if not np_result.get(item):
                        print('INFO: No data on NP in np_result which should hold data from SRS :' + item)
                        continue
                else:
                        print('INFO: processing NP: ' + item)

                #extracting details of 1st structurallyDiverse substance from the result (this avoids trying to add 'concepts' to tables)
                result_substance = get_whole_substance(item, np_result[item])
                if result_substance is None:
                        print('Substance ', item, ' is not structurallyDiverse.')
                        continue
                
                uuid = result_substance["uuid"]
                
                if result_substance['structurallyDiverse'].get('parentSubstance'):
                        parent_uuid = result_substance["structurallyDiverse"]["parentSubstance"]["uuid"]
                else:
                        parent_uuid = ''
                print('INFO: NP is structurallyDiverse: ' + item)
                flag = get_structurally_diverse_np(uuid, parent_uuid, conn, item, common_name)

        if flag:
                print('Success')
        if flag and GET_DSLD:
                run_dsld_query(conn)

        conn.close()


