import pandas as pd
import os, re
import psycopg2

working_dir = os.getcwd()
DIR_OUT = ''
writer1 = pd.ExcelWriter(DIR_OUT+'NP_name_constituents_workbook.xlsx', engine='xlsxwriter')
writer2 = pd.ExcelWriter(DIR_OUT+'NP_FAERS_strings_workbook_version2.xlsx', engine='xlsxwriter')
writer3 = pd.ExcelWriter(DIR_OUT+'NP_constituents_workbook_with_ID.xlsx', engine='xlsxwriter')
workbook = writer2.book
wrap_format = workbook.add_format({'text_wrap': True})

np = [
"Ashwaganda",
"Butcher''s-broom",
"Cat''s-claw",
"Cinnamon",
"Fenugreek",
"Feverfew",
"Flax seed",
"Ginger",
"Green tea",
"Guarana",
"Hemp extract",
"Horse-chestnut",
"Karcura",
"Kratom",
"Lion''s-tooth",
"Maca",
"Miracle-fruit",
"Moringa",
"Niu bang zi",
"Panax ginseng",
"Purple-coneflower",
"Reishi",
"Rhodiola",
"Scrub-palmetto",
"Slippery elm",
"Soy",
"Stinging nettle",
"St. John''s-wort",
"Swallowwort",
"Tang-kuei",
"Tulsi",
"Woodland hawthorn",
"Wood spider"]

def get_NP_constituents(np_name, conn):
	query_constituents = """
	select distinct related_common_name, constituent_name, related_latin_binomial, substance_uuid
	from scratch_sanya.staging_gsrs_constituents sgc 
	where sgc.related_common_name = '{}'
	""".format(np_name)

	cur = conn.cursor()
	try:
		cur.execute(query_constituents)
		result_constituents = cur.fetchall()
		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	cur.close()
	return result_constituents

def get_NP_data(np_name, conn):
	query_names = """
	select ncfr.concept_class_id, regexp_replace(regexp_replace(regexp_replace(ncfr.concept_name, '\[.*\]','','g'), '\(.*\)','','g'),'''''','''','g') concept_name, 
	regexp_matches(ncfr.concept_name, '\\[.*\\]','g')  concept_name 
	from staging_vocabulary.concept ncfr 
	where ncfr.concept_class_id = '{}'
	""".format(np_name)

	'''
	query_faers_strings = """
	select distinct fdtn.np_name, fdtn.drug_name_original
	from scratch_rich.faers_drug_to_np fdtn inner join scratch_rich.np_concepts_first_run c on fdtn.concept_id = c.concept_id 
	where c.concept_class_id = '{}'
	order by fdtn.np_name 
	""".format(np_name)'''
	query_faers_strings = """
	select distinct fdtnl.np_name, fdtnl.drug_name_original
	from scratch_aug2021_amia.faers_drug_to_np_lev_2 fdtnl 
	where fdtnl.np_name = '{}' or fdtnl.np_name = '{}'
	order by fdtnl.np_name
	""".format(np_name, np_name.upper())

	cur = conn.cursor()
	try:
		cur.execute(query_names)
		result_names = cur.fetchall()
		cur.execute(query_faers_strings)
		result_faers = cur.fetchall()
		cur.close()
	except (Exception, psycopg2.DatabaseError) as error:
		print(error)
	cur.close()
	return result_names, result_faers

def create_NP_sheet(np_name, np_data, np_constituents):
	
	npwb_data = {'Natural_Product': [],
	'NP_synonyms_constituent': [],
	'Latin_binomial': []}
	for item in np_data:
		npwb_data['Natural_Product'].append(item[0].strip())
		npwb_data['NP_synonyms_constituent'].append(item[1].strip())
		lb_name = item[2][0]
		lb_name = lb_name.replace('[', '')
		lb_name = lb_name.replace(']', ' ')
		npwb_data['Latin_binomial'].append(lb_name.strip())

	for item in np_constituents:
		npwb_data['Natural_Product'].append(item[0].strip())
		npwb_data['NP_synonyms_constituent'].append(item[1].strip())
		npwb_data['Latin_binomial'].append('CONSTITUENT')
		
	np_df = pd.DataFrame(npwb_data)
	np_df.to_excel(writer1, sheet_name=np_name, index=False)
	worksheet = writer1.sheets[np_name]
	worksheet.set_column('A:C', 25, None)
	
def create_NP_faers_sheet(np_name, np_faers_data):
	npwb_data = {'NP_name': [],
	'FAERS_drug_match': [],
	'Plant_name (yes/no)': [],
	'Include in counts (yes/no)': []}

	for item in np_faers_data:
		npwb_data['NP_name'].append(item[0].strip())
		npwb_data['FAERS_drug_match'].append(item[1].strip())
		npwb_data['Plant_name (yes/no)'].append('Yes')
		#npwb_data['Constituent_name (yes/no/maybe)'].append('No')
		npwb_data['Include in counts (yes/no)'].append('Yes')
		
	np_faers_df = pd.DataFrame(npwb_data)
	np_faers_df.to_excel(writer2, sheet_name=np_name, index=False)
	worksheet = writer2.sheets[np_name]
	worksheet.set_column('A:B', 40, wrap_format)

def create_NP_constituents_sheet(np_name, np_cons_data):
	npwb_data = {
	'Substance ID': [],
	'Natural_Product': [],
	'Latin_binomial': [],
	'Constituent': [],
	'Specific (yes/no)': []}
	for item in np_cons_data:
		npwb_data['Substance ID'].append(item[3].strip())
		npwb_data['Natural_Product'].append(item[0].strip())
		npwb_data['Latin_binomial'].append(item[2].strip())
		npwb_data['Constituent'].append(item[1].strip())
		npwb_data['Specific (yes/no)'].append('Yes')
		
	np_cons_df = pd.DataFrame(npwb_data)
	np_cons_df.to_excel(writer3, sheet_name=np_name, index=False)
	worksheet = writer3.sheets[np_name]
	worksheet.set_column('A:E', 25, None)

if __name__ == '__main__':

	#connect to DB - cem
	try:
		conn = psycopg2.connect(connection details here)
	except Exception as error:
		print(error)
		print('Unable to connect to DB')
		conn = None
	if not conn:
		sys.exit(1)

	#Loop over NP names and create workbooks with names, constituents and FAERS drug matches
	for np_name in np:
		np_wb1, np_wb2 = get_NP_data(np_name, conn)
		#np_cons = get_NP_constituents(np_name, conn)
		#remove special chars from np name to create sheet name
		np_name = re.sub('[^a-zA-Z0-9 -]', '', np_name)
		#create_NP_sheet(np_name, np_wb1, np_cons)
		create_NP_faers_sheet(np_name, np_wb2)

	#Loop over NP names and create workbook for constituents
	'''for np_name in np:
		np_cons = get_NP_constituents(np_name, conn)
		np_name = re.sub('[^a-zA-Z0-9 -]', '', np_name)
		create_NP_constituents_sheet(np_name, np_cons)'''
		
	conn.close()
	#writer1.save()
	writer2.save()
	#writer3.save()