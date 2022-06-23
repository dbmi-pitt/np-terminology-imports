from lxml import etree
import os
import json
import re

FILE_PATH = "./html/"
OUTPUT_PATH = "./output/"

xpath_string = "body/div[@class='page']/div[@class='core']/div[@id='temp']/div[@class='center']/table[@class='subject']/tr"

common_dict = {}

# Iterate over every webpage saved locally and use XPATH to extract the common names
for filename in os.listdir(FILE_PATH):
    if filename.endswith(".html"):
        latin_binomial = filename.replace('-', ' ').replace('.html', '')
        print(latin_binomial)
        # Set a value in the dictionary, will be overwritten later if there is a value
        common_dict[latin_binomial] = ['No common name']


        parser = etree.HTMLParser()
        tree = etree.parse(FILE_PATH + filename, parser)
        table_rows = tree.xpath(xpath_string)

        # need to find "Organism" under the first column
        if len(table_rows) > 0:
            for i in range(len(table_rows)):
                row = tree.xpath(xpath_string + "[" + str(i + 1) + "]/td[1]/br")
                if len(row) > 0:
                    if '(Organism)' in row[0].tail:
                        # if there is a list
                        ul = tree.xpath(xpath_string + "[" + str(i + 1) + "]/td[3]/ul/li")
                        # if its just the common name
                        common_name = tree.xpath(xpath_string + "[" + str(i + 1) + "]/td[3]/div")

                        if len(ul) > 1:
                            common_names = []
                            for name in ul:
                                common_names.append(name.text)
                            common_dict[latin_binomial] = common_names
                            print(common_names)
                            break
                        elif len(common_name) > 0:
                            common_dict[latin_binomial] = [re.sub('\s+', '', common_name[0].text)]
                            print(re.sub('\s+', '', common_name[0].text))
                            break
                        else:
                            common_dict[latin_binomial] = ['No common name']
                            print("No common name")
                            break
        else:
            common_dict[latin_binomial] = ['No common name']
            print("No common name")

with open(OUTPUT_PATH + "latin_binomial_common_names.json", 'w+') as f:
    f.write(json.dumps(common_dict, indent=4, sort_keys=True))

#https://www.convertcsv.com/json-to-csv.htm - convert into table view
#Smilax glauca - no file for this latin binomial
