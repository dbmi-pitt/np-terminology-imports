#!/usr/bin/env python3

# Parses the GSRS export file of Latin Binomials and searches www.hc-sc.gc.ca to get web pages
# commonNamesFromLatin.py then parses these files using XPath to get the common names

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import requests

FILE_PATH = "./latin_binomial_export_GSRS_test_srs_np_202107131730.tsv"
OUTPUT_PATH = "./html/"

base_url = "http://webprod.hc-sc.gc.ca/nhpid-bdipsn/ingredsReq.do?srchRchTxt="
searchRole = "&srchRchRole=MedicinalRole&mthd=Search&lang=eng"

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)

latinBinomial = []
# Iterate over TSV and store latin binomial into array
with open(FILE_PATH, newline='') as f:
    reader = csv.reader(f)
    for row in reader:  # each row is a list
        latinBinomial.append(row[0])

# Make a request to webprod with the latin binomial and save the webpage to a local directory
for np in latinBinomial:
    if '_' not in np:
        try:
            response = requests.get(base_url + np.replace(' ', '+') + searchRole)
            if response.status_code == 200:
                with open(OUTPUT_PATH + np.replace(' ', '-') + ".html", 'w+') as f:
                    f.write(response.text)

        except Exception as e:
            print("Error with " + np)
