Obtaining NP common names from Health Canada (www.hc-sc.gc.ca)

This requires Python 3 and the Python libraries in requirements.txt

It also requires a complete listing of latin binomials a txt file in the input/ folder

1. Run webprod_to_local_HTML.py after editing the file paths to your local environment (if needed)
2. Run local_HTML_to_common_name.py to parse the output HTML to a JSON of latin binomials mapped to common names
3. Convert the JSON to your preferred output format

Note that Health Canada does a great job but is not complete. Further manual effort is needed to complete the mappings.


