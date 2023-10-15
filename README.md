# np-terminology-imports
A natural product terminology that maps botanical natural product (NP) Latin binomials to chemical constituents (e.g., metabolites), common names, and spelling variations. The vocabulary is represented using the OMOP vocabulary schema but is not dependent on OMOP/OHDSI tools. 

### How to use vocabulary

* Download [vocabulary tables](https://github.com/dbmi-pitt/np-terminology-imports/tree/main/Vocabulary-tables).
* Create schema and tables with [DDL code](https://github.com/dbmi-pitt/np-terminology-imports/wiki/Loading-the-Vocabulary).
* See [example queries](https://github.com/dbmi-pitt/np-terminology-imports/wiki/Example-Queries).

### Versions

* [v1.1.0](https://github.com/dbmi-pitt/np-terminology-imports/tree/main/Vocabulary-tables/May2023): added validated RxNorm mappings and combination products relationships for NPs with two or more NP names. Included more spelling variations from semi-automated matching process with Siamese neural network and fuzzy matching.
* [v1.0.1](https://github.com/dbmi-pitt/np-terminology-imports/tree/main/Vocabulary-tables/October2022): added mappings from NP concepts to RxNorm concepts (when available) - mappings are currently not verified and are based on string matching.
* [v1.0.0](https://github.com/dbmi-pitt/np-terminology-imports/tree/main/Vocabulary-tables/June2022): custom NP concepts, chemical constituents, and common names in OMOP. Spelling variations included for [67 natural products](https://github.com/dbmi-pitt/np-terminology-imports/wiki/Natural-Products-List).

| Version | concept | concept_relationship | relationship |
|---------|---------|----------------------|--------------|
| 1.1.0   | 8215    | 70308                | 9            |
| 1.0.1   | 5075    | 32717                | 9            |
| 1.0.0   | 5073    | 17913                | 7            |

For more details, see [wiki](https://github.com/dbmi-pitt/np-terminology-imports/wiki).
