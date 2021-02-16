# reveal-dhis2-events
##Preamble
This solution enables exchange of information between Reveal and DHIS2 without the need for programmatic software integration. It allows data from a single Reveal activity or form to be pushed into DHIS2 program which has one stage with no registration. The solution consists of a dhis schema within the Reveal data warehouse database. This schema contains:
a number of mapping tables which need to be populated with DHIS2 metadata detailing the program, program stage, data elements, attribute option combinations and organisational units
A function which performs the transformation of the Reveal data into DHIS2 json
A table populated by the above function which contains the DHIS2 json and is queried by Superset to allow for download 

Reveal Superset relies on a materialized view to provide transformed data to the frontend and api. These data are pushed into the data warehouse as raw json events from OpenSRP by Nifi. A materialized view is created from the parsed json and is refreshed according to a specified schedule. The json documents are flattened into a tabular format using [postgres json notation](https://www.postgresql.org/docs/12/functions-json.html) .
