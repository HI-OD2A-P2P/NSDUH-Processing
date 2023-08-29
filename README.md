# NSDUH-Processing
NSDUH dataset processing scripts for example or adoption

## General info on what this is and how it works:

SAMHDA: Substance Abuse and Mental Health Data Archive <br> 
NSDUH: National Survey on Drug Use and Health <br>
RDAS: Restricted-Use Data Access System <br>  

This program gathers SAMHDA NSDUH data via the RDAS API on both the state and 
county level, as well as processing the SAMHDA NSDUH shapefile data. 

It starts by calling the "load_state_and_county_data" method.  This first calls
the same method "get_nsduh_data" twice, passing in different parameters to 
indicate if it's getting the state data or the county data.  The 
"get_nsduh_data" method calls the api repeatedly, cycling through the various 
inputs (demographic categories, AKA "rows" and diagnoses, AKA "columns") while 
gathering the data and formatting it into something that works for us. 

Next, "load_state_and_county_data" calls the "get_shapefile_data" method which 
uses a shapefile reader to process the zip file urls found in the 
"shapefile_years" variable.  

Finally, it writes it all out to a csv file and then writes it all out to either 
a MySQL or a MSSQL database.  

To convert the rdas.samhsa.gov api url calls from the json format 
https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?...
to the human-readable graphic format, change the "api/surveys" part to "#/survey", like:
https://rdas.samhsa.gov/#/survey/NSDUH-2010-2019-RD10YR/crosstab/?...
and remove the "format=json" param at the end. Other than that, leave the other 
parameters intact to get the same results just in the human-readable format.
You'll need to hit the "Run Crosstab" button to get the results.


## How to run:

Before running:
1. Set the csv file and database variables.  Search for "fields you will need
to edit before running this" and edit the section below.  Make sure the 
"is_mssql" variable is set to true if you are using  mssql and false if you 
are using mysql.

2. Make sure you do not have an already existing csv file or database table
that matches the ones this program will attempt to create.  If there is one 
of these, either rename it, or change the "csv_file" or "db_table" variables 
as needed.

To run, go to the directory in which you have installed it and type:
python NSDUH.py

Be prepared for this to run for quite a while if you're doing a full run.  
Half an hour?  I never actually timed it, but it seems to take forever.
"""
