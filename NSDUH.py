"""
General info on what this is and how it works:

SAMHDA: Substance Abuse and Mental Health Data Archive 
NSDUH: National Survey on Drug Use and Health
RDAS: Restricted-Use Data Access System

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

"""

"""
How to run:

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

import os
import pandas as pd
import pdb
import re
import requests as req # pip install requests
import json
import geopandas as gpd # pip install geopandas
import csv
import shapefile # pip install pyshp

import pyodbc # pip install pyodbc
import mysql.connector as msql
from mysql.connector import Error
import sqlalchemy as sa 
# pip install sqlalchemy_pyodbc_mssql
# pip install sqlalchemy-access
from sqlalchemy import create_engine
#from sqlalchemy import text
#from sqlalchemy import create_engine, types

import io
import zipfile
import urllib
import ssl


# TODO:
# - Omit "no/unknown" data?
# - verify blank row_type, row_value, and county are all legit and not errors
# - record all the year/variable combos that cause 500 errors and print them out at the end so we can verify and maybe change the variables

# Done:
# - strip off things like "1 - " at the start of each row_value
# - figure out how to incorporate shapefile data
# - get rid of all caps, maybe just lower case
# - drop count_unweighted column, it's only value is null
# - rename count_weighted to just count
# - strip out col_value results of "no/unknown", only want positives, not negatives eg: (0 - No/Unknown, 2 - No, 0 - No Past Year SMI, 0 - No Past Yr Any Mental Illness, 0 - No)       
# - once we only have positive col_values, we can ditch the column entirely

# fields you will need to edit before running this

# check in version:
is_mssql = False
db_driver = "<Your driver here>"
db_host = '<Your database host here>'
db_name = '<Your database name here>'
db_table = '<Your table name here>'
db_user = "<Your Username Here>"
db_pwd = "<Your Password Here>"
query_d = {}
dir = "<path to csv file>"

pyodbc.pooling = False
ssl._create_default_https_context = ssl._create_unverified_context

csv_file = "nsduh.csv"
csv_path = f"{dir}{csv_file}"

"""
rows (AKA demographics):
CATAG2 Rc-Age Category Recode (3 Levels)
CATAG3 Rc-Age Category Recode (5 Levels)
EXPRACE Rc-Expanded Race Recode
IRSEX Gender - Imputation Revised
"""
rows = ["CATAG2","CATAG3","EXPRACE","IRSEX"]
"""
county_columns:
ABODMRJ Rc-Marijuana Dependence Or Abuse - Past Year
ABODALC Rc-Alcohol Dependence Or Abuse - Past Year
ABODHER Rc-Heroin Dependence Or Abuse - Past Year
ABODCOC Rc-Cocaine Dependence Or Abuse - Past Year
AMIYR_U Rc-Ami Ind (1/0) Based On Revised Predicted Smi Prob. This is any mental illness in past year
SMIYR_U Rc-Smi Ind (1/0) Based On Revised Predicted Smi Prob. This is any serious mental illness in past year
AMHTXRC3 Rc-Rcvd Any Mental Health Trt In Pst Yr
AMHTXND2 Rc-Perceived Unmet Need/did Not Rcv Mh Trt In Pst Yr
"""
county_columns = ["ABODMRJ","ABODALC","ABODHER","ABODCOC","AMIYR_U","SMIYR_U","AMHTXRC3","AMHTXND2"]
state_columns = ["ABODMRJ","ABODALC","ABODHER","ABODCOC","UDPYIEM","UDPYILL","UDPYMTH","UDPYOPI","UDPYHRPNR","AMIYR_U","SMIYR_U","AMHTXRC3","AMHTXND2","AMISUDPY","SMISUDPY","TXYRRECVD2"]

state_years = [{"data_source":"SAMHDA RDAS NSDUH 2 year (2018-2019) State Level Data","url_term":"NSDUH-2018-2019-RD02YR","year_range":"2018-2019","start_year":"2018","end_year":"2019", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2016-2017) State Level Data","url_term":"NSDUH-2016-2017-RD02YR","year_range":"2016-2017","start_year":"2016","end_year":"2017", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2014-2015) State Level Data","url_term":"NSDUH-2014-2015-RD02YR","year_range":"2014-2015","start_year":"2014","end_year":"2015", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2012-2013) State Level Data","url_term":"NSDUH-2012-2013-RD02YR","year_range":"2012-2013","start_year":"2012","end_year":"2013", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2010-2011) State Level Data","url_term":"NSDUH-2010-2011-RD02YR","year_range":"2010-2011","start_year":"2010","end_year":"2011", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2008-2009) State Level Data","url_term":"NSDUH-2008-2009-RD02YR","year_range":"2008-2009","start_year":"2008","end_year":"2009", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2006-2007) State Level Data","url_term":"NSDUH-2006-2007-RD02YR","year_range":"2006-2007","start_year":"2006","end_year":"2007", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2004-2005) State Level Data","url_term":"NSDUH-2004-2005-RD02YR","year_range":"2004-2005","start_year":"2004","end_year":"2005", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"data_source":"SAMHDA RDAS NSDUH 2 year (2002-2003) State Level Data","url_term":"NSDUH-2002-2003-RD02YR","year_range":"2002-2003","start_year":"2002","end_year":"2003", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"}]

county_years = [{"data_source":"SAMHDA RDAS NSDUH 10 year (2010-2019) County Level Data","url_term":"NSDUH-2010-2019-RD10YR","year_range":"2010-2019","start_year":"2010","end_year":"2019", "num_years":"10", "weight":"DASWT_4", "control":"STCTYCOD2","filter":"STCTYCOD2%3D85%2C83%2C84%2C82"},
                {"data_source":"SAMHDA RDAS NSDUH 16 year (2002-2017) County Level Data","url_term":"NSDUH-2002-2017-RD16YR","year_range":"2002-2017","start_year":"2002","end_year":"2017", "num_years":"16", "weight":"DASWT_8", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"data_source":"SAMHDA RDAS NSDUH 15 year (2002-2016) County Level Data","url_term":"NSDUH-2002-2016-RD15YR","year_range":"2002-2016","start_year":"2002","end_year":"2016", "num_years":"15", "weight":"DASWT_7", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"data_source":"SAMHDA RDAS NSDUH 14 year (2002-2015) County Level Data","url_term":"NSDUH-2002-2015-RD14YR","year_range":"2002-2015","start_year":"2002","end_year":"2015", "num_years":"14", "weight":"DASWT_6", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"data_source":"SAMHDA RDAS NSDUH 12 year (2002-2013) County Level Data","url_term":"NSDUH-2002-2013-RD12YR","year_range":"2002-2013","start_year":"2002","end_year":"2013", "num_years":"12", "weight":"DASWT_5", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"data_source":"SAMHDA RDAS NSDUH 10 year (2002-2011) County Level Data","url_term":"NSDUH-2002-2011-RD10YR","year_range":"2002-2011","start_year":"2002","end_year":"2011", "num_years":"10", "weight":"DASWT_4", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"}]

# abbreviated version for testing so it doesn't take forever to run.  Can change county_years to this and comment out get_nsduh_date for state along with shape stuff for a fast run.
#county_years = [{"url_term":"NSDUH-2010-2019-RD10YR","year_range":"2010-2019","start_year":"2010","end_year":"2019", "num_years":"10", "weight":"DASWT_4", "control":"STCTYCOD2","filter":"STCTYCOD2%3D85%2C83%2C84%2C82"}]

shapefile_years = [{"data_source":"SAMHDA NSDUH 2 year (2016-2018) Shapefile Data","url":"https://www.samhsa.gov/data/sites/default/files/reports/rpt29384/NSDUHsubstateShapeFile2018/ShapeFile2018.zip","year_range":"2016-2018","start_year":"2016","end_year":"2018","num_years":"2","state_pop":"1,160,319","Hawaii Island":"154486","Honolulu":"818512","Kauai":"56197","Maui":"131124"},
                    {"data_source":"SAMHDA NSDUH 2 year (2014-2016) Shapefile Data","url":"https://www.samhsa.gov/data/sites/default/files/cbhsq-reports/NSDUHsubstateShapeFile2016/ShapeFile2016.zip","year_range":"2014-2016","start_year":"2014","end_year":"2016","num_years":"2","state_pop":"1,162,034","Hawaii Island":"155455","Honolulu":"818697","Kauai":"56499","Maui":"131383"},
                    {"data_source":"SAMHDA NSDUH 2 year (2012-2014) Shapefile Data","url":"https://www.samhsa.gov/data/sites/default/files/NSDUHsubstateShapeFile2014/NSDUHsubstateShapeFile2014.zip","year_range":"2012-2014","start_year":"2012","end_year":"2014","num_years":"2","state_pop":"1,145,942","Hawaii Island":"154193","Honolulu":"806141","Kauai":"56012","Maui":"129596"},
                    {"data_source":"SAMHDA NSDUH 2 year (2010-2012) Shapefile Data","url":"https://www.samhsa.gov/data/sites/default/files/NSDUHsubstateShapeFile2012/NSDUHsubstateShapeFile2012.zip","year_range":"2010-2012","start_year":"2010","end_year":"2012","num_years":"2","state_pop":"1,123,500","Hawaii Island":"152588","Honolulu":"787623","Kauai":"55634","Maui":"127655"},
                    {"data_source":"SAMHDA NSDUH 2 year (2008-2010) Shapefile Data","url":"https://www.samhsa.gov/data/sites/default/files/Substate2k10-NSDUHsubstateShapefile2010/NSDUHsubstateShapefile2010.zip","year_range":"2008-2010","start_year":"2008","end_year":"2010","num_years":"2","state_pop":"1,069,970","Hawaii Island":"146741","Honolulu":"753234","Kauai":"52513","Maui":"117482"}]

shapefile_variables = [{"variable":"TXNPILA","row_value":"12 or older","description":"needing but not receiving treatment at a specialty facility for substance use in the past year (12 or older)"},
                        {"variable":"METAMYR","row_value":"12 or older","description":"methamphetamine use in the past year (12 or older)"},
                        {"variable":"PNRNMYR","row_value":"12 or older","description":"pain reliever misuse in the past year (12 or older)"},
                        {"variable":"TXNOSPA","row_value":"12 or older","description":"needing but not receiving treatment at a specialty facility for alcohol use in the past year (12 or older)"},
                        {"variable":"TXNOSPI","row_value":"12 or older","description":"needing but not receiving treatment at a specialty facility for illicit drug use in the past year (12 or older)"},
                        {"variable":"TXREC3","row_value":"18 or older","description":"received mental health services in the past year (18 or older)"},
                        {"variable":"UDPYILA","row_value":"12 or older","description":"substance use disorder in the past year (12 or older)"},
                        {"variable":"UDPYILL","row_value":"12 or older","description":"illicit drug use disorder in the past year (12 or older)"},
                        {"variable":"UDPYPNR","row_value":"12 or older","description":"pain reliever use disorder in the past year (12 or older)"},
                        {"variable":"ABODALC","row_value":"12 or older","description":"past year alcohol dependence or abuse (12 or older)"},
                        {"variable":"AMIYR","row_value":"18 or older","description":"any mental illness (AMI) in the past year (18 or older)"},
                        {"variable":"COCYR","row_value":"12 or older","description":"past year use of marijuana (12 or older)"},
                        {"variable":"SMIYR","row_value":"18 or older","description":"serious mental illness (SMI) in the past year (18 or older)"}]

# list of shapefile col_values that result in us omitting the row.  We only want positives, not negatives
omit_results = ["0 - No/Unknown", "2 - No", "0 - No Past Year SMI", "0 - No Past Yr Any Mental Illness", "0 - No"]

# - Weird results on https://rdas.samhsa.gov/#/survey/NSDUH-2010-2019-RD10YR/crosstab/?column=ABODMRJ&control=STCTYCOD2&filter=STCTYCOD2%3D85%2C83%2C84%2C82&results_received=true&row=CATAG2&run_chisq=false&weight=DASWT_4
#   - turns out it's a rounding issue.  Not a problem

# turns out the older years use different values for the counties than the latest.
# fortunately, there was no overlap, so I just put them all in the same dict for simplicity
counties = {"74":"Hawaii", "75":"Honolulu","76":"Maui","77":"County Not Specified","82":"Hawaii", "83":"Honolulu","84":"Kauai","85":"Maui"}

# main driver of everything.  Calls methods to get county data, 
# state data, write it out to a csv (as a backup), and then out to the database
def load_state_and_county_data():
    results = set()
    get_nsduh_data(True, results) # county
    get_nsduh_data(False, results) # state
    get_shapefile_data(results)
    df = write_json_to_csv_file(results)
    write_data_frame_to_db(df)

# used in get_nsduh_data to generate a url using the given arguments and return the response from doing a get on that url
def get_url_data(index, base_url, control, row, column, filter, weight):
    local_url = f"{base_url}?control={control}&row={row}&column={column}&filter={filter}&weight={weight}&run_chisq=false&format=json"
    print(f"\n\nindex: {index}, control: {control}, row: {row}, column: {column},  filter: {filter}, weight: {weight}")
    if not control:
        local_url = f"{base_url}?row={row}&column={column}&filter={filter}&weight={weight}&run_chisq=false&format=json"
        #print(f"\n\nno control, index: {index}, row: {control}, column: {column},  filter: {filter}, weight: {weight}")
    print(f"{local_url}")
    resp = req.get(local_url)
    print(f"{resp}\n\n") # this just prints "<Response [code num]>"
    return resp

# this goes through all the combinations of control, rows, and columns. 
# If it gets a response code 400, {"errorCode":"DISCLOSURE_LIMITATION"}
# I drop the row (demographics), moving the control to be the new row value, then try again.
# In most cases, this is enough to get data, there are still a few that are failing.
# Once I have data, it then calls a method which parses out the data we want
def get_nsduh_data(isCounty, results):
    print(f"in get_all_nsduh_data")
    try:
        # set default values as if we're getting state level data
        columns = state_columns
        years = state_years
        # if we're getting county date, reset default values appropriately
        if isCounty:    
            columns = county_columns
            years = county_years
        # loop through all the different year ranges that we know of and get the data for them all
        for year in years:
            index = 0
            url_term = year["url_term"]
            base_url = f"https://rdas.samhsa.gov/api/surveys/{url_term}/crosstab/"
            data_source = year["data_source"]
            ctl = year["control"]
            fil = year["filter"]
            wt = year["weight"]
            start_year = year["start_year"]
            end_year = year["end_year"]
            year_range = year["year_range"]
            print(f"start_year: {start_year}, end_year: {end_year}, year_range: {year_range}")
            for row in rows:
                for column in columns:
                    index = index + 1
                    resp = get_url_data(index, base_url, ctl, row, column, fil, wt)
                    if resp.status_code == 200:
                        parse_data(isCounty, json.loads(resp.text), results, True, start_year, end_year, year_range, data_source)
                    # if previous attempt didn't work and got a 400, it's likely throttled due to disclosure limitations,
                    # move control to row and drop the previous row which was a demographic thing
                    elif resp.status_code == 400 and resp.text == '{"errorCode":"DISCLOSURE_LIMITATION"}':
                        print(f"resp.reason: {resp.reason}")
                        print(f"resp.text: {resp.text}")
                        resp = get_url_data(index, base_url, "", ctl, column, fil, wt)
                        if resp.status_code == 200:
                            parse_data(isCounty, json.loads(resp.text), results, False, start_year, end_year, year_range, data_source)
                        elif resp.status_code == 400:
                            print(f"resp.reason: {resp.reason}")
                            print(f"resp.text: {resp.text}")
    except Exception as err:
        print(f"get_nsduh_data error: {err}")
    print(f"leaving get_nsduh_data")

# called by parse_data to generate a dict using all the given parameters.
# kinda brain-dead, but I wanted to make sure if one got changed, they both got changed, and this would assure that.
def make_cell_dict(county, row_type, col_type, row_value, count, start_year, end_year, year_range, data_source):
    #return f"'Hawaii', {county}, {row_type}, {col_type}, {row_value}, {count_weighted}"
    row_type = row_type.lower()
    col_type = col_type.lower()
    if county and county == "Hawaii Island":
        county = "Hawaii"
    if row_type  and row_type.lower() == "imputation revised gender":
        row_type = "gender"
    if row_type  and row_type == "gender - imputation revised":
        row_type = "gender"
    if row_type  and row_type == "age category recode (3 levels)":
        row_type = "age (3 levels)" 
    if row_type  and row_type == "age category recode (5 levels)":
        row_type = "age (5 levels)" 
    if row_type  and row_type == "rc-age category recode (3 levels)":
        row_type = "age (3 levels)"    
    if row_type  and row_type == "rc-age category recode (5 levels)":
        row_type = "age (5 levels)"      
    if col_type and col_type == "adobalc: past year alcohol dependence or abuse":
        col_type = "alcohol abuse or dependence - past year"
    if col_type and col_type == "rc-alcohol dependence or abuse - past year":
        col_type = "alcohol abuse or dependence - past year"
    if col_type and col_type == "rc-ami ind (1/0) based on revised predicted smi prob":
        col_type = "ami ind (1/0) based on revised predicted smi prob"
    if col_type and col_type == "rc-cocaine dependence or abuse - past year":
        col_type = "cocaine abuse or dependence - past year"
    if col_type and col_type.startswith("rc-"):
        col_type = col_type[3:]
    return dict({
        "data_source": data_source,
        "state": "Hawaii",
        "county": county,
        "category": row_type,
        "category_type": row_value,
        "diagnosis": col_type,
        "count": count,
        "start_year": start_year,
        "end_year": end_year,
        "year_range": year_range
    })

# Given jsondata, it loops through all the cells
# - parses the data for each cell, pulling out what we want
# - creates a json item for each set, formatting it so all entries will be consistent in spite of original json differences
# this method is _messy_ as it has to account for the json being formatted differently 
# if the json was generated from a url with a control param or not.
# results - storage for all the data I'm parsing out.  Gets modified directly, so there's no return value from this
# hasControl - True if the jsondata was generated using a url that had a control parameter, 
#              False if the control was moved to the row param
def parse_data(isCounty, jsondata, results, hasControl, start_year, end_year, year_range, data_source):
    print(f"in parse_data: hasControl: {hasControl}")
    try:
        jsondata = jsondata["results"]
        # make dicts of the row and column options, we need this to translate the row/column numbers into human-readable text
        row_dict = make_dict_from_json(jsondata, "row")
        col_dict = make_dict_from_json(jsondata, "column")
        cells = jsondata["cells"]
        for cell in cells:
            row_option = cell["row_option"]
            col_option = cell["column_option"]
            #print(f"3: {row_option}, {col_option}, {hasControl}, {isCounty}")
            # don't bother with anything unless row and column values are set for that cell since we don't want totals
            if row_option and col_option:
                # Only let the positives go through, don't care about "no" answers
                if col_dict[col_option] not in omit_results:
                    if not hasControl:
                        county_value = ""
                        if isCounty:
                            county_value = counties[row_option]
                        d = make_cell_dict(county_value, "", col_dict["title"], "", cell["count"]["weighted"], start_year, end_year, year_range, data_source)
                        #print(f"dict: {d}")
                        results.add(tuple(d.items()))
                    else:
                        county_value = ""
                        control_option = cell["control_option"]
                        if control_option:
                            county_value = ""
                            if isCounty:
                                county_value = counties[control_option]
                            row_val = row_dict[row_option]
                            # strip off the number dash thing at the beginning
                            if (row_val and (len(row_val) > 5)):
                                row_val = row_val[4:]
                            d = make_cell_dict(county_value, row_dict["title"], col_dict["title"], row_val, cell["count"]["weighted"], start_year, end_year, year_range, data_source)
                            #print(f"dict: {d}")
                            results.add(tuple(d.items()))
        print("success")
    except Exception as err:
        print(f"parse_data error: error.text: error: {err} ")
    print("leaving parse_data")

# used for getting keys and titles of rows and columns, works for both state and county data
def make_dict_from_json(jsondata, key):
    top_level = jsondata[key]
    
    d = dict()
    d.update({"title": top_level["title"]})

    options = top_level["options"]
    for option in options:
        key = option["key"]
        value = option["title"]
        d.update({key: value})
    #print(d)
    return d

def get_shapefile_data(results):
    print(f"in get_shapefile_data: ")
    # page the link is found on:
    # https://www.samhsa.gov/data/report/2016-2018-nsduh-substate-region-shapefile
    # other years, for some, you need to click on the htm link and then click on the download link on that page.  Not consistent.
    # first link is main page, second link is the zip file, third is the population table (Table C1).  
    # Table C1 is found under "Other Related Reports" tab with label saying something like 
    # "NSDUH Guide to Substate Tables and Summary of Small Area Estimation Methodology"
    # or "NSDUH Overview and Summary of Substate Region Estimation Methodology"
    # Found all these via a search: https://www.samhsa.gov/data/all-reports?f%5B0%5D=location%3A182&keys=shapefile&sort_bef_combine=field_date_printed_on_report_DESC
    # 2008-2010
    # https://www.samhsa.gov/data/report/2008-2010-nsduh-substate-region-shapefile
    # https://www.samhsa.gov/data/sites/default/files/Substate2k10-NSDUHsubstateShapefile2010/NSDUHsubstateShapefile2010.zip
    # https://www.samhsa.gov/data/sites/default/files/Substate2k10-Methodology/Methodology/NSDUHsubstateMethodology2010.htm#TabC1
    # 2010-2012
    # https://www.samhsa.gov/data/report/2010-2012-nsduh-substate-region-shapefile-zip-file-download
    # https://www.samhsa.gov/data/sites/default/files/NSDUHsubstateShapeFile2012/NSDUHsubstateShapeFile2012.zip
    # https://www.samhsa.gov/data/sites/default/files/substate2k12-Methodology/NSDUHsubstateMethodology2012.htm#tabc1
    # 2012-2014
    # https://www.samhsa.gov/data/report/2012-2014-nsduh-substate-region-shapefile
    # https://www.samhsa.gov/data/sites/default/files/NSDUHsubstateShapeFile2014/NSDUHsubstateShapeFile2014.zip
    # https://www.samhsa.gov/data/sites/default/files/NSDUHsubstateMethodology2014/NSDUHsubstateMethodology2014.htm#tabc1
    # 2014-2016
    # https://www.samhsa.gov/data/report/2014-2016-nsduh-substate-region-shapefile
    # https://www.samhsa.gov/data/sites/default/files/cbhsq-reports/NSDUHsubstateShapeFile2016/ShapeFile2016.zip
    # https://www.samhsa.gov/data/sites/default/files/cbhsq-reports/NSDUHsubstateMethodology2016/NSDUHsubstateMethodology2016.htm#tabc1
    # 2016-2018
    # https://www.samhsa.gov/data/report/2016-2018-nsduh-substate-region-shapefile
    # https://www.samhsa.gov/data/sites/default/files/reports/rpt29384/NSDUHsubstateShapeFile2018/NSDUHsubstateShapeFile2018.htm # definitions
    # https://www.samhsa.gov/data/sites/default/files/reports/rpt29384/NSDUHsubstateShapeFile2018/ShapeFile2018.zip
    # https://www.samhsa.gov/data/sites/default/files/reports/rpt29372/NSDUHsubstateMethodology2018_0/NSDUHsubstateMethodology2018.htm#tabc1
    # 2018-2020: data not available, see https://www.samhsa.gov/data/report/2018-2020-nsduh-substate-region-shapefile for details
    #
    # the number given is the prevalence rate, which is a percentage.  Need to multiply that by population
    # ex: 56,197 * (6.5/100) =  3,652 = count of TXNPILA for Kauai in 2016-2018

    """
    2008-2010
    Hawaii State: 1,069,970
    Hawaii Island: 146,741
    Honolulu: 753,234
    Kauai: 52,513
    Maui: 117,482

    2010-2012
    Hawaii State: 1,123,500
    Hawaii Island: 152,588
    Honolulu: 787,623
    Kauai: 55,634
    Maui: 127,655

    2012-2014
    Hawaii State: 1,145,942
    Hawaii Island: 154,193
    Honolulu: 806,141
    Kauai: 56,012
    Maui: 129,596

    2014-2016
    Hawaii State: 1,162,034
    Hawaii Island: 155,455
    Honolulu: 818,697
    Kauai: 56,499
    Maui: 131,383

    2016-2018
    Hawaii State: 1,160,319
    Hawaii Island: 154,486
    Honolulu: 818,512
    Kauai: 56,197
    Maui: 131,124
    """
    for year in shapefile_years:
        print(f"\n\nyear: {year}, url: {year['url']}\n")
        sf = shapefile.Reader(year['url'], verify = "False")
        print(f"sf.fields: {sf.fields}\n")
        rec = sf.records()
        #print(f"rec: {rec}\n") # really big result, don't bother with this unless outputting to a file
        #print(rec["Record #"])
        #print(rec[1].as_dict())
        for r in rec:
            rd = r.as_dict()
            if rd["ST_NAME"] == "Hawaii":
                print(f"\n\nrecord: {rd}\n")
                for var in shapefile_variables:
                    try:
                        #print(f"record1: {rd}\n")
                        #print(f"record: {rd['ST_NAME']}")
                        #print(f"record: {rd['ST_NAME']},{rd['SR_NAME']},tx: {rd['TXNPILA']},{rd['METAMYR']},{rd['PNRNMYR']},{rd['TXNOSPA']},{rd['TXNOSPI']},{rd['TXREC3']},{rd['UDPYILA']},{rd['UDPYILL']},{rd['UDPYPNR']}\n")
                        var_key = var['variable']
                        if var_key in rd:
                            #print(f"rd[var_key]: {rd[var_key]}, rd['SR_NAME']: {rd['SR_NAME']}, year[rd['SR_NAME']]: {year[rd['SR_NAME']]}")
                            var_val = rd[var_key]
                            population = float(year[rd['SR_NAME']])
                            count = round(var_val * population / 100)

                            #print(f"county: {rd['SR_NAME']}, var: {var_key}, val: {count}")
                            d = make_cell_dict(rd['SR_NAME'], "age range", var["description"], var["row_value"], count, year["start_year"], year["end_year"], year["year_range"], year["data_source"])
                            results.add(tuple(d.items()))

                            # if no match, change to lower case and try again
                        elif var_key.lower() in rd:
                            var_key = var_key.lower()
                            var_val = rd[var_key]
                            population = float(year[rd['SR_NAME']])
                            count = round(var_val * population / 100)
                            #print(f"county: {rd['SR_NAME']}, var: {var_key}, val: {count}")
                            d = make_cell_dict(rd['SR_NAME'], "age range", var["description"], var["row_value"], count, year["start_year"], year["end_year"], year["year_range"], year["data_source"])
                            results.add(tuple(d.items()))


                    except Error as e:
                        print("Error: ", e) 


# writes the results out to a csv file and returns the results as a DataFrame
def write_json_to_csv_file(results):
    print(f"write_json_to_csv_file: {csv_path}")
    # used a set to store all the results in so it would weed out duplicates.  
    # Sets can't store dicts, so made the dicts into tuples.  
    # Now convert tuples back to dicts to write it out.  
    # There's likely an easier way, but I wrote this as lists/dicts and discovered 
    # I need to weed out the duplicates later, so this was the quickest way to fix that.
    list = []
    for r in results:
        d = dict(r)
        list.append(d)
    df = pd.DataFrame(list)
    try:
        df.to_csv(csv_path, index=False)
    except Error as e:
        print("Error while writing", e) 
    return df

def make_db_connection():
    print("Running make_db_connection")
    try:
        connection_url = ""
        # create the db connection
        # this works for mssql, not mysql
        if is_mssql:
            connection_url = sa.engine.URL.create(
                drivername=db_driver,
                username=db_user,
                password=db_pwd,
                host=db_host,
                port=1433,
                database=db_name,
                query=query_d)
        # this works for mysql, not mssql
        else:
            connection_url = sa.engine.URL.create(
                drivername=db_driver,
                username=db_user,
                password=db_pwd,
                host=db_host,
                database=db_name)

        print(connection_url)
        engine = create_engine(connection_url)
        print(f"returning connection: {connection_url}")
        # used this to make sure connection was good, uncomment import text to work
        #with engine.connect() as conn:
        #    query = "select count(*) from dbo.TEDS_XWALK_AGE"
        #    result = conn.execute(text(query))
        return engine
    except Error as e:
        print("Error while connecting", e) 

# read from the csv file and write it to the database
def read_csv_write_to_db():
    print("Running read_csv_write_to_db")
    try:
        df = pd.read_csv(csv_path, sep=',', quotechar='\'', encoding='utf8') 
        write_data_frame_to_db(df)
    except Error as e:
        print("Error while connecting", e)
    # state:
    # county: 1096
    # total: 1800

# write the data we just got off the samhsa site directly to the database
def write_data_frame_to_db(df):
    print("Running write_data_frame_to_db")
    try:
        engine = make_db_connection()
        # add data to the table
        if is_mssql:
            df.to_sql(db_table, schema="dbo", con=engine, index=True, if_exists='fail')
        else:
            df.to_sql(db_table, con=engine, index=True, if_exists='fail')
    except Error as e:
        print("Error while connecting", e)
    # state:
    # county: 1096
    # total: 1800

# test method to show what we get back from a given url 
def print_url_contents(url):
    print(f"print_url_contents: {url}")
    resp = req.get(url)
    print(f"{resp}\n\n") # this just prints "<Response [code num]>"
    if resp.status_code == 200:
        jsondata = json.loads(resp.text)
        print(jsondata)


load_state_and_county_data()
#print_url_contents()
#read_shape_file()
#read_csv_write_to_db()

# -------------------------

# after moving control to row, dropping demographics, for throttled results, these still fail:
# 3, 11, 19, 27
# all of these use "ABODHER" as the column.  
""" 
# test method which gets data from the current url and calls parse__data
# make sure current url is the county one, not state one.

# currently does not work since I change the results object to a set and 
# added in the start_year, end_year, and year_range

def parse_current_url_data(isCounty, hasControl):
    print(f"parse_current_url_data: {url}")
    resp = req.get(url)
    print(f"{resp}\n\n") # this just prints "<Response [code num]>"
    if resp.status_code == 200:
        jsondata = json.loads(resp.text)
        results = []
        parse_data(isCounty, jsondata, results, hasControl)
        #write_json_to_csv_file(results)
    else:
        print(f"resp.reason: {resp.reason}")
        print(f"resp.text: {resp.text}") 
"""

"""
# this one fails because the column AMIYR_U doesn't exist for this year group
index: 5, control: STCTYCOD, row: CATAG2, column: AMIYR_U,  filter: STCTYCOD%3D74%2C75%2C76%2C77, weight: DASWT_8
https://rdas.samhsa.gov/api/surveys/NSDUH-2002-2017-RD16YR/crosstab/?control=STCTYCOD&row=CATAG2&column=AMIYR_U&filter=STCTYCOD%3D74%2C75%2C76%2C77&weight=DASWT_8&run_chisq=false&format=json
<Response [500]>

# this one fails because the column SMIYR_U doesn't exist for this year group
index: 6, control: STCTYCOD, row: CATAG2, column: SMIYR_U,  filter: STCTYCOD%3D74%2C75%2C76%2C77, weight: DASWT_8
https://rdas.samhsa.gov/api/surveys/NSDUH-2002-2017-RD16YR/crosstab/?control=STCTYCOD&row=CATAG2&column=SMIYR_U&filter=STCTYCOD%3D74%2C75%2C76%2C77&weight=DASWT_8&run_chisq=false&format=json
<Response [500]>
"""
