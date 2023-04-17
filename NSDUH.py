
import os
import pandas as pd
import pdb
import re
import requests as req
import json
import geopandas as gpd
import csv


# TODO: 
# - need to get rid of duplicates, may need to change dict to write out a csv 
#   line instead and insert that into a set as a string.
# - add years/timeframe to the csv/dict/set
# - add in other years to state data
# - clean up!
#


# when I installed #pip-system-certs, everything else broke with 
# SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED], 
# I uninstalled it and everything worked again
# unfortunately, this means I can't load shapefiles by url

import shapefile


#import mysql.connector as msql
#from mysql.connector import Error
import sqlalchemy as sa
from sqlalchemy import create_engine
#from sqlalchemy import text
#from sqlalchemy import create_engine, types


# make repo here: https://github.com/orgs/HI-OD2A-P2P
# and check it in


# fields you will need to edit before running this

#-----------------------
# county level: Use RDAS (2010 to 2019) (first part of request)
#-----------------------
# to test any of the below urls via the user interface, 
# - edit the front part of the url to match <https://rdas.samhsa.gov/#/survey/>
# - strip off the format=json at the back

# This doesn't get data, this just gets all the possible rows, columns, and controls.
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR?format=json"
# Just an experiment of what happens if I add parameters to the above.  No data, just listing of options
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR?column=ABODMRJ&control=COUTYP2&results_received=true&row=CATAG2&run_chisq=false&weight=DASWT_4&format=json"
# same as precious w/ different searh params.  Got different results, but don't yet understand the significance and not sure if I need to.
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR?column=ABODMRJ&control=STCTYCOD2&results_received=false&row=CATAG2&run_chisq=false&weight=DASWT_4&format=json"

# adding crosstab to the url gets the actual data, the following urls are all crosstab
# won't load.  Gets: "{'errorCode': 'DISCLOSURE_LIMITATION'}"
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?row=CATAG2&column=ABODMRJ&control=STCTYCOD2&weight=DASWT_4&run_chisq=false&format=json"
# This data matches what you see on the site after a search.  Same as first crosstab url but had to remove control as it causes a no-result
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?row=CATAG2&column=ABODMRJ&weight=DASWT_4&run_chisq=false&format=json"
# from jason, note the STCTYCOD2 is a column, not a control
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?row=CIGYR&column=STCTYCOD2&weight=DASWT_4&run_chisq=false&filter=STCTYCOD2%3D85%2C83%2C84%2C82&format=json"

# USE THIS! one of the few county urls that works
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?control=STCTYCOD2&row=CATAG2&column=ABODMRJ&filter=STCTYCOD2%3D85%2C83%2C84%2C82&weight=DASWT_4&run_chisq=false&format=json"
# this is the url to see above results in the UI
#https://rdas.samhsa.gov/#/survey/NSDUH-2010-2019-RD10YR/crosstab/?column=ABODMRJ&control=STCTYCOD2&filter=STCTYCOD2%3D85%2C83%2C84%2C82&results_received=true&row=CATAG2&run_chisq=false&weight=DASWT_4

#-----------------------
# state level, RDAS 2018-2019 (last part of request)
#-----------------------
#url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2018-2019-RD02YR/crosstab/?column=ABODMRJ&control=STNAME&filter=STNAME%3DHAWAII&row=CATAG2&run_chisq=false&weight=DASWT_1&format=json"
#url = "https://rdas.samhsa.gov/#/survey/NSDUH-2016-2017-RD02YR/crosstab/?column=ABODMRJ&control=STNAME&filter=STNAME%3DHAWAII&row=CATAG2&run_chisq=false&weight=DASWT_1&format=json"



# this one is testing broken code
url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?control=STCTYCOD2&row=IRSEX&column=AMHTXND2&filter=STCTYCOD2%3D85%2C83%2C84%2C82&weight=DASWT_4&run_chisq=false&format=json"


dir = "/Users/jgeis/Work/DOH/NSDUH-Processing/data_files/"

shp_file = "ShapeFile2018/SubstateRegionData161718.shp"
shp_path = dir + shp_file
json_file_in = "temp.json"
json_path = dir + json_file_in
csv_file = "temp.csv"
csv_path = dir + csv_file


county_control = "STCTYCOD2" #  Collapsed State County Code = {15: HI, 001: Hawaii County, 15: HI, 003: Honolulu County, 15: HI 005: Kauai County, 15: HI 007: Maui County}
state_control = "STNAME"
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
county_filter = "STCTYCOD2%3D85%2C83%2C84%2C82"
state_filter = "STNAME%3DHAWAII"
state_years = ["NSDUH-2018-2019-RD02YR","NSDUH-2016-2017-RD02YR","NSDUH-2014-2015-RD02YR","NSDUH-2012-2013-RD02YR","NSDUH-2010-2011-RD02YR","NSDUH-2008-2009-RD02YR","NSDUH-2006-2007-RD02YR","NSDUH-2004-2005-RD02YR","NSDUH-2002-2003-RD02YR"]
#state_years = [{"start":"2018","end":"2019","length":"02","title":"2018-2019"}]
#county_years = ["NSDUH-2010-2019-RD10YR", ]

counties = {"82":"Hawaii County", "83":"Honolulu County","84":"Kauai County","85":"Maui County"}
state_weight = "DASWT_1"
county_weight = "DASWT_4"
county_base_url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/"
state_base_url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2018-2019-RD02YR/crosstab/"

# TODO: working here.  Change results from an array of dicts to a set of strings.
# edit the writing process to handle it.
# edit: makeCellDict, load_state_and_county_data, parse_data (append -> add), and write_json_to_csv_file


def load_state_and_county_data():
    results = []
    #get_all_nsduh_county(results) 
    #get_all_nsduh_state(results)
    get_nsduh_data(True, results)
    get_nsduh_data(False, results)
    write_json_to_csv_file(results)


def getUrlData(index, base_url, control, row, column, filter, weight):
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
# Once I have data, it then
# - calls a method which parses out the data we want
# - writes the data out to a csv file
def get_nsduh_data(isCounty, results):
    print(f"in get_all_nsduh_data")
    try:
        index = 0
        base_url = state_base_url
        ctl = state_control
        fil = state_filter
        wt = state_weight
        columns = state_columns
        if isCounty:
            base_url = county_base_url
            ctl = county_control
            fil = county_filter
            wt = county_weight      
            columns = county_columns
        for row in rows:
            for column in columns:
                index = index + 1
                resp = getUrlData(index, base_url, ctl, row, column, fil, wt)
                if resp.status_code == 200:
                    parse_data(isCounty, json.loads(resp.text), results, True)
                # if previous attempt didn't work and got a 400, it's likely throttled due to disclosure limitations,
                # move control to row and drop the previous row which was a demographic thing
                elif resp.status_code == 400 and resp.text == '{"errorCode":"DISCLOSURE_LIMITATION"}':
                    print(f"resp.reason: {resp.reason}")
                    print(f"resp.text: {resp.text}")
                    resp = getUrlData(index, base_url, "", ctl, column, fil, wt)
                    if resp.status_code == 200:
                        parse_data(isCounty, json.loads(resp.text), results, False)
                    elif resp.status_code == 400:
                        print(f"resp.reason: {resp.reason}")
                        print(f"resp.text: {resp.text}")
    except Exception as err:
        print(f"get_all_nsduh_state error: {err}")
        
    print(f"leaving get_all_nsduh_data")
    #write_json_to_csv_file(results)

def read_shape_file():
    print(f"in read_shape_file: {shp_path}")
    #df = gpd.read_file(shp_path, ignore_geometry=True)
    #print(list(df))
    #print(df)
    #print(df.head())
    #df_hawaii = df[df["ST_NAME"] == "Hawaii"]
    #print(df_hawaii)
    #print(df_hawaii.columns)
    #print(df_hawaii.head())

    sf = shapefile.Reader(shp_path)
    #shapefile_url = "https://www.samhsa.gov/data/sites/default/files/reports/rpt29384/NSDUHsubstateShapeFile2018/ShapeFile2018.zip"
    #sf = shapefile.Reader(shapefile_url, verify = "False")

    print(sf.fields)
    rec = sf.records()
    #print(rec)
    #print(rec["Record #"])
    print(rec[1].as_dict())
    for r in rec:
        rd = r.as_dict()
        if rd["ST_NAME"] == "Hawaii":
            #print(f"record: {rd}\n")
            #print(rd["TXNPILA"])
            print(f"record: {rd['ST_NAME']},{rd['SR_NAME']},{rd['TXNPILA']},{rd['METAMYR']},{rd['PNRNMYR']},{rd['TXNOSPA']},{rd['TXNOSPI']},{rd['TXREC3']},{rd['UDPYILA']},{rd['UDPYILL']},{rd['UDPYPNR']}\n")
            #print(f"record: {rd['ST_NAME']}")

# test method to show what we get back from the current url 
def print_url_contents():
    print(f"print_url_contents: {url}")
    resp = req.get(url)
    print(f"{resp}\n\n") # this just prints "<Response [code num]>"
    if resp.status_code == 200:
        jsondata = json.loads(resp.text)
        print(jsondata)

# test method which gets data from the current url and calls parse__data
# make sure current url is the county one, not state one.
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
        
def write_json_to_csv_file(results):
    # write out to csv
    keys = results[0].keys()
    #print(f"keys: {keys}")
    data_file = open(csv_path, 'w', newline='')
    csv_writer = csv.DictWriter(data_file, fieldnames = keys) 
    csv_writer.writeheader() 
    csv_writer.writerows(results) 
    data_file.close()


def makeCellDict(county, row_type, col_type, row_value, col_value, count_unweighted, count_weighted):
    #return f"'Hawaii', {county}, {row_type}, {col_type}, {row_value}, {col_value}, {count_unweighted}, {count_weighted}"
    return dict({
        "state": "Hawaii",
        "county": county,
        "row_type": row_type,
        "col_type": col_type,
        "row_value": row_value,
        "col_value": col_value,
        "count_unweighted": count_unweighted,
        "count_weighted": count_weighted
    })

# after moving control to row, dropping demographics, for throttled results, these still fail:
# 3, 11, 19, 27
# all of these use "ABODHER" as the column.  
# TODO: I need to make sure results array removes duplicate results because 
# we will get 4 duplicates for each time we drop the control
#
# Given jsondata, it loops through all the cells
# - parses the data for each cell, pulling out what we want
# - creates a json item for each set, formatting it so all entries will be consistent in spite of original json differences
# this method is _messy_ aas it has to account for the json being formatted differently 
# if the json was generated from a url with a control param or not.
# results - storage for all the data I'm parsing out.  Gets modified directly, so there's no return value from this
# hasControl - True if the jsondata was generated using a url that had a control parameter, 
#              False if the control was moved to the row param
def parse_data(isCounty, jsondata, results, hasControl):
    print(f"in parse_data: hasControl: {hasControl}")
    try:
        jsondata = jsondata["results"]
        # make dicts of the row and column options, we need this to translate the row/column numbers into human-readable text
        row_dict = make_dict(jsondata, "row")
        col_dict = make_dict(jsondata, "column")
        cells = jsondata["cells"]
        for cell in cells:
            row_option = cell["row_option"]
            col_option = cell["column_option"]
            #print(f"3: {row_option}, {col_option}, {hasControl}, {isCounty}")
            # don't bother with anything unless row and column values are set for that cell since we don't want totals
            if row_option and col_option:
                if not hasControl:
                    county_value = ""
                    if isCounty:
                        county_value = counties[row_option]
                    d = makeCellDict(county_value, "", col_dict["title"], "", col_dict[col_option], cell["count"]["unweighted"], cell["count"]["weighted"])
                    #print(f"dict: {d}")
                    results.append(d)
                else:
                    county_value = ""
                    control_option = cell["control_option"]
                    if control_option:
                        county_value = ""
                        if isCounty:
                            county_value = counties[control_option]
                        d = makeCellDict(county_value, row_dict["title"], col_dict["title"], row_dict[row_option], col_dict[col_option], cell["count"]["unweighted"], cell["count"]["weighted"])
                        #print(f"dict: {d}")
                        results.append(d) 
        print("success")
    except Exception as err:
        print(f"parse_data error: error.text: error: {err} ")
    print("leaving parse_data")

# used for getting keys and titles of rows and columns, works for both state and county data
def make_dict(jsondata, key):
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


load_state_and_county_data()
#print_url_contents()
#get_all_nsduh_state([])
#get_all_nsduh_county([])
#parse_current_url_data(True, True)
#parse_current_url_data(False, True)

#parse_current_url_county_data(False)
#parse_current_url_state_data(False)

#read_shape_file()

