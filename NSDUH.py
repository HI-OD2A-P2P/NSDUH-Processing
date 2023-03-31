
import os
import pandas as pd
import pdb
import re
import requests as req
import json
import geopandas as gpd
import csv

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
url = "https://rdas.samhsa.gov/api/surveys/NSDUH-2018-2019-RD02YR/crosstab/?column=ABODMRJ&control=STNAME&filter=STNAME%3DHAWAII&row=CATAG2&run_chisq=false&weight=DASWT_1&format=json"

dir = "/Users/jgeis/Work/DOH/NSDUH_Processing/data_files/"

shp_file = "ShapeFile2018/SubstateRegionData161718.shp"
shp_path = dir + shp_file
json_file_in = "temp.json"
json_path = dir + json_file_in
csv_file = "temp.csv"
csv_path = dir + csv_file

# STCTYCOD2 Collapsed State County Code = {15: HI, 001: Hawaii County, 15: HI, 003: Honolulu County, 15: HI 005: Kauai County, 15: HI 007: Maui County}
county_control = "STCTYCOD2"
state_control = "STNAME"
"""
CATAG2 Rc-Age Category Recode (3 Levels)
CATAG3 Rc-Age Category Recode (5 Levels)
EXPRACE Rc-Expanded Race Recode
IRSEX Gender - Imputation Revised
"""
rows = ["CATAG2","CATAG3","EXPRACE","IRSEX"]
"""
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
counties = {"82":"Hawaii County", "83":"Honolulu County","84":"Kauai County","85":"Maui County"}
state_weight = "DASWT_1"
county_weight = "DASWT_4"


def load_state_and_county_data():
    results = []
    get_all_nsduh_county(results) 
    get_all_nsduh_state(results)
    write_json_to_csv_file(results)

# this goes through all the combinations of control, rows, and columns. It then
# - It parses the data for each set of three, pulling out what we want
# - creates a json item for each set
# - writes the data out to a csv file
# 
# When I first ran this, only 29 and 31 brought back data. A few runs later, I see all these have come back:
# index: 1, control: STCTYCOD2, row: CATAG2, column: ABODMRJ
# index: 2, control: STCTYCOD2, row: CATAG2, column: ABODALC
# index: 10, control: STCTYCOD2, row: CATAG3, column: ABODALC
# index: 25, control: STCTYCOD2, row: IRSEX, column: ABODMRJ
# index: 26, control: STCTYCOD2, row: IRSEX, column: ABODALC
# index: 29, control: STCTYCOD2, row: IRSEX, column: AMIYR_U
# https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?control=STCTYCOD2&row=IRSEX&column=AMIYR_U&weight=DASWT_4&run_chisq=false&format=json
# index: 30, control: STCTYCOD2, row: IRSEX, column: SMIYR_U
# index: 31, control: STCTYCOD2, row: IRSEX, column: AMHTXRC3
# https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?control=STCTYCOD2&row=IRSEX&column=AMHTXRC3&weight=DASWT_4&run_chisq=false&format=json
# index: 32, control: STCTYCOD2, row: IRSEX, column: AMHTXND2
def get_all_nsduh_county(results):
    #https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?control=STCTYCOD2&row=CATAG2&column=ABODMRJ&weight=DASWT_4&run_chisq=false&format=json
    #substate_url = f"https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?control={control}&row={row}&column={column}&weight=DASWT_4&run_chisq=false&format=json"
    index = 1
    #results = []
    for row in rows:
        for column in county_columns:
            try:
                county_url = f"https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab/?control={county_control}&row={row}&column={column}&filter={county_filter}&weight={county_weight}&run_chisq=false&format=json"
                print(f"\n\nindex: {index}, control: {county_control}, row: {row}, column: {column},  filter: {county_filter}, weight: {county_weight}")
                print(f"{county_url}")
                resp = req.get(county_url)
                print(f"{resp}\n\n") # this just prints "<Response [code num]>"
                if resp.status_code == 200:
                    jsondata = json.loads(resp.text)
                    parse_county_data(jsondata, results)
                    #print(jsondata)
            except Exception as err:
                print(err)
            index = index + 1
    #print(results)
    #write_json_to_csv_file(results)

def get_all_nsduh_state(results):
    print(f"in load_all_nsduh_state")
    index = 1
    #results = []
    for row in rows:
        for column in state_columns:
            try:
                state_url = f"https://rdas.samhsa.gov/api/surveys/NSDUH-2018-2019-RD02YR/crosstab/?control={state_control}&row={row}&column={column}&filter={state_filter}&weight={state_weight}&run_chisq=false&format=json"
                print(f"\n\nindex: {index}, control: {state_control}, row: {row}, column: {column}, filter: {state_filter}, weight: {state_weight}")
                print(f"{state_url}")
                resp = req.get(state_url)
                print(f"{resp}\n\n") # this just prints "<Response [code num]>"
                if resp.status_code == 200:
                    jsondata = json.loads(resp.text)
                    parse_state_data(jsondata, results)
                    #print(jsondata)
            except Exception as err:
                print(err)
            index = index + 1
    #print(results)
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

# test method which gets data from the current url and calls parse_county_data
# make sure current url is the county one, not state one.
def parse_current_url_county_data():
    print(f"parse_current_url_county_data: {url}")
    resp = req.get(url)
    print(f"{resp}\n\n") # this just prints "<Response [code num]>"
    if resp.status_code == 200:
        jsondata = json.loads(resp.text)
        results = []
        parse_county_data(jsondata, results)
        #write_json_to_csv_file(results)

def write_json_to_csv_file(results):
    # write out to csv
    keys = results[0].keys()
    #print(f"keys: {keys}")
    data_file = open(csv_path, 'w', newline='')
    csv_writer = csv.DictWriter(data_file, fieldnames = keys) 
    csv_writer.writeheader() 
    csv_writer.writerows(results) 
    data_file.close()

def parse_county_data(jsondata, results):
    jsondata = jsondata["results"]

    # I don't need to look up control as I already have those
    row_dict = make_dict(jsondata, "row")
    col_dict = make_dict(jsondata, "column")
    cells = jsondata["cells"]
    for cell in cells:
        control_option = cell["control_option"]
        row_option = cell["row_option"]
        col_option = cell["column_option"]
        
        # don't bother with anything unless control, row, and column are set, don't need totals
        if control_option and row_option and col_option:
            d = dict({
                "state": "Hawaii",
                "county": counties[control_option],
                "row_type": row_dict["title"],
                "col_type": col_dict["title"],
                "row_value": row_dict[row_option],
                "col_value": col_dict[col_option],
                "count_unweighted": cell["count"]["unweighted"],
                "count_weighted": cell["count"]["weighted"]
                })
            results.append(d)
    #print (results)

def parse_state_data(jsondata, results):
    jsondata = jsondata["results"]

    # I don't need to look up control as I already have those
    row_dict = make_dict(jsondata, "row")
    col_dict = make_dict(jsondata, "column")
    cells = jsondata["cells"]
    for cell in cells:
        control_option = cell["control_option"]
        row_option = cell["row_option"]
        col_option = cell["column_option"]
        
        # don't bother with anything unless control, row, and column are set, don't need totals
        if control_option and row_option and col_option:
            d = dict({
                "state": "Hawaii",
                "county": "",
                "row_type": row_dict["title"],
                "col_type": col_dict["title"],
                "row_value": row_dict[row_option],
                "col_value": col_dict[col_option],
                "count_unweighted": cell["count"]["unweighted"],
                "count_weighted": cell["count"]["weighted"]
                })
            results.append(d)


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


#load_state_and_county_data()
#print_url_contents()
#get_all_nsduh_state([])
get_all_nsduh_county([])
#parse_current_url_county_data()
#read_shape_file()



"""
    CREATE TABLE `nsduh` (
    `id` int NOT NULL AUTO_INCREMENT,
    `age_group_3` varchar(20) NOT NULL,
    `age_group_5` varchar(20) NOT NULL,
    `race` varchar(255) NOT NULL,
    `gender` varchar(10) NOT NULL,
    `year` int NOT NULL,
    `county` varchar(20) NOT NULL,
    `substance` varchar(255) NOT NULL,
    `total_weighted_count_all` int NOT NULL,
    `total_weighted_count_no_or_unknown` int NOT NULL,
    `total_weighted_count_yes` int NOT NULL,
    `total_count_se_all` int NOT NULL,
    `total_count_se_no_or_unknown` int NOT NULL,
    `total_count_se_yes` int NOT NULL
    PRIMARY KEY (`id`)
    ) ENGINE=InnoDB;
"""

"""
Due to the number of cells in this analysis (# of Row Categories  * # of Column Categories * # of Control Categories), 
there is an increased likelihood that your results will be suppressed. Suppression occurs in RDAS to protect a respondents 
identity from disclosure, due to the granularity of the results from the data analysis.

Please try using the recoding feature to combine categories in order to reduce risk of disclosure. If your research 
requires more detailed results, you may want to consider a  Federal Statistical Research Data Center (RDC).

----

The results of this crosstab have been suppressed. Suppression occurs in RDAS to protect a respondent’s identity 
from disclosure, due to the granularity of the results from the data analysis.

Please try using the recoding feature to combine categories in order to reduce risk of disclosure. 
If your research requires more detailed results, you may want to consider a  Federal Statistical Research Data Center (RDC).
"""

"""
def dict_test():
    df = counties
    # df.loc[df['favorite_color'] == 'yellow']
    print(counties)
    print(df["82"]) # returns "Hawaii County"

def convert_county_json_to_csv():
    resp = req.get(url)
    print(f"resp: {resp}\n\n")

    jsondata = json.loads(resp.text)
    print(jsondata)
    print("\n")
    jsondata = jsondata["results"]
    #print(jsondata)
    keys = jsondata.keys()
    print(f"results keys: {keys}\n")
    # dict_keys(['error', 'cells', 'row', 'chisq', 'ddf', 'column', 'control', 'weight'])

    # I don't need to look up control as I already have those
    row_dict = make_dict(jsondata, "row")
    col_dict = make_dict(jsondata, "column")
    results = []

    cells = jsondata["cells"]
    for cell in cells:
        control_option = cell["control_option"]
        row_option = cell["row_option"]
        col_option = cell["column_option"]
        
        # don't bother with anything unless control, row, and column are set, don't need totals
        if control_option and row_option and col_option:
            d = dict({
                "county": counties[control_option],
                "row_type": row_dict["title"],
                "col_type": col_dict["title"],
                "row_value": row_dict[row_option],
                "col_value": col_dict[col_option],
                "count_unweighted": cell["count"]["unweighted"],
                "count_weighted": cell["count"]["weighted"]
                })
            results.append(d)
    print (results)
"""

"""
# 'control_option': any of 82, 83, 84, or 85 = Hawaii state counties, this are variables I passed in via the url
# 82 = Hawaii County, 83 = Honolulu County, 84 = Kauai County, 85 = Maui County
{
    'results': {
        'error': False,
        'cells': [{
            'count': {
                'unweighted': None,
                'weighted': 165000.0,
                'standard_error': 10000.0
            },
            'row': {
                'percent': 1.0,
                'standard_error': 0.0,
                'confidence_interval': [None, None]
            },
            'column': {
                'percent': 1.0,
                'standard_error': 0.0,
                'confidence_interval': [None, None]
            },
            'total': {
                'percent': 1.0,
                'standard_error': 0.0,
                'confidence_interval': [None, None]
            },
            'row_option': '',
            'column_option': '',
            'control_option': '82'
        },{}
        ]
    }
}
# the row option values come from here:
'row': 
{
    'id': 40673,
    'key': 'CATAG2',
    'title': 'RC-AGE CATEGORY RECODE (3 LEVELS)',
    'question': None,
    'options': [{
        'key': '1',
        'title': '1 - 12-17 Years Old',
        'missing': False,
        'nonresponse': False,
        'display_order': 0
    }, {
        'key': '2',
        'title': '2 - 18-25 Years Old',
        'missing': False,
        'nonresponse': False,
        'display_order': 1
    }, {
        'key': '3',
        'title': '3 - 26 or Older',
        'missing': False,
        'nonresponse': False,
        'display_order': 2
    }],
    'group_id': 1127,
    'weight_order': None,
    'default_weight': None,
    'stratum': None,
    'cluster': None
},
# the column option values come from here:
'column': 
{
    'id': 39873,
    'key': 'ABODMRJ',
    'title': 'RC-MARIJUANA DEPENDENCE OR ABUSE - PAST YEAR',
    'question': None,
    'options': [{
        'key': '0',
        'title': '0 - No/Unknown',
        'missing': False,
        'nonresponse': False,
        'display_order': 0
    }, {
        'key': '1',
        'title': '1 - Yes',
        'missing': False,
        'nonresponse': False,
        'display_order': 1
    }],
    'group_id': 1103,
    'weight_order': None,
    'default_weight': None,
    'stratum': None,
    'cluster': None
},
# if row, column, or control is not defined, it's a grand total for all categories
"""

"""
    #for row in df:
    #    print(row.values)
    #print(df["ST_NAME"])
    #print(df.dtypes)
    #print(df.values)
    #ndf = df.to_numpy()
    #print(ndf)
    #print(df.index)
    #print(df.columns)
    #items = df.items()
    #for item in items:
    #    print(item)
    #print(df.index)
    #print(df.keys())
    #])
    #for d in df:
    #    print(d)
    #print(df)
    #df = df.set_geometry('geometry')
    #print(df.shape)
"""



"""
Table definition: need to make a csv that looks like this, omitting 'id.'
CREATE TABLE `nsduh` (
  `id` int NOT NULL AUTO_INCREMENT,
  `age_group_3` varchar(20) NOT NULL,
  `age_group_5` varchar(20) NOT NULL,
  `race` varchar(255) NOT NULL,
  `gender` varchar(10) NOT NULL,
  `year` int NOT NULL,
  `county` varchar(20) NOT NULL,
  `substance` varchar(255) NOT NULL,
  `total_weighted_count_all` int NOT NULL,
  `total_weighted_count_no_or_unknown` int NOT NULL,
  `total_weighted_count_yes` int NOT NULL,
  `total_count_se_all` int NOT NULL,
  `total_count_se_no_or_unknown` int NOT NULL,
  `total_count_se_yes` int NOT NULL
  PRIMARY KEY (`id`)
) ENGINE=InnoDB;

json
{
  "age_group_3": ,
  "age_group_5": ,
  "race": ,
  "gender":,
  "year":,
  "county":,
  "substance":",
  "total_weighted_count_all":,
  "total_weighted_count_no_or_unknown":,
  "total_weighted_count_yes":,
  "total_count_se_all":,
  "total_count_se_no_or_unknown":,
  "total_count_se_yes":
}

Going to need to build these up bit by bit with each pass through the control (county), rows, and columns

"""

"""    
    csv_writer = csv.writer(data_file)
    header = jsondata.keys()
    csv_writer.writerow(header)
    csv_writer.writerow(jsondata.values())
    data_file.close()


    resp = req.get(url)
    jsondata = json.loads(resp.text)
    subset = jsondata["variables"]
    print(subset[0])
    data_file = open(csv_path, 'w', newline='')
    csv_writer = csv.writer(data_file)
    header = jsondata.keys()
    csv_writer.writerow(header)
    csv_writer.writerow(jsondata.values())
    data_file.close()    
"""




"""
{
    "key": "95",
    "title": "15:HI, 1:Hawaii Island",
    "missing": false,
    "nonresponse": false,
    "display_order": 94
}, {
    "key": "96",
    "title": "15:HI, 2:Honolulu",
    "missing": false,
    "nonresponse": false,
    "display_order": 95
}, {
    "key": "97",
    "title": "15:HI, 3:Kauai",
    "missing": false,
    "nonresponse": false,
    "display_order": 96
}, {
    "key": "98",
    "title": "15:HI, 4:Maui",
    "missing": false,
    "nonresponse": false,
    "display_order": 97
}], "survey_year": "2010-2019", "study": "NSDUH", "icpsr": "00000", "is_public": false, "slug": "NSDUH-2010-2019-RD10YR", "dataset_id": "RD10YR", "is_census": false, "study_group": "NSDUH RDAS (Substate)", "ddf": null}





 {15: HI, 001: Hawaii County, 15: HI, 003: Honolulu County, 15: HI 005: Kauai County, 15: HI 007: Maui County}

 
 {
        "id": 40810,
        "key": "STCTYCOD2",
        "title": "COLLAPSED STATE COUNTY CODE",
        "question": null,
        "options": [{
            "key": "82",
            "title": "15: HI, 001: Hawaii County",
            "missing": false,
            "nonresponse": false,
            "display_order": 81
        }, {
            "key": "83",
            "title": "15: HI, 003: Honolulu County",
            "missing": false,
            "nonresponse": false,
            "display_order": 82
        }, {
            "key": "84",
            "title": "15: HI, 007: Kauai County",
            "missing": false,
            "nonresponse": false,
            "display_order": 83
        }, {
            "key": "85",
            "title": "15: HI, 009: Maui County",
            "missing": false,
            "nonresponse": false,
            "display_order": 84
        }

        {"key":"95","title":"15:HI, 1:Hawaii Island","missing":false,"nonresponse":false,"display_order":94},
        {"key":"96","title":"15:HI, 2:Honolulu","missing":false,"nonresponse":false,"display_order":95},
        {"key":"97","title":"15:HI, 3:Kauai","missing":false,"nonresponse":false,"display_order":96},
        {"key":"98","title":"15:HI, 4:Maui","missing":false,"nonresponse":false,"display_order":97}

"""

"""
def run():
    print(f"run: {url}")
    json_in = req.get(url)
    print(f"{json_in}\n\n") # this just prints "<Response [code num]>"
    if json_in.status_code == 200:
        parsed_json = json.loads(json_in.text)

    index = 0
    groups = parsed_json["groups"]
    for group in groups:
        index = index + 1
        print(f'groups[{index}]: {group}')
    # see groups printout below

    index = 0
    groups = parsed_json["variables"]
    for group in groups:
        index = index + 1
        print(f'variables[{index}]: {group}')
    # each item is a list, need to explore this one more
    """
    
    #variables = parsed_json["variables"][0]
    #print(variables.keys())
    ## dict_keys(['id', 'key', 'title', 'question', 'options', 'group_id', 'weight_order', 'default_weight', 'stratum', 'cluster'])
    ## {'id': 40817, 'key': 'VEREP', 'title': 'ANALYSIS REPLICATE', 'question': None, 'options': [], 'group_id': 1142, 'weight_order': None, 'default_weight': None, 'stratum': None, 'cluster': 0}

    #for key in parsed_json.keys():
        #print(f'key: {key}')
        #index = 0
        #key_type = type(parsed_json[key])
        #print(f'key_type: {key_type}')
        #if key_type == str:
        #    print(f'parsed_json[key]: {parsed_json[key]}')
        #    for set in parsed_json[key]:
        #        index = index + 1
        #        print(f'group[{index}]: {set}')

    # print all the groups
    #index = 0
    # groups = parsed_json["groups"]
    #for group in groups:
        #index = index + 1
        #print(f'group[{index}]: {group}')

    #print(groups.keys())
    #print(groups)
    #CATAG2 = groups["CATAG2"]
    #print(CATAG2)


"""     
    groups = parsed_json["id"]
    for group in groups:
        index = index + 1
        print(f'id[{index}]: {group}') 
    # key is an int, no good

    index = 0
    groups = parsed_json["name"]
    for group in groups:
        index = index + 1
        print(f'name[{index}]: {group}')
    # prints out a string one char at a time
     
    index = 0
    groups = parsed_json["survey_year"]
    for group in groups:
        index = index + 1
        print(f'survey_year[{index}]: {group}')
    # prints out "2010-2019" one char at a time
    
    index = 0
    groups = parsed_json["study"]
    for group in groups:
        index = index + 1
        print(f'study[{index}]: {group}')
    # prints out "NSDUH" one char at a time
    
    index = 0
    groups = parsed_json["icpsr"]
    for group in groups:
        index = index + 1
        print(f'icpsr[{index}]: {group}')
    # prints out 5 zeros one at a time

    index = 0
    groups = parsed_json["is_public"]
    for group in groups:
        index = index + 1
        print(f'is_public[{index}]: {group}')
    # TypeError: 'bool' object is not iterable

    index = 0
    groups = parsed_json["slug"]
    for group in groups:
        index = index + 1
        print(f'slug[{index}]: {group}')
    # prints out "NSDUH" and years one char at a time
    
    index = 0
    groups = parsed_json["dataset_id"]
    for group in groups:
        index = index + 1
        print(f'dataset_id[{index}]: {group}')
    # prints out "RD10YR" one char at a time

    index = 0
    groups = parsed_json["is_census"]
    for group in groups:
        index = index + 1
        print(f'is_census[{index}]: {group}')
    # TypeError: 'bool' object is not iterable
    
    index = 0
    groups = parsed_json["study_group"]
    for group in groups:
        index = index + 1
        print(f'study_group[{index}]: {group}')
    # prints out "NSDUH RDAS (Substate)" one char at a time
    
    index = 0
    groups = parsed_json["ddf"]
    for group in groups:
        index = index + 1
        print(f'ddf[{index}]: {group}')
    # TypeError: 'NoneType' object is not iterable
"""

""" 
#all surveys in API
API_Query <- (url)
print(API_Query)
gt <- GET(API_Query)
con <- content(gt)

variables <- sapply(con[["variables"]], '[' , c(2))
print(variables)

variables_group <- sapply(con[["variables"]], '[' , c(6))
#print(variables_group)

names(variables) <- variables_group
print(names(variables))

groups <- sapply(con[["groups"]], '[' , c(1))
#print(groups)

group_names <- sapply(con[["groups"]], '[', c(2))
#print(group_names)

names(groups) <- group_names
print(names(groups))

API_Query <- ('https://rdas.samhsa.gov/api/surveys/NSDUH-2010-2019-RD10YR/crosstab.csv/?row=CIGYR&column=STCTYCOD2&weight=DASWT_4&run_chisq=false&filter=STCTYCOD2%3D85%2C83%2C84%2C82')
print(API_Query)
gt <- GET(API_Query)
con <- content(gt)
df <- as.data.frame(con) # converting from tibble to df
"""