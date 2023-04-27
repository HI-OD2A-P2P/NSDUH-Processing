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

# when I installed #pip-system-certs, everything else broke with 
# SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED], 
# I uninstalled it and everything worked again
# unfortunately, this means I can't load shapefiles by url

# TODO:
# - fix csv to mssql issue
# - figure out how to incorporate shapefile data
# - add key to db table so duplicates aren't inserted
# - record all the year/variable combos that cause 500 errors and print them out at the end so we can verify and maybe change the variables


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

shp_file = "ShapeFile2018/SubstateRegionData161718.shp"
shp_path = f"{dir}{shp_file}"
json_file_in = "temp.json"
json_path = f"{dir}{json_file_in}"
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

state_years = [{"url_term":"NSDUH-2018-2019-RD02YR","year_range":"2018-2019","start_year":"2018","end_year":"2019", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2016-2017-RD02YR","year_range":"2016-2017","start_year":"2016","end_year":"2017", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2014-2015-RD02YR","year_range":"2014-2015","start_year":"2014","end_year":"2015", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2012-2013-RD02YR","year_range":"2012-2013","start_year":"2012","end_year":"2013", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2010-2011-RD02YR","year_range":"2010-2011","start_year":"2010","end_year":"2011", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2008-2009-RD02YR","year_range":"2008-2009","start_year":"2008","end_year":"2009", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2006-2007-RD02YR","year_range":"2006-2007","start_year":"2006","end_year":"2007", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2004-2005-RD02YR","year_range":"2004-2005","start_year":"2004","end_year":"2005", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"},
               {"url_term":"NSDUH-2002-2003-RD02YR","year_range":"2002-2003","start_year":"2002","end_year":"2003", "num_years":"2", "weight":"DASWT_1", "control":"STNAME","filter":"STNAME%3DHAWAII"}]

county_years = [{"url_term":"NSDUH-2010-2019-RD10YR","year_range":"2010-2019","start_year":"2010","end_year":"2019", "num_years":"10", "weight":"DASWT_4", "control":"STCTYCOD2","filter":"STCTYCOD2%3D85%2C83%2C84%2C82"},
                {"url_term":"NSDUH-2002-2017-RD16YR","year_range":"2002-2017","start_year":"2002","end_year":"2017", "num_years":"16", "weight":"DASWT_8", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"url_term":"NSDUH-2002-2016-RD15YR","year_range":"2002-2016","start_year":"2002","end_year":"2016", "num_years":"15", "weight":"DASWT_7", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"url_term":"NSDUH-2002-2015-RD14YR","year_range":"2002-2015","start_year":"2002","end_year":"2015", "num_years":"14", "weight":"DASWT_6", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"url_term":"NSDUH-2002-2013-RD12YR","year_range":"2002-2013","start_year":"2002","end_year":"2013", "num_years":"12", "weight":"DASWT_5", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"},
                {"url_term":"NSDUH-2002-2011-RD10YR","year_range":"2002-2011","start_year":"2002","end_year":"2011", "num_years":"10", "weight":"DASWT_4", "control":"STCTYCOD","filter":"STCTYCOD%3D74%2C75%2C76%2C77"}]

# turns out the older years use different values for the counties than the latest.
# fortunately, there was no overlap, so I just put them all in the same dict for simplicity
counties = {"74":"Hawaii County", "75":"Honolulu County","76":"Maui County","77":"County Not Specified","82":"Hawaii County", "83":"Honolulu County","84":"Kauai County","85":"Maui County"}

# main driver of everything.  Calls methods to get county data, 
# state data, write it out to a csv (as a backup), and then out to the database
def load_state_and_county_data():
    results = set()
    get_nsduh_data(True, results) # county
    get_nsduh_data(False, results) # state
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
                        parse_data(isCounty, json.loads(resp.text), results, True, start_year, end_year, year_range)
                    # if previous attempt didn't work and got a 400, it's likely throttled due to disclosure limitations,
                    # move control to row and drop the previous row which was a demographic thing
                    elif resp.status_code == 400 and resp.text == '{"errorCode":"DISCLOSURE_LIMITATION"}':
                        print(f"resp.reason: {resp.reason}")
                        print(f"resp.text: {resp.text}")
                        resp = get_url_data(index, base_url, "", ctl, column, fil, wt)
                        if resp.status_code == 200:
                            parse_data(isCounty, json.loads(resp.text), results, False, start_year, end_year, year_range)
                        elif resp.status_code == 400:
                            print(f"resp.reason: {resp.reason}")
                            print(f"resp.text: {resp.text}")
    except Exception as err:
        print(f"get_nsduh_data error: {err}")
    print(f"leaving get_nsduh_data")

# called by parse_data to generate a dict using all the given parameters.
# kinda brain-dead, but I wanted to make sure if one got changed, the both got changed, and this would assure that.
def make_cell_dict(county, row_type, col_type, row_value, col_value, count_unweighted, count_weighted, start_year, end_year, year_range):
    #return f"'Hawaii', {county}, {row_type}, {col_type}, {row_value}, {col_value}, {count_unweighted}, {count_weighted}"
    return dict({
        "state": "Hawaii",
        "county": county,
        "row_type": row_type,
        "col_type": col_type,
        "row_value": row_value,
        "col_value": col_value,
        "count_unweighted": count_unweighted,
        "count_weighted": count_weighted,
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
def parse_data(isCounty, jsondata, results, hasControl, start_year, end_year, year_range):
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
                if not hasControl:
                    county_value = ""
                    if isCounty:
                        county_value = counties[row_option]
                    d = make_cell_dict(county_value, "", col_dict["title"], "", col_dict[col_option], cell["count"]["unweighted"], cell["count"]["weighted"], start_year, end_year, year_range)
                    #print(f"dict: {d}")
                    results.add(tuple(d.items()))
                else:
                    county_value = ""
                    control_option = cell["control_option"]
                    if control_option:
                        county_value = ""
                        if isCounty:
                            county_value = counties[control_option]
                        d = make_cell_dict(county_value, row_dict["title"], col_dict["title"], row_dict[row_option], col_dict[col_option], cell["count"]["unweighted"], cell["count"]["weighted"], start_year, end_year, year_range)
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
        # this works for mysql, not mssql
        if is_mssql:
            connection_url = sa.engine.URL.create(
                drivername=db_driver,
                username=db_user,
                password=db_pwd,
                host=db_host,
                port=1433,
                database=db_name,
                query=query_d)
        else:
            connection_url = sa.engine.URL.create(
                drivername=db_driver,
                username=db_user,
                password=db_pwd,
                host=db_host,
                database=db_name)
        
        #driver = "ODBC+Driver+18+for+SQL+Server"
        #connection_url = f"{db_driver}://{db_user}:doh_AMHD%402022%21@{db_host}:1433/{db_name}?driver={driver}"


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
        """
        engine = make_db_connection()
        # get data from the csv file
        df = pd.read_csv(csv_path, sep=',', quotechar='\'', encoding='utf8') 

        # add data to the table
        if is_mssql:
            df.to_sql(db_table, schema="dbo", con=engine, index=False, if_exists='fail')
        else:
            df.to_sql(db_table, con=engine, index=False, if_exists='fail')
        """
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
            df.to_sql(db_table, schema="dbo", con=engine, index=False, if_exists='fail')
        else:
            df.to_sql(db_table, con=engine, index=False, if_exists='fail')

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


#load_state_and_county_data()
#print_url_contents()
#read_shape_file()
#write_data_frame_to_db()
read_csv_write_to_db()

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

"""
def convert_to_db():
    print("Running convert_to_db")
    try:
        # create the db connection
        connection_url = sa.engine.URL.create(
            drivername=db_driver,
            username=db_user,
            password=db_pwd,
            host=db_host,
            database=db_name)
        
        print(connection_url)
        engine = create_engine(connection_url)

        # read the data from the csv file, yes, I could have just made a bunch
        # of dicts and used convertersdict, but the python stuff to myssql is flaky as it is
        df = pd.read_csv(csv_path, sep=',', quotechar='\'', encoding='utf8') 
        
        # add data to the table
        df.to_sql(db_table, con=engine, index=False, if_exists='append')

        # used this to make sure connection was good, uncomment import text to work
        #with engine.connect() as conn:
        #    query = "select count(*) from dbo.TEDS_XWALK_AGE"
        #    result = conn.execute(text(query))
    except Error as e:
        print("Error while connecting", e)
"""
#combined_data.to_csv(fullFilePath, index=False)

"""
def read_csv_write_to_mssql_db():
    print("Running read_csv_write_to_db")
    # f"{db_driver}://{db_host}/{db_name}?driver=SQL Server Native Client 11.0?trusted_connection=yes?UID={db_user}?PWD={db_pwd}"
    # this worked, I think, at least it didn't give errors
    conn = pyodbc.connect("DRIVER={ODBC Driver 18 for SQL Server};"
                      f"SERVER=tcp:{db_host},1433;"
                      f"DATABASE={db_name}; UID={db_user}; PWD={db_pwd};")
    cursor = conn.cursor()

    #cursor.execute('''drop table if exists dbo.NSDUH_Jen''')

    # Create Table
    cursor.execute('''
        CREATE TABLE dbo.NSDUH_Jen (
                state VARCHAR(max) NULL,
                county VARCHAR(max) NULL,
                row_type VARCHAR(max) NULL,
                col_type VARCHAR(max) NULL,
                row_value VARCHAR(max) NULL,
                col_value VARCHAR(max) NULL,
                count_unweighted VARCHAR(max) NULL,
                count_weighted VARCHAR(max) NULL,
                start_year BIGINT NULL,
                end_year BIGINT NULL,
                year_range VARCHAR(max) NULL
        )
    ''')

    # Insert DataFrame to Table
    data = pd.read_csv(csv_path, sep=',', quotechar='\'', encoding='utf8') 
    df = pd.DataFrame(data)

    for row in df.itertuples():
        cursor.execute('''
                        INSERT INTO dbo.NSDUH_Jen (state, county, row_type, col_type, row_value, col_value, count_unweighted, count_weighted, start_year, end_year, year_range)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                        row.state, 
                        row.county, 
                        row.row_type, 
                        row.col_type, 
                        row.row_value, 
                        row.col_value, 
                        row.count_unweighted, 
                        row.count_weighted, 
                        row.start_year, 
                        row.end_year, 
                        row.year_range
                    )
    conn.commit()
"""
"""
    pyodbc.ProgrammingError: ('42000', '[42000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]
    The incoming tabular data stream (TDS) remote procedure call (RPC) protocol stream is incorrect. 
    Parameter 5 (""): The supplied value is not a valid instance of data type float. 
    Check the source data for invalid values. An example of an invalid value is data of numeric type 
    with scale greater than precision. (8023) (SQLExecDirectW)')
"""