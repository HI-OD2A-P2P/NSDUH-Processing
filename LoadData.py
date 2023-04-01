# data is retrieved from <https://www.samhsa.gov/data/nsduh/state-reports>
# click on the year (ex: <https://www.samhsa.gov/data/nsduh/state-reports-NSDUH-2018>)
# then select NSDUH State Result Data Tables, <year> Estimated Totals By State.
# ex: https://www.samhsa.gov/data/report/2017-2018-nsduh-estimated-totals-state
# Then select the csv file from the download section, ex: "NSDUHsaeTotalsCSVs2018"
# https://www.samhsa.gov/data/report/2018-2019-nsduh-estimated-totals-state
# https://www.samhsa.gov/data/sites/default/files/reports/rpt32879/NSDUHsaeTotal2019/2019NSDUHsaeCountTabsCSVs.zip
# https://www.samhsa.gov/data/report/2017-2018-nsduh-state-prevalence-estimates
# https://www.samhsa.gov/data/sites/default/files/reports/rpt23259/NSDUHsaeTotals2018/NSDUHsaeTotalsCSVs2018.zip
# https://www.samhsa.gov/data/report/2016-2017-nsduh-state-prevalence-estimates
# https://www.samhsa.gov/data/sites/default/files/cbhsq-reports/NSDUHsaePercentsExcelCSVs2017/NSDUHsaeExcelCSVs2017.zip
# https://www.samhsa.gov/data/report/2015-2016-nsduh-estimated-totals-state
# https://www.samhsa.gov/data/sites/default/files/cbhsq-reports/NSDUHsaeTotal2016/NSDUHsaeTotals2016-CSVs.zip
# https://www.samhsa.gov/data/report/2014-2015-nsduh-estimated-totals-state
# https://www.samhsa.gov/data/sites/default/files/NSDUHsaeTotals2015A/NSDUHsaeTotalsCSVs-2015.zip
# https://www.samhsa.gov/data/report/2013-2014-nsduh-estimated-totals-state
# https://www.samhsa.gov/data/sites/default/files/NSDUHsaeTotalsCSVs2014%20%282%29/NSDUHsaeTotalsCSVs2014.zip

# there's no data pre-2013/2014.  Also, there's no data for the 2019-2020 year due to covid.
# notice that all the file names are different, so can't search on a pattern for that.
# but it seems that the .zip file is the only zip link on the page, and the urls seem to be standardized.

# note that different tabs have different numbers of columns, for example 
# "NSDUHsaeTotalsTab26-2019.csv" has 17 columns, but "NSDUHsaeTotalsTab27-2019.csv" has 11

# 1.) See if there are any zip files that we don't already have.
#     If yes, download any new zip files and expand them
# 2.) For each data set, we need to see if we've aleady processed it by checking data against the existing data in the database.
# 3.) Need to verify any new files match the expected format.  Be aware different tabs have different columns.
# 4.) If format is correct and data is new, add it to the database

# expected format in the file (will need to add filename, year, and substance columns to my database)
# Order
# State
# "12 or Older  Estimate"
# "12 or Older  95% CI (Lower)"
# "12 or Older  95% CI (Upper)"
# "12-17 Estimate"
# "12-17 95% CI (Lower)"
# "12-17 95% CI (Upper)"
# "18-25 Estimate"
# "18-25 95% CI (Lower)"
# "18-25 95% CI (Upper)"
# "26 or Older  Estimate"
# "26 or Older  95% CI (Lower)"
# "26 or Older  95% CI (Upper)"
# "18 or Older  Estimate"
# "18 or Older  95% CI (Lower)"
# "18 or Older  95% CI (Upper)"

import os
import pandas as pd
import pdb
import re
#import csv
#import plotly.express as px
#from dash import Input, Output, dcc, html, register_page, callback
#import dash_bootstrap_components as dbc
import mysql.connector as msql
from mysql.connector import Error
import sqlalchemy as sa
from sqlalchemy import create_engine
#from sqlalchemy import text
#from sqlalchemy import create_engine, types

# fields you will need to edit before running this
#dir = "<Your Directory Path Here>"
dir = "/Users/jgeis/Work/DOH/NSDUH_Processing/data_files/"
fileName = "NSDUH_2020_Tab.txt"
fullFilePath = dir + fileName
db_driver = "mysql+pymysql"
#db_driver = "mssql+pymssql"
#db_host = '<Your database host here>'
#db_host = "amhd-sql-data.database.usgovcloudapi.net"
db_host = "localhost"
#db_name = '<Your database name here>'
#db_name = "DOH_AMHD_NO_PII"
db_name = "doh"
#db_table = '<Your table name here>'
#db_table = 'dbo.TEDS_ALL_NUMERIC'
db_table = 'TEDS_A_Numeric'
#db_user = "<Your Username Here>"
#db_user = "JenniferGeis"
db_user = "jgeis"
#db_pwd = "<Your Password Here>"
#db_pwd = "doh_AMHD@2022!"
db_pwd = "ehuKanoa"

# don't change the encoding or this failes on NSDUHsaeTotals-Tab27-2018.csv
encoding_type = "unicode_escape"
#pd.set_option("display.max_rows", n)
pd.set_option("display.expand_frame_repr", True)
pd.set_option("display.max_colwidth", 1000)

# Some files are formatted oddly with newlines in the table's column headers, so need to strip out those newlines.
# this is a destructive process, the files are modified directly
def preprocess_file(file_path):
    #print(f"preprocess_file: {file_path}")
    with open(file_path,'r+', encoding=encoding_type) as f:
  
        # Reading the file data and store
        # it in a file variable
        file = f.read()
          
        # Replacing the pattern with the string
        # in the file data
        file = re.sub(r'"[^"]*(?:""[^"]*)*"', lambda m: m.group(0).replace("\n", ""), file)
        #print(f"file: {file}")

        # Setting the position to the top
        # of the page to insert data
        f.seek(0)
          
        # Writing replaced data in the file
        f.write(file)
  
        # Truncating the file size
        f.truncate()
        f.close()

def get_header_row(file_path):
    #print(f"get_header_row: {file_path}")

    file = open(file_path, 'r', encoding=encoding_type)
    count = 0
    while True:
        count += 1
        #print(f"count: {count}")
        line = file.readline()
        print(f"line: {line}")
        if (line.startswith("Order,State")):
            #print(f'Header: {count}')
            break

        # if line is empty, end of file is reached
        if not line:
            break
    file.close()
    #print(f'Returning: {count}')
    return (count - 1)

def read_file(file_path, combined_data):
    print(f"read_file: {file_path}")

    #preprocess_file(file_path)
    #header_index = get_header_row(file_path)
    # do NOT change file_path to fullFilePath here as they refer to different files
    #data = pd.read_csv(file_path, sep=",", header=header_index, encoding=encoding_type, quotechar='"')
    #data = pd.read_csv(file_path, sep="\t", header=header_index, encoding=encoding_type, quotechar='"', lineterminator='\r')
    data = pd.read_csv(file_path, sep="\t", encoding=encoding_type, quotechar='"', lineterminator='\r')

    #TODO: get subject line

    #data_us = data[data['State'] == "Total U.S."]
    #combined_data = pd.concat([combined_data, data_us])

    # strip out Hawaii as the end result was just too big
    data_hawaii = data[data['State'] == "Hawaii"]
    #print(data_hawaii)
    combined_data = pd.concat([combined_data, data_hawaii])
    #combined_data = pd.concat([combined_data, data])
    print(f"combined_data: {combined_data}")
    return combined_data
    #combined_data = combined_data.append(data)

# merges all .csv files found in the <dir> into one csv named <fileName>
def combine_csv_files():
    print("Running combine_csv_files")
    # Get a list of all files in the directory
    all_files = os.listdir(dir)
    #print(all_files)
    # Filter the list to only include .csv files
    csv_files = [file for file in all_files if file.endswith('.csv')]
    # Initialize an empty DataFrame to store the combined data
    combined_data = pd.DataFrame()
    # Loop through each .csv file and append its data to the combined data DataFrame
    for file in csv_files:
        #print(f'File: {file}')

        file_path = os.path.join(dir, file)
        #print(file_path)
        combined_data = read_file(file_path, combined_data)

    print(f"final_data: {combined_data}")
    # Write the combined data to a new .csv file
    combined_data.to_csv(fullFilePath, index=False)

# reads in <filename> and appends the data to <tableName>
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
        df = pd.read_csv(fullFilePath, sep=',', quotechar='\'', encoding='utf8') 
        
        # add data to the table
        df.to_sql(db_table, con=engine, index=False, if_exists='append')

        # used this to make sure connection was good, uncomment import text to work
        #with engine.connect() as conn:
        #    query = "select count(*) from dbo.TEDS_XWALK_AGE"
        #    result = conn.execute(text(query))

        # inserted 1,416,357 rows with 62 columns
    except Error as e:
        print("Error while connecting", e)

# uncomment one or both of these to make something happen 
#combine_csv_files()
#convert_to_db()

# method calls useful for debugging
#preprocess_file("/Users/jgeis/Work/DOH/NSDUH_Processing/data_files/NSDUHsaeTotalsCSVs2018/NSDUHsaeTotals-Tab27-2018.csv")
#get_header_row("/Users/jgeis/Work/DOH/NSDUH_Processing/data_files/NSDUHsaeTotalsCSVs2018/NSDUHsaeTotals-Tab27-2018.csv")
#read_file("/Users/jgeis/Work/DOH/NSDUH_Processing/data_files/NSDUHsaeTotalsCSVs2018/NSDUHsaeTotals-Tab31-2018.csv", pd.DataFrame())
read_file(fullFilePath, pd.DataFrame())



"""      # NSDUHsaeTotals-Tab16-2018.csv and others are formatted so strings have line breaks in them, need to ignore line breaks inside a string
        # do NOT change file_path to fullFilePath here as they refer to different files
        data = pd.read_csv(file_path, header=get_header_row(file_path), encoding=encoding_type)
        
        #data_us = data[data['State'] == "Total U.S."]
        #combined_data = pd.concat([combined_data, data_us])

        # strip out Hawaii as the end result was just too big
        data_hawaii = data[data['State'] == "Hawaii"]
        combined_data = pd.concat([combined_data, data_hawaii])
        #combined_data = pd.concat([combined_data, data])
        print(combined_data)
        #combined_data = combined_data.append(data) """

""" 
processFiles <- function() {
    files <- list.files(path = ".", pattern = NULL, all.files = FALSE,
            full.names = FALSE, recursive = FALSE,
            ignore.case = FALSE, include.dirs = FALSE, no.. = FALSE)
    len <- length(files)

    # loop through every file in the given directory
    # intentionally leaving off the zeroth place as it's the description file and not needed
    for (i in 2:len) {
        message("filename: ", files[i])
        #data <- read.csv(file = files[i], header = TRUE)

        # get the subject of this file
        # TODO: parse off the "Table n." thing at the beginning
        data <- read.csv(file = files[i], header=FALSE, stringsAsFactors = FALSE, check.names=FALSE)
        subject <- data[1, 1]
        message("subject: ", subject)

        # unfortunately, the dept of health and human services don't standardize the format of their files,
        # some have more "info rows" than others, so I can't count on skipping the same number of rows to
        # get to the actual data with each file.  To handle this, I read the file, skipping an additional
        # row with each iteration until I get to the row with the headers showing where the actual data starts.
        #data <-  c()
        foundFirstLine <- FALSE
        firstLine <- 0
        subjectLine <- ""

        # put the firstLine < 10 for now to avoid runaway if there's a bad file
        while(!foundFirstLine && firstLine < 10) {
            firstLine <- firstLine + 1
            # returns a data frame containing one list per column
            # may need na.strings=c(-1,'') argument if there are missing values
            # check.names=FALSE is needed, otherwise it adds periods where there are spaces in header names
            data <- read.csv(file = files[i], skip = firstLine, header = TRUE, stringsAsFactors = FALSE, check.names=FALSE)

            # get the value at the first column of the first row, if we've found the header row, it's value will be "Order"
            dataString <- data[1, 1]
            if (dataString == "Order") {
                foundFirstLine <- TRUE
                # need to read it one more time to get the right header row
                data <- read.csv(file = files[i], skip = firstLine + 1, header = TRUE, stringsAsFactors = FALSE, check.names=FALSE)
            }
        }

        columns <- colnames(data)
        #print(columns)

        # get the state column and find the index of "hawaii"
        # this gets everything in the given column, including column header
        states <- data[[2]]
        #print(states)
        for (state in 1:length(states)) {

            # TODO: get the US data along with Hawaii data.
            #if (states[state] == "Total U.S.") {
            #    message(states[state])
            #}

            if (states[state] == "Total U.S.") {
                getStateValues(data = data, index = state, columns = columns)
            }
            if (states[state] == "Hawaii") {
                getStateValues(data = data, index = state, columns = columns)
                # #print(states[state])
                # hawaiiIndex <- state
                # #message("hawaiiIndex: ", hawaiiIndex)
            }
        }
        #show(data)
        print("-------")
    }
} """