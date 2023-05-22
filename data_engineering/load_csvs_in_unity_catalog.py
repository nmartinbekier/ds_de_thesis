# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This notebook uploads csvs to Delta Tables in Unity Catalog.
# MAGIC
# MAGIC It requires:
# MAGIC * The folder path where it will recursively process csvs from
# MAGIC * The catalog and database where the tables will be uploaded
# MAGIC
# MAGIC Notice:
# MAGIC * The tables will have the same name as the files, with a `bronze_` prefix, and replacing dashes and parenthesis for underscores
# MAGIC * If there are failed records, they will be placed in the specified badRecordPath for the table
# MAGIC * The tables will include as metadata the original filepath, delimiter used, badRecordPath
# MAGIC * Each csv is initially loaded as a raw text file to identify its delimiter, and to check if it has carriage returns (^M). If it does, it removes them from the header and creates an adjusted temporary csv that will be used to create the table from.
# MAGIC
# MAGIC To-do:
# MAGIC * Create a table registering the details of each transaction, including:
# MAGIC   * Timestamp with the start of the process
# MAGIC   * Filepath of csv source
# MAGIC   * Filename of csv source
# MAGIC   * Name of the table, database and catalog (in different fields)
# MAGIC   * Success of the transaction (bool)
# MAGIC   * Number of records written
# MAGIC   * Number of bad records
# MAGIC   * badRecordPath

# COMMAND ----------

# MAGIC %md
# MAGIC # Global definitions

# COMMAND ----------

# DBTITLE 0,Global definitions
from pyspark.sql.functions import *
import time
import re
import ntpath
data_bucket = "s3a://trase-storage"
catalog_bucket = "s3a://trase-catalog"

# COMMAND ----------

# MAGIC %md
# MAGIC # Main functions

# COMMAND ----------

# MAGIC %md
# MAGIC ## General supporting functions

# COMMAND ----------

def find_delimiter(header_text):
    """Given a header, returns the first ocurrence of a delimiter character from |;,\t """
    delimiters = [";",",","\t","|"]
    del_index = next((i for i,c in enumerate(header_text) if c in delimiters))
    return(header_text[del_index])

def get_non_alphanumeric_chars(string):
    non_alphanumeric_chars = set(re.findall(r'[^a-zA-Z0-9]', string))
    allowed_subet = set([";",",","\t","|"," ","_","\r","\n"])
    return list(non_alphanumeric_chars - allowed_subet)

def create_temp_csv(new_content, file_name, data_bucket):
    """Creates a temporary csv based on the string provided and places it in s3:{data_bucket}/tmp/file_name_TSTIMESTAMP.csv"""
    current_time = int(time.time())
    filepath = f"{data_bucket}/tmp/{file_name}_TS{current_time}.csv"
    dbutils.fs.put(filepath, new_content)
    return(filepath)

def dir_contents(path):
    """Returns a list of filenames and a list of directories within file_path"""
    records_list = dbutils.fs.ls(f"{path}")
    file_list = []
    dir_list = []
    for record in records_list:
        if (record.isFile() == True): file_list.append(record.name)
        elif (record.isDir() == True): dir_list.append(record.name)
    return(file_list, dir_list)

def list_files_recursively(dir_path, files_list):
    """Recursively list files of a directory and return them in provided 'files_list'""" 
    for record in dbutils.fs.ls(dir_path):
        file_path = f"{dir_path}/{record.name}"
        if (record.isDir() == True):
            list_files_recursively(file_path[:-1], files_list)
        else:
            if (record.size > 50000000):
                print(f"\n***\nWarning: {file_path} is bigger than 50M, skipping it. \
                    Header replacement has to be done manually for the moment\n***")
                break;
            elif (record.name.lower().find(".csv") == -1):
                print(f"***Info: {file_path} is not a csv file. Skipping it***")
            else:
                files_list.append(file_path)

def generate_table_name(csv_file_name):
    """Returns a table name with a 'bronze_' prefix, and replacing special characters"""
    table_name = re.sub('[^0-9a-zA-Z]+', '_', csv_file_name).lower().removesuffix("_csv")
    return("bronze_"+table_name)

def create_catalog(catalog_name, catalog_bucket, comment=""):
    location = f"{catalog_bucket}/{catalog_name}"
    comment = f"{catalog_name} catalog, hosted in {location}. {comment}"
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name} " \
    f" MANAGED LOCATION '{location}' " \
    f" COMMENT '{comment}' ")
    return True

def create_db(db_name, catalog_name, catalog_bucket, comment=""):
    location = f"{catalog_bucket}/{catalog_name}/{db_name}"
    comment = f"{db_name} database, hosted in {location}"
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name} " \
    f" MANAGED LOCATION '{location}' " \
    f" COMMENT '{comment}' ")
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-process csv

# COMMAND ----------

def pre_process_csv(csv_filepath):
    """Load the first part of a csv file to identify the delimiter and if there's a need to adjust the header
    Returns a tuple with the delimiter and a path to a temporary csv in case it needs to adjust it.
    In particular, if there are carriage returns (^M) within the column names in the header it removes them,
    as they would cause the header to be partially processed and generate inconsistencies
    """
    filename = ntpath.basename(csv_filepath)
    print(f"\nTruncating the read of '{filename}' to just review its header")
    content = dbutils.fs.head(csv_filepath, 2000) # Read the first 2Kb of the csv to review its header
    return_pos = content.find("\n") # Needed if there's a header replacement
    header = content.split("\n")[0] # Takes header as the content up to a newline
    delimiter = find_delimiter(header)

    # If the header has carriage return or unalowed character, replace it for a '_' 
    # and write/load csv from a temp file
    new_csv_filepath = ""
    find_pos = header.find("\r")
    non_alphn = get_non_alphanumeric_chars(header)
    # If there is a carriage return or special character in the middle of the header
    if ((find_pos > 0) and (find_pos < (len(header)-2)) or (len(non_alphn)>0) ):
        # Replaces carriage returns and special characters from the header
        header = header.replace("\r", "_")
        for char in non_alphn:
            header = header.replace(char, "_")
        
        # Read the whole csv as plain text, and replace the header with an adjusted version
        df = spark.read.text(csv_filepath, wholetext=True)
        content = df.first()['value']
        new_content = header.replace(" ", "_") + content[return_pos:].replace("\r", " ")
        new_csv_filename = ntpath.basename(csv_filepath).removesuffix(".csv") # Get the filename

        # Generate the temporary csv file
        new_csv_filepath = create_temp_csv(new_content, new_csv_filename, data_bucket) 
        
    return delimiter, new_csv_filepath

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create delta table from csv function

# COMMAND ----------

# DBTITLE 0,Create delta table from csv function
def create_table_from_csv(source_filepath, catalog, db, 
        tmp_csv_filepath="",
        table_name="",
        inferschema="true",
        header="true",
        delimiter=";",
        encoding="UTF-8",
        ignoreLeadingWhiteSpace="true",
        ignoreTrailingWhiteSpace="true",
        multiLine="true",
        nullvalue="NA",
        badRecordsPath=f"{data_bucket}/tmp/badRecordsPath",
        write_mode = "overwrite",
        reader_version = 1,
        writer_version = 4,
        description = ""):
    """Create a delta table from a csv file. Includes common csv and delta table options"""

    # If it needs to read from a temporary pre-processed csv, it specifies it as input    
    if (tmp_csv_filepath != ""):
        input_file_path = tmp_csv_filepath
    else: 
        input_file_path = source_filepath

    df = spark.read.format("csv") \
      .option("inferSchema", inferschema) \
      .option("sep", delimiter) \
      .option("header", header) \
      .option("encoding", encoding) \
      .option("ignoreLeadingWhiteSpace", ignoreLeadingWhiteSpace) \
      .option("ignoreTrailingWhiteSpace", ignoreTrailingWhiteSpace) \
      .option("multiLine", multiLine) \
      .option("nullvalue", nullvalue) \
      .option("badRecordsPath", badRecordsPath) \
      .load(input_file_path)

    # Adding a description for the table
    creation_note = "Table schema and data created automatically from " + source_filepath
    if (description != ""): description = creation_note + " . " + description
    else: description = creation_note
    
    # Replace white spaces in column names
    #df = df.select([col(c).alias(re.sub('[^0-9a-zA-Z]+', '_', c)) for c in df.columns])
    df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])

    # If the table name is not specified, created based on the csv filename
    if (table_name == ""):
        file_name = ntpath.basename(source_filepath)
        # adds prefix and removes .csv suffix and special characters
        table_name = generate_table_name(file_name)

    # When writing, the option key-value pairs will be saved as metadata
    table_path = f"{catalog}.{db}.{table_name}"
    df.write.format("delta") \
        .mode(write_mode) \
        .option("delta.minReaderVersion", reader_version) \
        .option("delta.minWriterVersion", writer_version) \
        .saveAsTable(table_path)

    spark.sql(f"COMMENT ON TABLE {table_path} IS \"{description}\"")
    spark.sql(f"ALTER TABLE {table_path} SET TBLPROPERTIES ('source_csv_file'='{source_filepath}', \
        'write_mode'='{write_mode}', 'source_delimiter'='{delimiter}')")
    print (f"{table_path} table saved")
    return True

# COMMAND ----------

# MAGIC %md
# MAGIC # Creation of Ecuador tables
# MAGIC From here on, do the actual loading. Making an example with the Ecuador tables

# COMMAND ----------

catalog = "ecuador"
create_catalog(catalog_name=catalog, catalog_bucket=catalog_bucket)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecuador logistics

# COMMAND ----------

db = "logistics"
create_db(db_name=db, catalog_name=catalog, catalog_bucket=catalog_bucket)
dir_path = f"{data-bucket}/ecuador/logistics"
files_list = []
list_files_recursively(dir_path, files_list)
print(files_list)

# COMMAND ----------

# DBTITLE 1,Actual creation
for csv_record in files_list:
    delimiter, temp_csv_path = pre_process_csv(csv_record)
    create_table_from_csv(source_filepath=csv_record, catalog=catalog, db=db, tmp_csv_filepath=temp_csv_path, delimiter=delimiter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecuador Trade

# COMMAND ----------

# DBTITLE 0,Trade
catalog = "ecuador_catalog"
db = "trade"
dir_path = f"{data_bucket}/ecuador/trade"
files_list = []
list_files_recursively(dir_path, files_list)
print(files_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecuador Spatial

# COMMAND ----------

# DBTITLE 0,Trade
catalog = "ecuador_catalog"
db = "spatial"
dir_path = f"{data_bucket}/ecuador/spatial"
files_list = []
list_files_recursively(dir_path, files_list)
print(files_list)

# COMMAND ----------

# DBTITLE 1,Actual creation
for csv_record in files_list:
    delimiter, temp_csv_path = pre_process_csv(csv_record)
    create_table_from_csv(source_filepath=csv_record, catalog=catalog, db=db, tmp_csv_filepath=temp_csv_path, delimiter=delimiter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecuador Production

# COMMAND ----------

# DBTITLE 0,Trade
catalog = "ecuador_catalog"
db = "production"
dir_path = f"{data_bucket}/ecuador/production"
files_list = []
list_files_recursively(dir_path, files_list)
print(files_list)

# COMMAND ----------

# DBTITLE 1,Actual creation
for csv_record in files_list:
    delimiter, temp_csv_path = pre_process_csv(csv_record)
    create_table_from_csv(source_filepath=csv_record, catalog=catalog, db=db, tmp_csv_filepath=temp_csv_path, delimiter=delimiter)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecuador Metadata

# COMMAND ----------

# DBTITLE 0,Trade
catalog = "ecuador_catalog"
db = "metadata"
dir_path = f"{data_bucket}/ecuador/metadata"
files_list = []
list_files_recursively(dir_path, files_list)
print(files_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ecuador Indicators

# COMMAND ----------

# DBTITLE 0,Trade
catalog = "ecuador_catalog"
db = "indicators"
dir_path = f"{data_bucket}/ecuador/indicators"
files_list = []
list_files_recursively(dir_path, files_list)
print(files_list)

# COMMAND ----------

# DBTITLE 1,Actual creation
for csv_record in files_list:
    delimiter, temp_csv_path = pre_process_csv(csv_record)
    create_table_from_csv(source_filepath=csv_record, catalog=catalog, db=db, tmp_csv_filepath=temp_csv_path, delimiter=delimiter)
