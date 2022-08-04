import typer
import snowflake.connector
import pandas as pd
import httpx
import datetime
from sys import stderr
from snowflake.connector.errors import DatabaseError, ProgrammingError
from controller import SnowflakeCred
from auth import *

app = typer.Typer()    
# url = "https://officedepot--oduat.my.salesforce.com"
# token = "00D590000004ea0!AQ8AQC1XOqoQE1AItY.cvdT9S9YUkXd1xDiArCYsASTGuaESNoIjkGjmb2CYZq_gkkWuoqrB5R0R.LfIqgePmZ0nDAMwa2fw"

# con = snowflake.connector.connect(user="rakesh.molakala@officedepot.com",
#                                      account="tp97266.east-us-2.azure",                                      
#                                      authenticator="externalbrowser")  

url=""
token=""
con=None  

# Fetches the detailed job status of given job
@app.command()
def getJobInfo(job_idd: str, versionn: str):
    query_path = f"services/data/v{versionn}/jobs/ingest/{job_idd}"
    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    return data.json()

# Loads the given job's error data into Snowflake error table
# external/env variables

@app.command()
def load_to_snowflake(job_id: str, version: str, schema: str="TEMPDB"):
    # schema = "TEMPDB"
    # add as paraemeter schema

    # Getting detailed job information and checking whether it is succussful or not
    job_info = getJobInfo(job_idd=job_id, versionn=version)
    if(job_info['numberRecordsFailed']==0):
        print(f"No failed records for this job {job_id}")
        return

    # Defining error table name and original table name from object information
    object_name = job_info['object']
    original_table_name = ("sfdc_"+object_name+"_object").upper()
    error_table_name = ("sfdc_"+object_name+"_errors").upper()
    db_cursor_eb = con.cursor()

    # Creating new error table if not exists and adding three columns that define error table from original table
    sql = "select exists (select * from information_schema.tables where table_schema='"+ schema +"' and table_name ='"+ error_table_name + "') as res"
    res = db_cursor_eb.execute(sql)
    if(not res.fetchone()[0]):
        db_cursor_eb.execute(f'CREATE TABLE {schema}.{error_table_name} LIKE SFDC_DS.{original_table_name};')
        db_cursor_eb.execute(f'ALTER TABLE {schema}.{error_table_name} ADD COLUMN JOB_ID VARCHAR(30);')
        db_cursor_eb.execute(f'ALTER TABLE {schema}.{error_table_name} ADD COLUMN SF__ID VARCHAR(30);')
        db_cursor_eb.execute(f'ALTER TABLE {schema}.{error_table_name} ADD COLUMN SF__ERROR VARCHAR;')

    # Checking whether the job error data is already loaded into Snowflake error table ot not
     # job_id and record_id
    try:
        res = db_cursor_eb.execute(f'SELECT DISTINCT job_id FROM {schema}.{error_table_name};')
        exis_jobs = set()
        for c in res:
            exis_jobs.add(c[0])
    except ProgrammingError as db_ex:
        print(f"Programming error: {db_ex}")
        raise
    if(job_id in exis_jobs):
        print("Job's error data already loaded into Snowflake")
        return

    # Getting failed records for the given job_id into a local CSV file and adding the Job_id field as the first column and populating its data
    get_failed_ingest_job_result(job_id, version="53.0")
    csv_input = pd.read_csv('curr_fail_job.csv')
    csv_input.insert(0,'JOB_ID','')
    csv_input['JOB_ID'] = job_id
    csv_input.to_csv('curr_fail_job.csv', index=False)
    csv_file = 'curr_fail_job.csv'

    # Getting field names that are not in the error table but in CSV file and adding those fields to error table
    csv_column_names_list = []
    csv_column_names = set()
    for col in csv_input.columns:
        csv_column_names_list.append(col.upper())
        csv_column_names.add(col.upper())

    table_column_names = set()
    tempres1 = db_cursor_eb.execute(f"select COLUMN_NAME from information_schema.columns where table_schema='{schema}' and table_name ='{error_table_name}';")
    for col in tempres1.fetchall():
        table_column_names.add(col[0])

    new_columns = csv_column_names - table_column_names
    for nc in new_columns:
        db_cursor_eb.execute(f'ALTER TABLE {schema}.{error_table_name} ADD COLUMN {nc} VARCHAR;')

    tempstr1 = ""
    for i in range(len(csv_column_names_list)):
        tempstr1 = tempstr1 + "t.$"+str(i+1)+","
    tempstr1 = tempstr1[:-1]
    tempstr2 = ",".join(csv_column_names_list)



    db_cursor_eb.execute('use schema TEMPDB')

    # Preparing and using @internal_stage present in TEMPDB schema as a staging table to store CSV data 
    db_cursor_eb.execute("remove @tempdb.internal_stage pattern='.*.csv.gz';")
    db_cursor_eb.execute(""" alter stage tempdb.internal_stage set file_format = (type = 'CSV' field_delimiter = ',' skip_header = 1 field_optionally_enclosed_by = '"') """)
    db_cursor_eb.execute("PUT file://" + csv_file + " @tempdb.internal_stage auto_compress=true")
    db_cursor_eb.execute(""" create or replace file format mycsvformat type = 'csv' field_delimiter = ',' skip_header=1  field_optionally_enclosed_by = '"' """)

    # Using the copy command to move data from staging table to Snowflake error table
    # use util db
    try:
        db_cursor_eb.execute(f'copy into {schema}.{error_table_name}({tempstr2}) from (select {tempstr1} from @tempdb.internal_stage/curr_fail_job.csv.gz t) file_format = (format_name = mycsvformat)' \
          'ON_ERROR = "ABORT_STATEMENT" ')
        print(f"Successfully loaded {job_id}'s failed data into Snowflake Error tables")
    except ProgrammingError as db_ex:
        print(f"Programming error: {db_ex}")
        raise
        
    

# Fetches the list of all ingest job ids
@app.command()
def get_all_ingest_job_ids(version: str, hours: int):
    query_path = f"services/data/v{version}/jobs/ingest"
    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    ingest_job_ids = [] 
    for job in data.json()['records']:
        created_date = job['createdDate']
        job_date_time = datetime.datetime.strptime(created_date[:19], "%Y-%m-%dT%H:%M:%S")
        curr_date_time = datetime.datetime.now()
        
        temp = curr_date_time - job_date_time
        hours_till_now = int((temp.total_seconds())/(60*60))
        if(hours_till_now<=hours):
            ingest_job_ids.append(job['id'])
    
    return ingest_job_ids

# Loads failed records of all ingest jobs for the past 7 days
# add day parameter 
@app.command()
def load_all_failed_jobs_to_snowflake(versionn: str, hours: int):
    ingest_job_ids = get_all_ingest_job_ids(versionn, hours)
    for job in ingest_job_ids:
        load_to_snowflake(job_id=job, version=versionn)


def establish_connection():
    snowflake_cred = SnowflakeCred()
    salesforce_cred = SalesforceCred()
    global url,token,con
    con=get_snowflake_client(snowflake_cred)
    print(con)
    url=salesforce_cred.url
    token = salesforce_cred.token
    

# Loads the Job's failed records data into a local CSV file
def get_failed_ingest_job_result(job_id: str, version: str):
    query_path = f"services/data/v{version}/jobs/ingest/{job_id}/failedResults"
    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    print(url+query_path)
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    
    url_content = data.content
    filename = 'curr_fail_job.csv'
    csv_file = open(filename, 'wb')
    csv_file.write(url_content)
    csv_file.close()     

if __name__ == "__main__":
    establish_connection()
    app()
    con.close()
    



    

