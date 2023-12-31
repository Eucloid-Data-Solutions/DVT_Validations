import os
#source
credential_path = "harshround-pen1-118dcd4f1129.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

query = """
    SELECT table_name 
    FROM harshround-pen1.harsh2.INFORMATION_SCHEMA.TABLES
"""
source = client.query(query)  # Make an API request.
source_tables=[]
for row in source:
    # Row values can be accessed by field name or index.
    source_tables.append(row[0])
#Destinstion
credential_path = "round-pen-403209-9120443118c3.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
client = bigquery.Client()
query = """
    SELECT table_name 
    FROM round-pen-403209.harsh_1412.INFORMATION_SCHEMA.TABLES
"""
destination = client.query(query)  # Make an API request.
de_tables=[]
for row in destination:
    # Row values can be accessed by field name or index.
    de_tables.append(row[0])
s1=0
matched_tables=[]
if(source_tables==de_tables):
    print("step:1 Passed")
    for i in source_tables:
        matched_tables.append(i)
    s1=1

else:
    print("Missing tables:")
    for i in source_tables:
        if i not in de_tables:
            print(i)
        else:
            matched_tables.append(i)
    
matched_tables
cols_d={}
rows_d={}

for i in matched_tables:
    query = """
    SELECT column_name, data_type
    FROM round-pen-403209.harsh_1412.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = @table_name
    """

    query2 = f"""
    SELECT count(*)
    FROM round-pen-403209.harsh_1412.{i}
    """

    query_params = [bigquery.ScalarQueryParameter("table_name", "STRING", i)]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    results2 = client.query(query2).result()
    
    temp=[]
    for row in results:
        temp.append(row[0])
    cols_d[i]=temp
    for r in results2:
        rows_d[i]=r[0]

rows_d
credential_path = "harshround-pen1-118dcd4f1129.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
client = bigquery.Client()
cols_s={}
rows_s={}

for i in matched_tables:
    query = """
    SELECT column_name, data_type
    FROM harshround-pen1.harsh2.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = @table_name
    """

    query2 = f"""
    SELECT count(*)
    FROM harshround-pen1.harsh2.{i}
    """

    query_params = [bigquery.ScalarQueryParameter("table_name", "STRING", i)]
    job_config = bigquery.QueryJobConfig(query_parameters=query_params)
    query_job = client.query(query, job_config=job_config)
    results = query_job.result()
    results2 = client.query(query2).result()
    
    temp=[]
    for row in results:
        temp.append(row[0])
    cols_s[i]=temp
    for r in results2:
        rows_s[i]=r[0]

print(cols_s==cols_d) #checked for column names and count
# check for number of records
syncp={}
a=rows_s.keys()
for i in a:
    syncp[i]=100-(rows_s[i]-rows_d[i])/rows_d[i]*100

for i in a:
    if(syncp[i]>=90):
        print(i,':',syncp[i],' %')
