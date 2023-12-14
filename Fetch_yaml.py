import yaml
with open('config_test2.yaml', 'r') as yaml_file:
    yaml_content = yaml.load(yaml_file, Loader=yaml.FullLoader)
#step 1: Checking if all the tables match in Source and Target Schema
matched_tables=[]
missing_tables=[]
for i in yaml_content['Source_tables']:
    if i in yaml_content['Target_tables']:
        matched_tables.append(i)
    else:
        missing_tables.append(i)
#Checking for all matching tables if the number of columns is matching between source and target
#Destination
from google.cloud import bigquery
import os
credential_path = "round-pen-403209-9120443118c3.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
client = bigquery.Client()
cols_s={}
cols_s_names={}

for i in matched_tables:
    q = f"""
    SELECT count(distinct column_name)
    FROM {yaml_content['Source_schema']}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{i}'
    """

    q2 = f"""
    SELECT column_name
    FROM {yaml_content['Source_schema']}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{i}'
    """

    results = client.query(q).result()
    results2= client.query(q2).result()
    temp1=[]
    for row in results2:
        temp1.append(row[0])
    cols_s_names[i]=temp1

    temp=''
    for row in results:
        temp=row[0]
    cols_s[i]=temp

    
credential_path = "harshround-pen1-118dcd4f1129.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
client = bigquery.Client()
cols_d={}
cols_d_names={}

for i in matched_tables:
    query = f"""
    SELECT count(distinct column_name)
    FROM {yaml_content['Target_schema']}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{i}'
    """
    query2 = f"""
    SELECT column_name
    FROM {yaml_content['Target_schema']}.INFORMATION_SCHEMA.COLUMNS
    WHERE table_name = '{i}'
    """

    results = client.query(query).result()
    results2= client.query(query2).result()
    temp1=[]
    for row in results2:
        temp1.append(row[0])
    cols_d_names[i]=temp1

    temp=''
    for row in results:
        temp=row[0]
    cols_d[i]=temp

missing_c=[]
matched_c=[]
for i in cols_d.keys():
    if cols_s[i]==cols_d[i]:
        matched_c.append(i)
    else:
        missing_c.append(i)

with open('log.txt', 'w') as file:
    if len(missing_tables)>0:
        file.write('Missing tables in Target schema:\n')
        for i in missing_tables:
            file.write("-"+i+"\n")
    else:
        file.write("Step 1 Success\n")
    file.write("\n\n")
    if len(missing_c)>0:
        file.write('Missing columns:\n')
        for i in missing_c:
            file.write(i+":\n")
            for j in cols_s_names[i]:
                if j not in cols_d_names[i]:
                    file.write("\t-"+j+"\n")
    else:
        file.write("Step 2 success with no missing columns")
