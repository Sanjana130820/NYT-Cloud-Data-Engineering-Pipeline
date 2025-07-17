#!/usr/bin/env python
# coding: utf-8

# In[ ]:

import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def task1():
    print ("Executing Task 1")

def task2():
    print ("Executing Task 2")


def run_streamline_script():
    import pandas as pd
    import datetime
    import requests
    import time
    from google.cloud import storage
    from datetime import timedelta
    

    def streaming_fetch(api_key, begin_date, end_date, page = 0):
        page_size = 10
        url = "https://api.nytimes.com/svc/search/v2/articlesearch.json?"
        article = []
        response = requests.get(url,params = {"api-key":api_key,"begin_date":begin_date,"end_date":end_date,"page":page})
        time.sleep(12)
        if response.status_code == 200:
            data = response.json()
            article.extend(data["response"]["docs"])
            if data["response"]["meta"]["hits"] > (page + 1) * page_size:
                next_page = streaming_fetch(api_key, begin_date, end_date, page=page+1)
                article.extend(next_page)
        return article

    api_key = "e9fu5FKIXZHEPv6YFwFdnCa8Zxc3AcHY"

    start = datetime.datetime.now() - timedelta(days=2)
    begin_date = start.strftime('%Y%m%d')
    stop = datetime.datetime.now() - timedelta(days=1)
    end_date = stop.strftime('%Y%m%d')
    
    #begin_date = "20240503"
    #end_date = "20240506"

    df = streaming_fetch(api_key,begin_date,end_date)
    streaming_data = pd.DataFrame(df)

    def upload_csv_to_gcs(dataframe, bucket_name, file_name):
    # Initialize GCS client
        
        client = storage.Client()
        bucket = client.bucket('group-project9-data225')
        

        credentials_blob = bucket.blob('Streamlinedata/crack-will-422608-j1-166ab9162e68.json')
        credentials_blob.download_to_filename('/tmp/credentials.json')

        csv_string = dataframe.to_csv(index=False)

        upload_bucket = client.bucket(bucket_name)

    # Create a new blob and upload the CSV data
        blob = upload_bucket.blob(file_name)
        blob.upload_from_string(csv_string, content_type='text/csv')

        print(f'CSV data uploaded to gs://{bucket_name}/{file_name}')

# Example usage
    bucket_name = "group-project9-data225"
    file_name = f"files/{begin_date}_to_{end_date}.csv"

    upload_csv_to_gcs(streaming_data, bucket_name, file_name)

    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    # Initialize a BigQuery client
    bq_client = bigquery.Client.from_service_account_json('/tmp/credentials.json')

    schema = [
        bigquery.SchemaField("abstract", "STRING"),
        bigquery.SchemaField("web_url", "STRING"),
        bigquery.SchemaField("snippet", "STRING"),
        bigquery.SchemaField("lead_paragraph", "STRING"),
        bigquery.SchemaField("print_section", "STRING"),
        bigquery.SchemaField("print_page", "STRING"),
        bigquery.SchemaField("source", "STRING"),
        bigquery.SchemaField("multimedia", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("rank", "INT64"),
            bigquery.SchemaField("subtype", "STRING"),
            bigquery.SchemaField("caption", "STRING"),
            bigquery.SchemaField("credit", "STRING"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("url", "STRING"),
            bigquery.SchemaField("height", "INT64"),
            bigquery.SchemaField("width", "INT64"),
            bigquery.SchemaField("crop_name", "STRING"),
            bigquery.SchemaField("legacy", "RECORD", fields=[
                bigquery.SchemaField("xlarge", "STRING"),
                bigquery.SchemaField("xlargewidth", "INT64"),
                bigquery.SchemaField("xlargeheight", "INT64"),
                bigquery.SchemaField("thumbnail", "STRING"),
                bigquery.SchemaField("thumbnailwidth", "INT64"),
                bigquery.SchemaField("thumbnailheight", "INT64"),
                bigquery.SchemaField("wide", "STRING"),
                bigquery.SchemaField("widewidth", "INT64"),
                bigquery.SchemaField("wideheight", "INT64"),
            ]),
        ]),
        bigquery.SchemaField("headline", "RECORD", fields=[
            bigquery.SchemaField("main", "STRING"),
            bigquery.SchemaField("kicker", "STRING"),
            bigquery.SchemaField("content_kicker", "STRING"),
            bigquery.SchemaField("print_headline", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("seo", "STRING"),
            bigquery.SchemaField("sub", "STRING"),
        ]),
        bigquery.SchemaField("keywords", "RECORD", mode="REPEATED", fields=[
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("value", "STRING"),
            bigquery.SchemaField("rank", "INT64"),
            bigquery.SchemaField("major", "STRING"),
        ]),
        bigquery.SchemaField("pub_date", "TIMESTAMP"),
        bigquery.SchemaField("document_type", "STRING"),
        bigquery.SchemaField("news_desk", "STRING"),
        bigquery.SchemaField("section_name", "STRING"),
        bigquery.SchemaField("byline", "RECORD", fields=[
            bigquery.SchemaField("original", "STRING"),
            bigquery.SchemaField("person", "RECORD", mode="REPEATED", fields=[
                bigquery.SchemaField("firstname","STRING"),
                bigquery.SchemaField("middlename","STRING"),
                bigquery.SchemaField("lastname","STRING"),
                bigquery.SchemaField("qualifier","STRING"),
                bigquery.SchemaField("title","STRING"),
                bigquery.SchemaField("role","STRING"),
                bigquery.SchemaField("organization","STRING"),
                bigquery.SchemaField("rank","INT64"),
            ]),
            bigquery.SchemaField("organization", "STRING"),
        ]),
        bigquery.SchemaField("type_of_material", "STRING"),
        bigquery.SchemaField("_id", "STRING"),
        bigquery.SchemaField("word_count", "INT64"),
        bigquery.SchemaField("uri", "STRING"),
        bigquery.SchemaField("subsection_name", "STRING"),
        bigquery.SchemaField("run_timestamp", "TIMESTAMP"),
    ]

    streaming_data['pub_date'] = pd.to_datetime(streaming_data['pub_date'])
    streaming_data['pub_date'] = streaming_data['pub_date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    streaming_data.fillna("", inplace = True)

    current_timestamp = datetime.datetime.now()
    current_timestamp_str = current_timestamp.strftime('%Y-%m-%d %H:%M:%S')
    streaming_data['run_timestamp'] = current_timestamp_str

    rows_to_insert = streaming_data.to_dict(orient="records")
    for item in rows_to_insert:
        for multimedia_item in item.get('multimedia', []):
            if 'subType' in multimedia_item:
                del multimedia_item['subType']

    table_id = "crack-will-422608-j1.GroupProject.article"

    def date_format(a):
        from datetime import datetime
        date_obj = datetime.strptime(a, "%Y%m%d")
        formatted_date_str = date_obj.strftime("%Y-%m-%d")
        return formatted_date_str

    sql = f"""SELECT *except(pub_date,run_timestamp),date(pub_date) as pub_date  FROM `{table_id}` WHERE DATE(pub_date) BETWEEN '{date_format(begin_date)}' and '{date_format(begin_date)}'"""

    data = bq_client.query(sql)
    rows = []

    for row in data.result():
        rows.append(dict(row.items()))

    if len(rows) ==0:
        bigquery_data = pd.DataFrame(columns=streaming_data.columns)
    else:
        bigquery_data = pd.DataFrame(rows)

    #bigquery_data.head()

    def identify_changes(streaming_data, bigquery_data):

        current_data = streaming_data.drop(columns = ['run_timestamp'])
        current_data['pub_date'] = pd.to_datetime(current_data['pub_date'])
        current_data['pub_date'] = current_data['pub_date'].dt.strftime('%Y-%m-%d')

        bigquery_data['pub_date'] = pd.to_datetime(bigquery_data['pub_date'])
        bigquery_data['pub_date'] = bigquery_data['pub_date'].dt.strftime('%Y-%m-%d')

        columns_to_drop = ['keywords','headline','multimedia','byline']
        api_source = current_data.drop(columns = columns_to_drop)
        bq_destination = bigquery_data.drop(columns = columns_to_drop)

        common_records = pd.merge(api_source, bq_destination, how='inner', on='_id', suffixes=('_source', '_destination'))

        new_records = api_source[~api_source['_id'].isin(common_records['_id'])]

        updated_records = common_records[~common_records.drop('_id', axis=1).apply(lambda row: all(row[f'{col}_source'] == row[f'{col}_destination'] for col in api_source.drop('_id', axis=1).columns), axis=1)]


        return new_records, updated_records

    new_records, updated_records = identify_changes(streaming_data, bigquery_data)

    log_table_id = "crack-will-422608-j1.GroupProject.log_table"

    try:
        bq_client.get_table(log_table_id)
        print("Log table exists already")
    except:
        table = bigquery.Table(log_table_id, schema=schema)
        table = bq_client.create_table(table)
        print("Log table created successfully")

    if not updated_records.empty:
        updated_ids = set(updated_records['_id'])
        updated_ids_str = ", ".join([f"'{id}'" for id in updated_ids])
    else:
        updated_ids = {}
        updated_ids_str = ""

    #updated_rows = bigquery_data[bigquery_data['_id'].isin(updated_ids)]

    if updated_ids_str:
        update_query = f"""
            INSERT INTO `{log_table_id}`
            SELECT *
            FROM `{table_id}`
            WHERE _id IN ({updated_ids_str})
        """
        query_job = bq_client.query(update_query)

        query_job.result()
        print("Updated records copied to log_table successfully.")

        delete_query = f"""
            DELETE FROM `{table_id}`
            WHERE _id IN ({updated_ids_str})
        """
        query_job = bq_client.query(delete_query)

        query_job.result()

        print("Deleted the updated records from original table successfully.")
    else:
        print("No Updated records.")
   

    batch_size = 1000

    if not new_records.empty:
        new_ids = set(new_records['_id'])
    else:
        new_ids = {}

    final_rows_to_insert = streaming_data[streaming_data['_id'].isin(updated_ids) | streaming_data['_id'].isin(new_ids)]
    final_rows_to_insert = final_rows_to_insert.to_dict(orient="records")


    num_batches = (len(final_rows_to_insert) + batch_size - 1) // batch_size

    for i in range(num_batches):
        start_idx = i * batch_size
        end_idx = min((i + 1) * batch_size, len(final_rows_to_insert))
        batch_data = final_rows_to_insert[start_idx:end_idx]

        print(table_id)
        errors = bq_client.insert_rows_json(table_id, batch_data)

        if errors:
            print(f"Errors occurred while uploading batch {i}: {errors}")
        else:
            print(f"Batch {i} uploaded successfully.")


dag = DAG('abc_dag',
    default_args=default_args,
    description='A DAG to run abc.py script',
    schedule_interval='@daily',
)


task_1 = PythonOperator(
 task_id='task_1',
 python_callable=task1,
 dag=dag,
)
task_2 = PythonOperator(
 task_id='task_2',
 python_callable=task2,
 dag=dag,
)

run_script = PythonOperator(
    task_id='run_streamline_script',
    python_callable=run_streamline_script,
    dag=dag,
)

task_1 >> run_script >> task_2

