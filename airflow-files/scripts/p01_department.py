import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO
import pandas_gbq

def extract_data_from_gcs(bucket_name, file_name):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    content = blob.download_as_text()
    return content

def transform_data(df):
    department = pd.DataFrame(df["Department"].unique(), columns=["Department"]).reset_index().rename(columns={"index": "DepartmentId"})
    department_job_role = df["Department"] + "|" + df["JobRole"]
    department_job_role = department_job_role.unique()
    dep = []
    job_role = []
    for i in range(len(department_job_role)):
        dep.append(department_job_role[i].split("|")[0])
        job_role.append(department_job_role[i].split("|")[1])
    department_job_role = pd.DataFrame({"Department": dep, "JobRole": job_role}).reset_index().rename(columns={"index": "JobId"})
    department_job_role = pd.merge(department_job_role, department, on="Department", how="left").drop(["Department"], axis=1)
    return department, department_job_role

def load_data_to_gcs(bucket_name, file_name, data, content_type="text/csv"):
    client =  storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data, content_type=content_type)

def load_data_to_bigquery(dataset_id, table_id, data):
    client = bigquery.Client()
    table_ref = f"{dataset_id}.{table_id}"
    pandas_gbq.to_gbq(data, destination_table=table_ref,if_exists='replace')

def department_etl():
    bucket_name = 'person-data-warehouse'
    file_name = 'general_data.csv'

    # Extraction
    content = extract_data_from_gcs(bucket_name, file_name)

    # Transformation
    df = pd.read_csv(StringIO(content))
    department, department_job_role = transform_data(df)

    # Loading to GCS
    load_data_to_gcs(bucket_name, "Department.csv", department.to_csv(index=False))
    load_data_to_gcs(bucket_name, "JobDetails.csv", department_job_role.to_csv(index=False))

    # Loading to BigQuery
    load_data_to_bigquery("PersonDataWarehouse", "Department", department)
    load_data_to_bigquery("PersonDataWarehouse", "JobDetails", department_job_role)

if __name__ == "__main__":
    department_etl()
