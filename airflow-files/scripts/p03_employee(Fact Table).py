import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
import pandas_gbq
from io import StringIO

def extract_data_from_gcs (bucket,file_name):
    blob = bucket.get_blob(file_name)
    content = blob.download_as_text()
    csv_data = StringIO(content)
    return pd.read_csv(csv_data)

def load_data_to_bigquery(data_set,table_id,data):
    client =  bigquery.Client()
    table_ref = f"{data_set}.{table_id}"
    pandas_gbq.to_gbq(data,destination_table=table_ref,if_exists='replace')

def export_data_to_gcs(bucket,file_name,data,content_type = "text/csv"):
    blob = bucket.blob(file_name)
    blob.upload_from_string(data, content_type=content_type)

def tranform_fact_table(employee_stage):
    employee_data = employee_stage[["EmployeeID","DistanceFromHome","MonthlyIncome","NumCompaniesWorked","PercentSalaryHike","StandardHours","StockOptionLevel","TotalWorkingYears","TrainingTimesLastYear","YearsAtCompany","YearsSinceLastPromotion","YearsWithCurrManager","EmployeeSurveyId","ManagerSurveyId"]]
    return employee_data

def employee_data_etl():

    client = storage.Client()
    bucket_name = 'person-data-warehouse'
    bucket = client.get_bucket(bucket_name)

    # Extraction
    employee_stage = extract_data_from_gcs(bucket,'EmployeeStage.csv')

    # Transformation
    employee_data = tranform_fact_table(employee_stage)

    # Load data to gcs bucket
    export_data_to_gcs(bucket,'EmployeeData.csv',employee_data.to_csv(index = False))

    # Load Data to BigQuery
    dataset_id = "PersonDataWarehouse"
    load_data_to_bigquery(dataset_id,"EmployeeData",employee_data)

if __name__ == "__main__" :
    employee_data_etl()