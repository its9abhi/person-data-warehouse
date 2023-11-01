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

def tranform_employee_personal_data (general_data,job_details,department):
    general_data = general_data[["EmployeeID","Employee Name ","Age","Education","EducationField","Gender","JobLevel","MaritalStatus","Over18","Attrition","JobRole","Department"]]
    general_data = general_data.rename(columns = {"Over18" : "IsOver18", "Attrition":"IsActiveEmployee","Employee Name ":"EmployeeName"})
    dep_job = job_details.merge(department,on="DepartmentId")
    general_data = general_data.merge(dep_job , on = ["JobRole","Department"])
    general_data = general_data.drop(columns= ["Department","DepartmentId","JobRole"],axis=1)
    education_level_mapping = {
        1 : 'Below College',
        2 : 'College',
        3 : 'Bachelor',
        4 : 'Master',
        5 : 'Doctor',
    }
    general_data["Education"] = general_data["Education"].map(education_level_mapping)
    return general_data

def employee_personal_data_etl():

    client = storage.Client()
    bucket_name = 'person-data-warehouse'
    bucket = client.get_bucket(bucket_name)

    # Extraction
    job_details = extract_data_from_gcs(bucket,"JobDetails.csv")
    department = extract_data_from_gcs(bucket,"Department.csv")
    general_data = extract_data_from_gcs(bucket,"general_data.csv")

    # Transformation
    employee_personal_data = tranform_employee_personal_data(general_data,job_details,department)

    # Load data to gcs bucket
    export_data_to_gcs(bucket,"EmployeePersonalData.csv",employee_personal_data.to_csv(index= False))

    # Load Data to BigQuery

    dataset_id = "PersonDataWarehouse"
    load_data_to_bigquery(dataset_id,"EmployeePersonalData",employee_personal_data)

if __name__ == "__main__":
    employee_personal_data_etl()


