import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO
import pandas_gbq

def load_data_to_bigquery(dataset_id, table_id, data):
    client = bigquery.Client()
    table_ref = f"{dataset_id}.{table_id}"
    pandas_gbq.to_gbq(data, destination_table=table_ref)

def extract_data_from_gcs(bucket, file_name):
    blob = bucket.get_blob(file_name)
    content = blob.download_as_text()
    return pd.read_csv(StringIO(content))

def transform_survey_data(emp_survey, manager_survey, general_data):
    emp_survey = emp_survey.fillna(0)
    manager_survey = manager_survey.fillna(0)
    rating_mapping = {
        0: 'N/A',
        1: 'Low',
        2: 'Medium',
        3: 'High',
        4: 'Very High'
    }
    emp_survey_table = emp_survey[["EnvironmentSatisfaction","JobSatisfaction","WorkLifeBalance"]].drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"SurveyId"})
    df = pd.merge(emp_survey_table,emp_survey,on = ["EnvironmentSatisfaction","JobSatisfaction","WorkLifeBalance"])
    general_data = pd.merge(general_data,df,on = "EmployeeID").drop(columns =["EnvironmentSatisfaction","JobSatisfaction","WorkLifeBalance"])
    manger_survey_table = manager_survey[['JobInvolvement', 'PerformanceRating']].drop_duplicates().reset_index(drop=True).reset_index().rename(columns = {"index":"SurveyId"})
    df = pd.merge(manger_survey_table,manager_survey,on = ['JobInvolvement', 'PerformanceRating'])
    general_data = pd.merge(general_data,df,on = "EmployeeID").drop(columns =['JobInvolvement', 'PerformanceRating']).rename(columns = {"SurveyId_x":"EmployeeSurveyId","SurveyId_y" : "ManagerSurveyId"})
    emp_survey_columns= ["EnvironmentSatisfaction","JobSatisfaction","WorkLifeBalance"]
    manager_survey_columns = ["JobInvolvement","PerformanceRating"]
    emp_survey_table[emp_survey_columns] = emp_survey_table[emp_survey_columns].astype(int)
    manger_survey_table[manager_survey_columns] = manger_survey_table[manager_survey_columns].astype(int)
    emp_survey_table["EnvironmentSatisfaction"] = emp_survey_table["EnvironmentSatisfaction"].map(rating_mapping)
    emp_survey_table["JobSatisfaction"] = emp_survey_table["JobSatisfaction"].map(rating_mapping)
    emp_survey_table["WorkLifeBalance"] = emp_survey_table["WorkLifeBalance"].map(rating_mapping)
    manger_survey_table["JobInvolvement"] = manger_survey_table["JobInvolvement"].map(rating_mapping)
    manger_survey_table["PerformanceRating"] = manger_survey_table["PerformanceRating"].map(rating_mapping)

    return emp_survey_table, manger_survey_table, general_data

def load_data_to_gcs(bucket, file_name, data, content_type="text/csv"):
    blob = bucket.blob(file_name)
    blob.upload_from_string(data, content_type=content_type)

def survey_etl():
    client = storage.Client()
    bucket_name = 'person-data-warehouse'
    bucket = client.get_bucket(bucket_name)

    # Extraction
    emp_survey = extract_data_from_gcs(bucket, "employee_survey_data.csv")
    manager_survey = extract_data_from_gcs(bucket, "manager_survey_data.csv")
    general_data = extract_data_from_gcs(bucket, "general_data.csv")

    # Transformation
    emp_survey, manager_survey, general_data = transform_survey_data(emp_survey, manager_survey, general_data)

    # Loading to GCS
    load_data_to_gcs(bucket, "EmployeeSurveyData.csv", emp_survey.to_csv(index=False))
    load_data_to_gcs(bucket, "ManagerSurveyData.csv", manager_survey.to_csv(index=False))
    load_data_to_gcs(bucket, "EmployeeStage.csv", general_data.to_csv(index=False))

    # Loading to BigQuery
    dataset_id = "PersonDataWarehouse"
    load_data_to_bigquery(dataset_id, "EmployeeSurveyData", emp_survey)
    load_data_to_bigquery(dataset_id, "ManagerSurveyData", manager_survey)

if __name__ == "__main__":
    survey_etl()
