from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import os

# Configuration variables for Google Analytics and BigQuery
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = '../keys/gtm-w6kpsfd7-yjbhm-5808ebc38263.json'  # Path to your Google Cloud service account key file
URU_REPORTING_VIEW = '79428303'
URU_MAIN_VIEW = '151196979'
VIEW_ID = URU_REPORTING_VIEW  # Your Google Analytics View ID
BIGQUERY_PROJECT = 'gtm-w6kpsfd7-yjbhm'  # Your Google Cloud Project ID
BIGQUERY_DATASET = 'ua_storage_test'  # BigQuery Dataset name where the data will be stored
BIGQUERY_TABLE = '26-june-reporting-5'  # BigQuery Table name where the data will be stored
# Setting up the environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE_LOCATION

def initialize_analyticsreporting():
    """Initializes the Google Analytics Reporting API client."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def get_report(analytics, goals):
    """Fetches the report data from Google Analytics."""
    # Here, specify the analytics report request details
    if not(goals):
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'dateRanges': [{'startDate': '2020-07-01', 'endDate': '2020-07-31'}],
                        # Metrics and dimensions are specified here
                        'metrics': [
                            {'expression': 'ga:sessions'},
                            {'expression': 'ga:pageviews'},
                            {'expression': 'ga:users'},
                            {'expression': 'ga:newUsers'},
                            {'expression': 'ga:bounceRate'},
                            {'expression': 'ga:sessionDuration'},
                            # Add or remove metrics as per your requirements
                        ],
                        'dimensions': [  # 2 + date is the limit !!!
                            {'name': 'ga:date'},
                            {'name': 'ga:campaign'},
                            {'name': 'ga:source'},
                            {'name': 'ga:medium'},
                            # {'name': 'ga:country'},
                            # {'name': 'ga:pageTitle'},
                            # {'name': 'ga:browser'},
                            # {'name': 'ga:pagePath'},
                            # {'name': 'ga:source'},
                            # {'name': 'ga:date'},
                            # {'name': 'ga:pagePath'},
                            # {'name': 'ga:deviceCategory'},
                            # Add or remove dimensions as per your requirements
                        ],
                        'pageSize': 20000  # Adjust the pageSize as needed
                    }
                ]
            }
        ).execute()
    else: # IF THIS IS THE GOALS REPORT
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'dateRanges': [{'startDate': '2021-03-17', 'endDate': '2021-10-17'}],
                        # Metrics and dimensions are specified here
                        'metrics': [
                            {'expression': 'ga:goal1Completions'},
                            {'expression': 'ga:goal2Completions'},
                            {'expression': 'ga:goal3Completions'},
                            {'expression': 'ga:goal4Completions'},
                            {'expression': 'ga:goal5Completions'},
                            {'expression': 'ga:goal6Completions'},
                            {'expression': 'ga:goal7Completions'},
                            {'expression': 'ga:goal8Completions'},
                        ],
                        'dimensions': [  # 2 + date is the limit !!!
                            {'name': 'ga:date'},
                            {'name': 'ga:campaign'},
                            {'name': 'ga:source'},
                            {'name': 'ga:medium'},
                            # {'name': 'ga:country'},
                            # {'name': 'ga:pageTitle'},
                            # {'name': 'ga:browser'},
                            # {'name': 'ga:pagePath'},
                            # {'name': 'ga:source'},
                            # {'name': 'ga:date'},
                            # {'name': 'ga:pagePath'},
                            # {'name': 'ga:deviceCategory'},
                            # Add or remove dimensions as per your requirements
                        ],
                        'pageSize': 20000  # Adjust the pageSize as needed
                    }
                ]
            }
        ).execute()

    

def response_to_dataframe(response):
    """Converts the API response into a pandas DataFrame."""
    # This function parses the response and converts it into a structured DataFrame
    list_rows = []
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])

        for row in report.get('data', {}).get('rows', []):
            dimensions = row.get('dimensions', [])
            dateRangeValues = row.get('metrics', [])

            row_data = {}
            for header, dimension in zip(dimensionHeaders, dimensions):
                row_data[header] = dimension

            for values in dateRangeValues:
                for metricHeader, value in zip(metricHeaders, values.get('values')):
                    row_data[metricHeader.get('name')] = value

            list_rows.append(row_data)

    return pd.DataFrame(list_rows)

def upload_to_bigquery(df, project_id, dataset_id, table_id):
    """Uploads the DataFrame to Google BigQuery."""
    # The DataFrame's column names are formatted for BigQuery compatibility
    df.columns = [col.replace('ga:', 'gs_') for col in df.columns]

    bigquery_client = bigquery.Client(project=project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    schema = []

    # Generating schema based on DataFrame columns
    for col in df.columns:
        dtype = df[col].dtype
        if pd.api.types.is_integer_dtype(dtype):
            bq_type = 'INTEGER'
        elif pd.api.types.is_float_dtype(dtype):
            bq_type = 'FLOAT'
        elif pd.api.types.is_bool_dtype(dtype):
            bq_type = 'BOOLEAN'
        else:
            bq_type = 'STRING'
        schema.append(bigquery.SchemaField(col, bq_type))

    try:
        bigquery_client.get_table(table_ref)
    except NotFound:
        # Creating a new table if it does not exist
        table = bigquery.Table(table_ref, schema=schema)
        bigquery_client.create_table(table)
        print(f"Created table {table_id}")

    # Loading data into BigQuery and confirming completion
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    load_job = bigquery_client.load_table_from_dataframe(df, table_ref)
    load_job.result()
    print(f"Data uploaded to {full_table_id}")

def main():
    """Main function to execute the script."""
    try:
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, goals=False)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, BIGQUERY_TABLE)
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, goals=True)
        df = response_to_dataframe(response)
        goals_table = BIGQUERY_TABLE + '_goals'
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, goals_table)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(f"Error occurred: {e}")

if __name__ == '__main__':
    main()  # Entry point of the script
