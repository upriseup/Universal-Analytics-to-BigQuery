from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import os

# Configuration variables for Google Analytics and BigQuery
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

POSTAL_KEY = '../keys/UA Data Storage Keys/Postal Museum/oceanic-cache-426909-q9-fb060f06ea89.json'
NHSVR_KEY = '../keys/UA Data Storage Keys/NHSVR/ua-storage-426914-b77531a2d41b.json'
RVS_KEY = '../keys/UA Data Storage Keys/RVS/bubbly-fuze-426813-p9-371ff48952a4.json'
URU_FILE_LOCATION = '../keys/gtm-w6kpsfd7-yjbhm-5808ebc38263.json' 
 # Path to your Google Cloud service account key file
KEY_FILE_LOCATION = RVS_KEY


URU_REPORTING_VIEW = '79428303'
URU_MAIN_VIEW = '151196979'#
POSTAL_VIEW = '132787952'
RVS_VIEW = '107499264'
NHSVR_VIEW = '225376456'
VIEW_ID = RVS_VIEW  # Your Google Analytics View ID

POSTAL_PROJECT = 'oceanic-cache-426909-q9'
RVS_PROJECT = 'bubbly-fuze-426813-p9'
NHSVS_PROJECT = 'ua-storage-426914'
URU_PROJECT = 'gtm-w6kpsfd7-yjbhm'  # Your Google Cloud Project ID

BIGQUERY_PROJECT = RVS_PROJECT
BIGQUERY_DATASET = 'ua_storage'  # BigQuery Dataset name where the data will be stored
BIGQUERY_TABLE = 'reports'  # BigQuery Table name where the data will be stored

POSTAL_DATE_RANGE = [{'startDate': '2016-11-01', 'endDate': '2023-08-26'}]
RVS_DATE_RANGE = [{'startDate': '2015-08-25', 'endDate': '2023-10-04'}]
NHSVR_DATE_RANGE = [{'startDate': '2016-11-01', 'endDate': '2023-08-26'}]


NUMBER_OF_GOALS = 10
# Setting up the environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE_LOCATION
metrics_goals_1 = [{'expression': f'ga:goal{i+1}Completions'} for i in range(NUMBER_OF_GOALS)]
#metrics_goals_2 = [{'expression': f'ga:goal{i+11}Completions'} for i in range(NUMBER_OF_GOALS)]

def initialize_analyticsreporting():
    """Initializes the Google Analytics Reporting API client."""
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        KEY_FILE_LOCATION, SCOPES)
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics

def get_report(analytics, dimensions, metrics):
    """Fetches the report data from Google Analytics."""
    # Here, specify the analytics report request details
    return analytics.reports().batchGet(
        body={
            'reportRequests': [
                {
                    'viewId': VIEW_ID,
                    'dateRanges': RVS_DATE_RANGE,
                    # Metrics and dimensions are specified here
                    'metrics': metrics,
                    'dimensions': dimensions,
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

# def metrics_goals(numberOfGoals):
#     metrics_goals_1 = [{'expression': f'ga:goal{i+1}Completions'} for i in range(numberOfGoals)]
#     NUMBER_OF_GOALS_2 = 10
#     metrics_goals_2 = [{'expression': f'ga:goal{i+11}Completions'} for i in range(NUMBER_OF_GOALS_2)]
#     #NUMBER_OF_GOALS_2 = 10
#         #metrics_goals_2 = [{'expression': f'ga:goal{i+11}Completions'} for i in range(NUMBER_OF_GOALS_2)]

def main():
    """Main function to execute the script."""
    try:
        tableName = "Acquisition Overview"
        dimensions = [
            {'name': 'ga:date'},
            {'name': 'ga:campaign'},
            {'name': 'ga:source'},
            {'name': 'ga:medium'},
        ]
        metrics = [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:users'},
            {'expression': 'ga:newUsers'},
            {'expression': 'ga:bounceRate'},
            {'expression': 'ga:pageviews'},
            {'expression': 'ga:avgSessionDuration'},
        ]
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
        tableName = tableName + " Goals"
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics_goals_1)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(tableName)
        print(f"Error occurred: {e}")

    try:
        tableName = "Audience Demographics Overview"
        dimensions = [
            {'name': 'ga:date'},
            {'name': 'ga:userGender'},
            {'name': 'ga:userAgeBracket'},
            # {'name': 'ga:sourceMedium'}, # unknown schema with this
        ]
        metrics = [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:users'},
            {'expression': 'ga:newUsers'},
            {'expression': 'ga:bounceRate'},
            {'expression': 'ga:pageviews'},
            {'expression': 'ga:avgSessionDuration'},
        ]
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
        tableName = tableName + " Goals"
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics_goals_1)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(tableName)
        print(f"Error occurred: {e}")

    try:
        tableName = "Geographic Distribution" 
        dimensions = [
            {'name': 'ga:date'},
            {'name': 'ga:country'},
            {'name': 'ga:city'},
        ]
        metrics = [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:users'},
            {'expression': 'ga:newUsers'},
            {'expression': 'ga:bounceRate'},
            {'expression': 'ga:pageviews'},
            {'expression': 'ga:avgSessionDuration'},
        ]
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
        tableName = tableName + " Goals"
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics_goals_1)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(tableName)
        print(f"Error occurred: {e}")
        print(" ")

    try:
        tableName="Device and Technology Usage" 
        dimensions = [
            {'name': 'ga:date'},
            {'name': 'ga:deviceCategory'},
            #{'name': 'ga:browser'}, # conficts with devicecat
            # {'name': 'ga:operatingSystem '},Unknown dimension(s): ga:operatingSystem
        ]
        metrics = [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:users'},
            {'expression': 'ga:pageviews'},
        ]
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
        tableName = tableName + " Goals"
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics_goals_1)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(tableName)
        print(f"Error occurred: {e}")
        print()

    try:
        tableName="Site Content Performance Overview"  
        dimensions = [
            {'name': 'ga:date'},
            {'name': 'ga:pagePath'},
            #{'name': 'ga:sourceMedium'},
        ]
        metrics = [
            {'expression': 'ga:pageviews'},
            {'expression': 'ga:uniquePageviews'},
            {'expression': 'ga:avgTimeOnPage'},
            {'expression': 'ga:bounceRate'},
            {'expression': 'ga:exitRate'},
        ]
        analytics = initialize_analyticsreporting()
        response = get_report(analytics, dimensions, metrics)
        df = response_to_dataframe(response)
        upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    except Exception as e:
        # Handling exceptions and printing error messages
        print()
        print("error in: " + tableName)
        print(f"Error occurred: {e}")
        print()
    
if __name__ == '__main__':
    main()  # Entry point of the script
