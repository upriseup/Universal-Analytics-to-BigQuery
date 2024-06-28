from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd
import os

# Configuration variables for Google Analytics and BigQuery
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']

POSTAL_KEY_OLD = '../keys/UA Data Storage Keys/Postal Museum/oceanic-cache-426909-q9-fb060f06ea89.json'
POSTAL_KEY = '../keys/UA Data Storage Keys/Postal Backup Key/ua-data-storage-postal-cd258757b593.json'
NHSVR_KEY = '../keys/UA Data Storage Keys/NHSVR/ua-storage-426914-b77531a2d41b.json'
RVS_KEY = '../keys/UA Data Storage Keys/RVS/bubbly-fuze-426813-p9-371ff48952a4.json'
URU_FILE_LOCATION = '../keys/gtm-w6kpsfd7-yjbhm-5808ebc38263.json' 
 # Path to your Google Cloud service account key file

URU_REPORTING_VIEW = '79428303'
URU_MAIN_VIEW = '151196979'#
POSTAL_VIEW = '132787952'
RVS_VIEW_OLD = '107499264'
RVS_VIEW = '3658780'
NHSVR_VIEW = '225376456'
  # Your Google Analytics View ID

POSTAL_PROJECT_OLD = 'oceanic-cache-426909-q9'
POSTAL_PROJECT = 'ua-data-storage-postal'
RVS_PROJECT = 'bubbly-fuze-426813-p9'
NHSVS_PROJECT = 'ua-storage-426914'
URU_PROJECT = 'gtm-w6kpsfd7-yjbhm'  # Your Google Cloud Project ID

URU_DATE_RANGE = [{'startDate': '2020-07-01', 'endDate': '2020-07-31'}]

POSTAL_DATE_RANGE = [{'startDate': '2016-11-01', 'endDate': '2023-08-26'}]

POSTAL_DATE_RANGE_1 = [{'startDate': '2016-11-01', 'endDate': '2019-12-31'}]
POSTAL_DATE_RANGE_2 = [{'startDate': '2020-01-01', 'endDate': '2021-12-31'}]
POSTAL_DATE_RANGE_3 = [{'startDate': '2022-01-01', 'endDate': '2023-08-26'}]

DATE_2017 = [{'startDate': '2017-01-01', 'endDate': '2017-12-31'}]
DATE_2017_Q1_Q2 = [{'startDate': '2017-01-01', 'endDate': '2017-06-30'}]
DATE_2017_Q3_Q4 = [{'startDate': '2017-07-01', 'endDate': '2017-12-31'}]

DATE_2015_RVS = [{'startDate': '2015-01-01', 'endDate': '2015-12-31'}]
DATE_2016 = [{'startDate': '2016-01-01', 'endDate': '2016-12-31'}]
DATE_2017 = [{'startDate': '2017-01-01', 'endDate': '2017-12-31'}]
DATE_2018 = [{'startDate': '2018-01-01', 'endDate': '2018-12-31'}]
DATE_2019 = [{'startDate': '2019-01-01', 'endDate': '2019-12-31'}]
DATE_2020 = [{'startDate': '2020-01-01', 'endDate': '2020-12-31'}]
DATE_2021 = [{'startDate': '2021-01-01', 'endDate': '2021-12-31'}]
DATE_2022 = [{'startDate': '2022-01-01', 'endDate': '2022-12-31'}]
DATE_2022_Q1Q2 = [{'startDate': '2022-01-01', 'endDate': '2022-06-30'}]
DATE_2022_Q3Q4 = [{'startDate': '2022-07-01', 'endDate': '2022-12-31'}]
DATE_2023 = [{'startDate': '2023-01-01', 'endDate': '2023-12-31'}]

#RVS_DATE_RANGE = [{'startDate': '2015-08-25', 'endDate': '2023-10-04'}]
RVS_NEW_DATE_RANGE = [{'startDate': '2007-01-01', 'endDate': '2023-10-04'}]
RVS_NEW_DATE_RANGE_1 = [{'startDate': '2007-01-01', 'endDate': '2018-12-31'}]
RVS_NEW_DATE_RANGE_2 = [{'startDate': '2019-01-01', 'endDate': '2023-10-04'}]

# NHSVR_DATE_RANGE = [{'startDate': '2016-11-01', 'endDate': '2023-08-26'}]

# KEY_FILE_LOCATION = RVS_KEY
# DATE_RANGE = RVS_DATE_RANGE
# VIEW_ID = RVS_VIEW
# BIGQUERY_PROJECT = RVS_PROJECT

# KEY_FILE_LOCATION = NHSVR_KEY
# DATE_RANGE = NHSVR_DATE_RANGE
# VIEW_ID = NHSVR_VIEW
# BIGQUERY_PROJECT = NHSVS_PROJECT

KEY_FILE_LOCATION = RVS_KEY
#DATE_RANGE = DATE_2017_Q1_Q2
DATE_RANGE = RVS_NEW_DATE_RANGE#DATE_2023 #POSTAL_DATE_RANGE iterate through all years, two halfs for 2022
startYear = DATE_RANGE[0].get('startDate')[:4]
endYear = DATE_RANGE[0].get('endDate')[:4]
startDate = DATE_RANGE[0].get('startDate')[:4] + " Q1 Q2"
endDate = DATE_RANGE[0].get('endDate')[:4] + " Q3 Q4"

VIEW_ID = RVS_VIEW
BIGQUERY_PROJECT = RVS_PROJECT

# KEY_FILE_LOCATION = URU_FILE_LOCATION
# DATE_RANGE = URU_DATE_RANGE
# VIEW_ID = URU_REPORTING_VIEW
# BIGQUERY_PROJECT = URU_PROJECT

BIGQUERY_DATASET = 'ua_data_storage'  # BigQuery Dataset name where the data will be stored
# BIGQUERY_TABLE = 'reports'  # BigQuery Table name where the data will be stored

NUMBER_OF_GOALS = 10
# Setting up the environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = KEY_FILE_LOCATION
metrics_goals_1 = [{'expression': f'ga:goal{i+1}Completions'} for i in range(NUMBER_OF_GOALS)]
metrics_goals_2 = [{'expression': f'ga:goal{i+11}Completions'} for i in range(NUMBER_OF_GOALS)]
#        print('\n',metrics_goals_1, '\n','\n', metrics_goals_2,'\n')

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
                    'dateRanges': DATE_RANGE,
                    # Metrics and dimensions are specified here
                    'metrics': metrics,
                    'dimensions': dimensions,
                    'pageSize': 200000  # Adjust the pageSize as needed
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
    # try:
    #     tableName = "Acquisition Overview"# +" "+ startYear 
    #     dimensions = [
    #         {'name': 'ga:date'},
    #         {'name': 'ga:campaign'},
    #         {'name': 'ga:source'},
    #         {'name': 'ga:medium'},
    #     ]
    #     metrics = [
    #         {'expression': 'ga:sessions'},
    #         {'expression': 'ga:users'},
    #         {'expression': 'ga:newUsers'},
    #         {'expression': 'ga:bounceRate'},
    #         {'expression': 'ga:pageviews'},
    #         {'expression': 'ga:avgSessionDuration'},
    #     ]
    #     analytics = initialize_analyticsreporting()
    #     response = get_report(analytics, dimensions, metrics)
    #     df = response_to_dataframe(response)
    #     upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    #     # goals bit
    #     # tableName = tableName + " Goals"
    #     # analytics = initialize_analyticsreporting()
    #     # response = get_report(analytics, dimensions, metrics_goals_1)
    #     # df = response_to_dataframe(response)
    #     # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    #     # tableName = tableName + " 2"
    #     # analytics = initialize_analyticsreporting()
    #     # response = get_report(analytics, dimensions, metrics_goals_2)
    #     # df = response_to_dataframe(response)
    #     # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    # except Exception as e:
    #     # Handling exceptions and printing error messages
    #     print(tableName)
    #     print(f"Error occurred: {e}")

    try:
        tableName = "Audience Demographics Overview"#  +" "+ startYear 
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
        # tableName = tableName + " Goals"
        # analytics = initialize_analyticsreporting()
        # response = get_report(analytics, dimensions, metrics_goals_1)
        # df = response_to_dataframe(response)
        # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
        # tableName = tableName + " 2"
        # analytics = initialize_analyticsreporting()
        # response = get_report(analytics, dimensions, metrics_goals_2)
        # df = response_to_dataframe(response)
        # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(tableName)
        print(f"Error occurred: {e}")

    # try:
    #     tableName = "Geographic Distribution"#+" "+ startYear 
    #     dimensions = [
    #         {'name': 'ga:yearWeek'},
    #         {'name': 'ga:country'},
    #         {'name': 'ga:city'},
    #     ]
    #     metrics = [
    #         {'expression': 'ga:sessions'},
    #         {'expression': 'ga:users'},
    #         {'expression': 'ga:newUsers'},
    #         {'expression': 'ga:bounceRate'},
    #         {'expression': 'ga:pageviews'},
    #         {'expression': 'ga:avgSessionDuration'},
    #     ]
    #     analytics = initialize_analyticsreporting()
    #     response = get_report(analytics, dimensions, metrics)
    #     df = response_to_dataframe(response)
    #     upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    #     # tableName = tableName + " Goals"
    #     # analytics = initialize_analyticsreporting()
    #     # response = get_report(analytics, dimensions, metrics_goals_1)
    #     # df = response_to_dataframe(response)
    #     # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    #     # tableName = tableName + " 2"
    #     # analytics = initialize_analyticsreporting()
    #     # response = get_report(analytics, dimensions, metrics_goals_2)
    #     # df = response_to_dataframe(response)
    #     # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    # except Exception as e:
    #     # Handling exceptions and printing error messages
    #     print(tableName)
    #     print(f"Error occurred: {e}")
    #     print(" ")

    try:
        tableName="Device and Technology Usage"#  +" "+ startYear 
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
        # tableName = tableName + " Goals"
        # analytics = initialize_analyticsreporting()
        # response = get_report(analytics, dimensions, metrics_goals_1)
        # df = response_to_dataframe(response)
        # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
        # tableName = tableName + " 2"
        # analytics = initialize_analyticsreporting()
        # response = get_report(analytics, dimensions, metrics_goals_2)
        # df = response_to_dataframe(response)
        # upload_to_bigquery(df, BIGQUERY_PROJECT, BIGQUERY_DATASET, tableName)
    except Exception as e:
        # Handling exceptions and printing error messages
        print(tableName)
        print(f"Error occurred: {e}")
        print()

    try:
        tableName="Site Content Performance Overview"# +" "+ startYear 
        dimensions = [
            # {'name': 'ga:yearMonth'},
            {'name': 'ga:yearWeek'},
            {'name': 'ga:pageTitle'},
            #{'name': 'ga:date'}, # hits row limit for half a year
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

    try:
        tableName = "Transaction History"  # + " " + startYear
        dimensions = [
            {'name': 'ga:date'},
            # {'name': 'ga:productName'},
            {'name': 'ga:transactionId'},
            # {'name': 'ga:productCategory'},
            # {'name': 'ga:productSku'},
            {'name': 'ga:daysToTransaction'},
        ]
        metrics = [
            {'expression': 'ga:transactions'},
            # {'expression': 'ga:itemQuantity'},
            # {'expression': 'ga:revenuePerItem'},
            {'expression': 'ga:totalValue'},
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

    try:
        tableName = "Product Performance"  # + " " + startYear
        dimensions = [
            {'name': 'ga:date'},
            {'name': 'ga:productName'},
            {'name': 'ga:productCategory'},
            {'name': 'ga:productSku'},
            #{'name': 'ga:transactionId'},
            #{'name': 'ga:productCategory'},
            #{'name': 'ga:productSku'},
            #{'name': 'ga:daysToTransaction'},
        ]
        metrics = [
            {'expression': 'ga:itemRevenue'},
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

    try:
        tableName = "Ecommerce Performance Overview"  # + " " + startYear
        dimensions = [
            {'name': 'ga:date'},
            # {'name': 'ga:productName'},
            # {'name': 'ga:productCategory'},
            # {'name': 'ga:productSku'},
            # {'name': 'ga:transactionId'},
            # {'name': 'ga:productCategory'},
            # {'name': 'ga:productSku'},
            # {'name': 'ga:daysToTransaction'},
        ]
        metrics = [
            {'expression': 'ga:itemRevenue'},
            {'expression': 'ga:transactionsPerSession'},
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
        
    try:
        tableName = "User Engagement Over Time"  # + " " + startYear
        dimensions = [
            {'name': 'ga:date'},
            {'name': 'ga:hour'},
            {'name': 'ga:dayOfWeek'},
        ]
        metrics = [
            {'expression': 'ga:users'},
            {'expression': 'ga:sessions'},
            {'expression': 'ga:sessionDuration'},
            {'expression': 'ga:avgSessionDuration'},
            {'expression': 'ga:avgSessionDuration'},
            {'expression': 'ga:bounceRate'},
            {'expression': 'ga:pageviews'},
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

    try:
        tableName = "Audience Loyalty and Retention"  # + " " + startYear
        dimensions = [
            {'name': 'ga:userType'},
            {'name': 'ga:sessionCount'},
            {'name': 'ga:daysSinceLastSession'},
        ]
        metrics = [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:pageviews'},
            {'expression': 'ga:avgSessionDuration'},
            {'expression': 'ga:bounceRate'},
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
