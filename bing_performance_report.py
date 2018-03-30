#!/usr/bin/env python
import numpy as np
import pandas as pd
import codecs
import time
import os
import csv
import psycopg2   # For connection to postgres DB
from sqlalchemy import create_engine
import keyring
import cStringIO

from auth_helper import *
from output_helper import *

FILE_DIRECTORY='./'
DOWNLOAD_FILE_NAME='bing_api_report.csv'
REPORT_FILE_FORMAT='Csv'
TIMEOUT_IN_MILLISECONDS=3600000

def main(authorization_data):
    try:
        report_request=get_adgroup_performance_report_request()
        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            result_file_directory = FILE_DIRECTORY,
            result_file_name = DOWNLOAD_FILE_NAME,
            overwrite_result_file = True,
            timeout_in_milliseconds=TIMEOUT_IN_MILLISECONDS
        )
        output_status_message("Awaiting Background Completion . . .");
        background_completion(reporting_download_parameters)
        output_status_message("Program execution completed")

    except WebFault as ex:
        output_webfault_errors(ex)
    except Exception as ex:
        output_status_message(ex)

def background_completion(reporting_download_parameters):
    global reporting_service_manager
    result_file_path = reporting_service_manager.download_file(reporting_download_parameters)
    #output_status_message("Download result file: {0}\n".format(result_file_path))

def get_adgroup_performance_report_request():
    report_request=reporting_service.factory.create('AdGroupPerformanceReportRequest')
    report_request.Format=REPORT_FILE_FORMAT
    report_request.ReportName='Adgroup Performance Report'
    report_request.ReturnOnlyCompleteData=False
    report_request.Aggregation='Daily'
    report_request.Language='English'

    scope=reporting_service.factory.create('AccountThroughAdGroupReportScope')

    #scope.AccountIds={'long': [authorization_data.account_id] }
    scope.AccountIds={'long': ['2844570', '151000876', '151001021', '151001865', '151001864'] }
    scope.Campaigns=None
    scope.AdGroups=None
    report_request.Scope=scope

    report_time=reporting_service.factory.create('ReportTime')

    report_time.PredefinedTime='Yesterday'
    report_request.Time=report_time

    report_columns=reporting_service.factory.create('ArrayOfAdGroupPerformanceReportColumn')
    report_columns.AdGroupPerformanceReportColumn.append([
        'TimePeriod',
        'AccountName',
        'CampaignName',
        'AdGroupName',
        'CampaignId',
        'AdGroupId',
        'DeviceType',
        'Impressions',
        'Clicks',
        'AveragePosition',
        'Conversions',
        'Spend',
        'CurrencyCode'
    ])
    report_request.Columns=report_columns
    return report_request

def standarise_device(dev):
    if dev == 'Computer':
        return 'Computers'
    elif dev == 'Tablet':
        return 'Tablets with full browsers'
    elif dev == 'Smartphone':
        return 'Mobile devices with full browsers'
    else:
        return 'Other'

def process_file():
    df = pd.read_csv('bing_api_report.csv', sep=',', header=9)
    df.dropna(inplace=True)
    df['engine'] = 'bing'
    df['DeviceType'] = df['DeviceType'].apply(lambda x: standarise_device(x))
    df['Impressions'] = df['Impressions'].apply(lambda x: int(x))
    df['Clicks'] = df['Clicks'].apply(lambda x: int(x))
    df['Conversions'] = df['Conversions'].apply(lambda x: int(x))
    df['CampaignId'] = df['CampaignId'].apply(lambda x: int(x))
    df['AdGroupId'] = df['AdGroupId'].apply(lambda x: int(x))
    df2 = df[df['Impressions'] > 0]
    df2.to_csv('bing_api_report_processed.csv',index=False)

def output_to_db(postgres_path, db_password, db_table):
    try:
        df_report = pd.read_csv('bing_api_report_processed.csv', sep=',', delimiter=None, header=0)
        temp_csv = cStringIO.StringIO()
        df_report.to_csv(temp_csv, sep='\t', header=False, index=False)
        temp_csv.seek(0)
        cursor = postgres_path.cursor()
        cursor.copy_from(temp_csv, db_table, null='\\N')
        postgres_path.commit()
        cursor.close()
    except Exception,e:
        print(e)

# Main execution
if __name__ == '__main__':
    authorization_data=AuthorizationData(
        account_id=None,
        customer_id=None,
        developer_token=DEVELOPER_TOKEN,
        authentication=None,
    )
    reporting_service_manager=ReportingServiceManager(
        authorization_data=authorization_data,
        poll_interval_in_milliseconds=5000,
        environment=ENVIRONMENT,
    )
    reporting_service=ServiceClient(
        'ReportingService',
        authorization_data=authorization_data,
        environment=ENVIRONMENT,
        version=11,
    )
    authenticate(authorization_data)
    main(authorization_data)
    process_file()

    db_table = 'reports.sem_adgroup_raw_upload'   # 'reports.tmp_mt_paid_search_raw_upload'  # lookup_paid_search_raw_upload
    db_password = keyring.get_password('DB_NAME', 'USER')
    postgres_path = psycopg2.connect(database='DB_NAME', user='USER', password=db_password, host='HOST', port=1234)
    output_to_db(postgres_path, db_password, db_table)
