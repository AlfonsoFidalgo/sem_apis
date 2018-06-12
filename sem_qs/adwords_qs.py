#!/usr/bin/env python

from datetime import datetime, timedelta
from googleads import adwords
import codecs
import time
import os
import csv
import psycopg2   # For connection to postgres DB
import pandas as pd
from sqlalchemy import create_engine
import keyring
import cStringIO

def get_accounts(client):
    managed_customer_service = client.GetService('ManagedCustomerService', version='v201710')
    selector = {
                'fields': ['Name','CustomerId'],
                'predicates': [
                    {
                        'field': 'Name',
                        'operator': 'DOES_NOT_CONTAIN_IGNORE_CASE',
                        'values': 'MCC'
                    }
                ]
    }
    accounts = managed_customer_service.get(selector)
    return accounts

columns = ['Date','AccountDescriptiveName','CampaignName','AdGroupName', 'Criteria', 'CampaignId', 'AdGroupId', 'Device', 'Impressions', 'QualityScore']
def get_data(client):
    accounts = get_accounts(client)
    report_downloader = client.GetReportDownloader(version='v201710')
    try:
        with open('qs_raw_data.csv','wb') as a:
            for entry in accounts['links']:
                client.client_customer_id= entry['clientCustomerId']
                report = {
                      'reportName': 'KEYWORDS_PERFORMANCE_REPORT',
                      'dateRangeType': 'LAST_7_DAYS',#'CUSTOM_DATE',
                      #'dateRangeType': 'CUSTOM_DATE',
                      'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
                      'downloadFormat': 'CSV',
                      'selector': {
                          #'dateRange' : {'min': 20180301, 'max': 20180327},
                          'fields': columns,
                          'predicates': [
                              {
                                  'field': 'Impressions',
                                  'operator': 'GREATER_THAN', #GREATER_THAN
                                  'values': '0'
                              },
                              {
                                  'field': 'HasQualityScore',
                                  'operator': 'EQUALS',
                                  'values': 'True'
                              }
                          ]
                       },

                }
                report_downloader.DownloadReport(report,a)

    except Exception,e:
        print('error:')
        print(e)


##FILE IS PROCESSED
def remove_row (r):
    try:
        if len(str(r)) != 10:
            return 'yes'
        else:
            return 'no'
    except Exception,e:
        print(r)
        print(e)



def process_file():
    df = pd.read_csv('qs_raw_data.csv')
    df.reset_index(inplace=True)
    columns = ['Date','AccountDescriptiveName','CampaignName','AdGroupName', 'Criteria', 'CampaignId', 'AdGroupId', 'Device', 'Impressions', 'QualityScore']
    df.columns = columns
    df['remove'] = df['Date'].apply(lambda x: remove_row(x))
    #removed unnecessary rows
    df.drop(df[df['remove'] == 'yes'].index, inplace=True)
    df.drop(['remove'], axis=1, inplace=True)
    df['Impressions'] = df['Impressions'].apply(lambda x: int(x))
    df['QualityScore'] = df['QualityScore'].apply(lambda x: int(x))
    df['wQS'] = df['Impressions'] * df['QualityScore']
    df['engine'] = 'google'
    df.drop(['QualityScore'], axis=1, inplace=True)
    df.drop(['Criteria'], axis=1, inplace=True)
    #df.drop(['CampaignId', 'AdGroupId'], axis=1, inplace=True)
    df2 = df.groupby(by=['Date','AccountDescriptiveName','CampaignName','AdGroupName', 'CampaignId', 'AdGroupId', 'Device', 'engine']).sum()#.reset_index(inplace=True)
    df2.reset_index().to_csv('qs_report.csv',index=False)


def output_to_db(postgres_path, db_password, db_table):
    try:
        df_report = pd.read_csv('qs_report.csv', sep=',', delimiter=None, header=0)
        temp_csv = cStringIO.StringIO()
        df_report.to_csv(temp_csv, sep='\t', header=False, index=False)
        temp_csv.seek(0)
        cursor = postgres_path.cursor()
        cursor.copy_from(temp_csv, db_table, null='\\N')
        postgres_path.commit()
        cursor.close()
    except Exception,e:
        print(e)


if __name__ == '__main__':    # Check that this whole script is actually being run directly (as the main script) Then run the below
    adwords_client = adwords.AdWordsClient.LoadFromStorage('googleads.yaml')

    db_table = 'reports.sem_qs_upload'   # 'reports.tmp_mt_paid_search_raw_upload'  # lookup_paid_search_raw_upload
    db_password = keyring.get_password('db_postgres', 'usr_stats')

    postgres_path = psycopg2.connect(database='db_replica', user=db_username, password=db_password, host='analyticsdb.tw.ee', port=5432)
    get_data(adwords_client)
    process_file()
    output_to_db(postgres_path, db_password, db_table )
