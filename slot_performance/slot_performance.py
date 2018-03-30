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


def get_data(client):
    accounts = get_accounts(client)
    report_downloader = client.GetReportDownloader(version='v201710')
    try:
        with open('raw_data_slot.csv','wb') as a:
            for entry in accounts['links']:
                client.client_customer_id= entry['clientCustomerId']
                report = {
                      'reportName': 'ADGROUP_PERFORMANCE',
                      'dateRangeType': 'LAST_7_DAYS',#'CUSTOM_DATE',
                      'reportType': 'ADGROUP_PERFORMANCE_REPORT',
                      'downloadFormat': 'CSV',
                      'selector': {
                          #'dateRange' : {'min': 20180219, 'max': 20180221},
                          'fields': ['Date','AccountDescriptiveName','CampaignName','AdGroupName', 'CampaignId', 'AdGroupId', 'Device', 'Impressions', 'Clicks', 'AveragePosition', 'Conversions', 'Cost', 'AccountCurrencyCode', 'Slot'],
                          'predicates': [
                              {
                                  'field': 'Impressions',
                                  'operator': 'GREATER_THAN',
                                  'values': '0'
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


def process_slot(x):
    if 'Top' in x:
        return 'Top'
    else:
        return 'Other'

def process_file():
    df = pd.read_csv('raw_data_slot.csv')
    df.reset_index(inplace=True)
    column_names = ['Date','AccountDescriptiveName','CampaignName','AdGroupName', 'CampaignId', 'AdGroupId', 'Device', 'Impressions', 'Clicks', 'AveragePosition', 'Conversions', 'Cost', 'AccountCurrencyCode', 'Slot']
    df.columns = column_names
    df['remove'] = df['Date'].apply(lambda x: remove_row(x))
    #removed unnecessary rows
    df.drop(df[df['remove'] == 'yes'].index, inplace=True)
    df['Cost'] = df['Cost'].apply(lambda x: float(x)/1000000)
    df['Impressions'] = df['Impressions'].apply(lambda x: int(x))
    df['Clicks'] = df['Clicks'].apply(lambda x: int(x))
    df['Conversions'] = df['Conversions'].apply(lambda x: float(x))
    df['AveragePosition'] = df['AveragePosition'].apply(lambda x: float(x))
    df.drop(['remove'], axis=1, inplace=True)
    df['engine'] = 'google'
    df['Slot'] = df['Slot'].apply(lambda x: process_slot(x))
    df['wPos'] = df['AveragePosition'] * df['Impressions']
    grouped = df.groupby(by=['Date', 'AccountDescriptiveName', 'CampaignName', 'AdGroupName', 'CampaignId', 'AdGroupId', 'Device', 'AccountCurrencyCode', 'engine', 'Slot'])
    grouped.sum().reset_index().to_csv('grouped.csv', index=False)
    df2 = pd.read_csv('grouped.csv')
    df2.drop(['AveragePosition'], axis=1, inplace=True)
    df2.to_csv('adgroup_slot_report.csv',index=False)



def output_to_db(postgres_path, db_password, db_table):
    try:
        df_report = pd.read_csv('adgroup_slot_report.csv', sep=',', delimiter=None, header=0)
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

    db_table = 'reports.sem_slot_raw_upload'   # 'reports.tmp_mt_paid_search_raw_upload'  # lookup_paid_search_raw_upload
    db_username = 'alfonso_fidalgo'
    db_password = keyring.get_password('db_postgres', db_username)

    postgres_path = psycopg2.connect(database='db_replica', user=db_username, password=db_password, host='localhost', port=15432)
    get_data(adwords_client)
    process_file()
    output_to_db(postgres_path, db_password, db_table )
