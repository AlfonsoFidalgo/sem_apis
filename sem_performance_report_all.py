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

def get_accounts_uac(client):
    managed_customer_service = client.GetService('ManagedCustomerService', version='v201710')
    selector = {
                'fields': ['Name','CustomerId'],
                'predicates': [
                    {
                        'field': 'Name',
                        'operator': 'CONTAINS_IGNORE_CASE',
                        'values': 'Universal App Campaign'
                    }
                ]
    }
    accounts = managed_customer_service.get(selector)
    return accounts



def get_data(client):
    accounts = get_accounts(client)
    report_downloader = client.GetReportDownloader(version='v201710')
    try:
        with open('raw_data.csv','wb') as a:
            for entry in accounts['links']:
                client.client_customer_id= entry['clientCustomerId']
                report = {
                      'reportName': 'ADGROUP_PERFORMANCE',
                      'dateRangeType': 'LAST_7_DAYS',
                      'reportType': 'ADGROUP_PERFORMANCE_REPORT',
                      'downloadFormat': 'CSV',
                      'selector': {
                          'fields': ['Date','AccountDescriptiveName','CampaignName','AdGroupName', 'CampaignId', 'AdGroupId', 'Device', 'Impressions', 'Clicks', 'AveragePosition', 'Conversions', 'Cost', 'AccountCurrencyCode'],
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

def get_data_uac(client):
    accounts = get_accounts(client)
    report_downloader = client.GetReportDownloader(version='v201710')
    try:
        with open('uac_data.csv','wb') as a:
            for entry in accounts['links']:
                client.client_customer_id= entry['clientCustomerId']
                report = {
                      'reportName': 'CAMPAIGN_PERFORMANCE',
                      'dateRangeType': 'LAST_7_DAYS',
                      'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
                      'downloadFormat': 'CSV',
                      'selector': {
                          'fields': ['Date','AccountDescriptiveName','CampaignName', 'CampaignId', 'Device', 'Impressions', 'Clicks', 'AveragePosition', 'Conversions', 'Cost', 'AccountCurrencyCode'],
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

def process_file():
    df = pd.read_csv('raw_data.csv')
    df.reset_index(inplace=True)
    column_names = ['Date','AccountDescriptiveName','CampaignName','AdGroupName', 'CampaignId', 'AdGroupId', 'Device', 'Impressions', 'Clicks', 'AveragePosition', 'Conversions', 'Cost', 'AccountCurrencyCode']
    df.columns = column_names
    df['remove'] = df['Date'].apply(lambda x: remove_row(x))
    #removed unnecessary rows
    df.drop(df[df['remove'] == 'yes'].index, inplace=True)
    df['Cost'] = df['Cost'].apply(lambda x: float(x)/1000000)
    df.drop(['remove'], axis=1, inplace=True)
    df.to_csv('adgroup_performance_report.csv',index=False)

def process_file_uac():
    df = pd.read_csv('uac_data.csv')
    df.reset_index(inplace=True)
    column_names = ['Date','AccountDescriptiveName','CampaignName', 'CampaignId', 'Device', 'Impressions', 'Clicks', 'AveragePosition', 'Conversions', 'Cost', 'AccountCurrencyCode']
    df.columns = column_names
    df['remove'] = df['Date'].apply(lambda x: remove_row(x))
    #removed unnecessary rows
    df.drop(df[df['remove'] == 'yes'].index, inplace=True)
    df['Cost'] = df['Cost'].apply(lambda x: float(x)/1000000)
    df.drop(['remove'], axis=1, inplace=True)
    df['engine'] = 'google'
    df['AdgroupId'] = 0
    df['AdgroupName'] = ""
    columns = df.columns.tolist()
    df = df[columns[0:3] + columns[-1:] + columns[3:4] + columns[12:13] + columns[4:-2]]
    df.to_csv('uac_report.csv',index=False)


def output_to_db(postgres_path, db_password, db_table):
    try:
        df_report = pd.read_csv('adgroup_performance_report.csv', sep=',', delimiter=None, header=0)
        temp_csv = cStringIO.StringIO()
        df_report.to_csv(temp_csv, sep='\t', header=False, index=False)
        temp_csv.seek(0)
        cursor = postgres_path.cursor()
        cursor.copy_from(temp_csv, db_table, null='\\N')
        postgres_path.commit()
        cursor.close()
    except Exception,e:
        print(e)

def output_to_db_uac(postgres_path, db_password, db_table):
    try:
        df_report = pd.read_csv('uac_report.csv', sep=',', delimiter=None, header=0)
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
    adwords_client = adwords.AdWordsClient.LoadFromStorage('./googleads.yaml')

    db_table = 'reports.sem_adgroup_raw_upload'   # 'reports.tmp_mt_paid_search_raw_upload'  # lookup_paid_search_raw_upload
    db_password = keyring.get_password('DB_NAME', 'USER')

    postgres_path = psycopg2.connect(database='DB_NAME', user='USER', password=db_password, host='HOST', port=1234)
    get_data(adwords_client)
    get_data_uac(adwords_client)
    process_file()
    process_file_uac()
    output_to_db(postgres_path, db_password, db_table )
    output_to_db_uac(postgres_path, db_password, db_table )
