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
#import io

def get_accounts(client):
    managed_customer_service = client.GetService('ManagedCustomerService', version='v201710')
    selector = {
                'fields': ['Name','CustomerId'],
                'predicates': [
                    {
                        'field': 'Name',
                        'operator': 'DOES_NOT_CONTAIN_IGNORE_CASE',
                        'values': 'MCC'
                    },
                                        {
                        'field': 'Name',
                        'operator': 'CONTAINS_IGNORE_CASE',
                        'values': 'App Campaign'
                    }
                ]
    }
    accounts = managed_customer_service.get(selector)
    return accounts


def get_data(client):
    accounts = get_accounts(client)
    report_downloader = client.GetReportDownloader(version='v201710')
    try:
        with open('uac_raw_elements.csv','wb') as a:
            for entry in accounts['links']:
                client.client_customer_id= entry['clientCustomerId']
                report = {
                      'reportName': 'CAMPAIGN_PERFORMANCE',
                      'dateRangeType': 'LAST_14_DAYS',
                      'reportType': 'CAMPAIGN_PERFORMANCE_REPORT',
                      'downloadFormat': 'CSV',
                      'selector': {
                          'fields': ['AccountDescriptiveName','CampaignName','CampaignId'],
                          'predicates': [
                              {
                                  'field': 'Clicks',
                                  'operator': 'GREATER_THAN',
                                  'values': 0
                              }
                          ]
                       }
                }
                report_downloader.DownloadReport(report,a)

    #except Exception,e:
    except ValueError:
        print('error:')
        #print(e)


##FILE IS PROCESSED
def remove_row (r):
    try:
        if r in ['Account', 'Total'] :
            return 'yes'
        elif r.split(' ')[0] == 'CAMPAIGN_PERFORMANCE':
            return 'yes'
        else:
            return 'no'
    #except Exception,e:
    #    print(r)
    #    print(e)
    except ValueError:
        print('error')

def process_file():
    df = pd.read_csv('uac_raw_elements.csv')
    df.reset_index(inplace=True)
    column_names = ['AccountDescriptiveName','CampaignName', 'CampaignId']
    df.columns = column_names
    df['remove'] = df['AccountDescriptiveName'].apply(lambda x: remove_row(x))
    #removed unnecessary rows
    df.drop(df[df['remove'] == 'yes'].index, inplace=True)
    df.drop(['remove'], axis=1, inplace=True)
    df.to_csv('uac_elements.csv',index=False)



def output_to_db(postgres_path, db_password, db_table):
    try:
        df_report = pd.read_csv('uac_elements.csv', sep=',', delimiter=None, header=0)
        temp_csv = cStringIO.StringIO()
        #temp_csv = io.StringIO()
        df_report.to_csv(temp_csv, sep='\t', header=False, index=False)
        temp_csv.seek(0)
        cursor = postgres_path.cursor()
        cursor.copy_from(temp_csv, db_table, null='\\N')
        postgres_path.commit()
        cursor.close()
    #except Exception,e:
    #    print(e)
    except ValueError:
        print('error')

if __name__ == '__main__':    # Check that this whole script is actually being run directly (as the main script) Then run the below
    adwords_client = adwords.AdWordsClient.LoadFromStorage('googleads.yaml')

    db_table = 'reports.lookup_uac_elements'   # 'reports.tmp_mt_paid_search_raw_upload'  # lookup_paid_search_raw_upload
    db_password = keyring.get_password('DB_NAME', 'USER_NAME')

    postgres_path = psycopg2.connect(database='DB_NAME', user='USER_NAME', password=db_password, host='analyticsdb-fe.tw.ee', port=5432)
    get_data(adwords_client)
    process_file()
    output_to_db(postgres_path, db_password, db_table )
