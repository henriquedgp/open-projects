# Importing libraries
import datetime as dt
import dotenv
import logging
import os
import requests
import pandas as pd
import pandas_gbq 
import tqdm
from google.cloud import bigquery
from datetime import date, timedelta

# Credentials

dotenv.load_dotenv()
api_token = os.getenv("api_token")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'keys.json'

# Getting total of automations available through meta and calculating necessary requests with offset.

base_url = 'https://vincishoes.api-us1.com'

def qty_automations():

    headers = {
        "accept": "application/json",
        "Api-Token": f'{api_token}'
    }

    r = requests.get(f'{base_url}/api/3/automations', headers=headers)
    data = r.json()
    total_automations = int(data['meta']['total'])
    qty_requests = int((total_automations/100)+1)

    return qty_requests   

# Using qty_requests with variable to get all data.
def list_automations():
    list_automations = []
    
    for i in range(qty_automations()):
        limit = 100 # Maximum per request
        offset = str(i*100)

        headers = {
            "accept": "application/json",
            "Api-Token": f'{api_token}'
        }

        params = {
            "limit": f'{limit}',
            "offset": f'{offset}'
        }

        r = requests.get(f'{base_url}/api/3/automations', headers=headers, params=params)
        data = r.json()
        data = data['automations']
        list_automations.extend(data)

    return list_automations

automations_ids = pd.DataFrame(list_automations())

# Now to be able to get performance metrics from automations, I need a list of the email campaigns from those automations.

def get_campaigns_ids(list_automations):
    report = []

    headers = {
        "accept": "application/json",
        "Api-Token": f'{api_token}'
    }

    for id in tqdm.tqdm(automations_ids['id'], desc="Pulling data"):

        params = {
            "filters[seriesid]": f'{id}'
        }

        r = requests.get(f'{base_url}/api/3/campaigns', headers=headers, params=params)
        data = r.json()
        meta = data['meta']['total']

        # Log automations without response from the API
        if r.status_code != 200:
            logging.info(f'Had problem pulling data from automation {id}.\nError: {r.status_code})
            continue
        
        # Won't pull data from automations without email campaigns
        if meta == '0':
            continue

        else:
            data = r.json()
            data = data['campaigns']
            report.extend(data)

    return report
        

list_campaigns = pd.DataFrame(get_campaign_ids(automations_ids))

# Now that I have the campaign IDs, its time to pull performance data.

def get_performance_data(list_campaigns):
    report = []

    headers = {
        "accept": "application/json",
        "Api-Token": f'{api_token}'
    }

    for id in tqdm.tqdm(list_campaigns['id'], desc="Pulling data"):

        r = requests.get(f'{base_url}/api/3/campaigns/{id}', headers=headers)
        
        if r.status_code != 200:
            continue

        else:
            data = r.json()
            data = data['campaign']
            report.append(data)

    return report
        
automations_performance_report = pd.DataFrame(get_performance_data(list_campaigns))

# Transforming data
## Select only the variables needed for each observation
df_report = automations_performance_report[['id', 'seriesid', 'name', 'send_amt', 'verified_opens', 'verified_unique_opens', 'linkclicks', 'subscriberclicks', 'unsubscribes', 'hardbounces', 'softbounces']]

# Add extraction_date and insert it as the first column
extract_date = date.today() - timedelta(days=1)
df_report.insert(0, 'extract_date', extract_date)

# Rename columns for better understanding of people outside the project on BigQuery
df_report = df_report.rename(columns={"id": "campaign_id", "seriesid": "automation_id", "name": "campaign_name", "send_amt": "qty_sent", "verified_opens": "qty_open", "verified_unique_opens": "qty_unique_open", "linkclicks": "qty_clicks", "subscriberclicks": "qty_unique_clicks", "unsubscribes": "qty_unsubscribes", "hardbounces": "qty_hardbounces", "softbounces": "qty_softbounces"})

# Transform dtypes

df_report = df_report.astype({
    'extract_date': 'datetime64[ns]',
    'campaign_id': 'str',
    'automation_id': 'str',
    'campaign_name': 'str',
    'qty_sent': 'int64',
    'qty_open': 'int64',
    'qty_unique_open': 'int64',
    'qty_clicks': 'int64',
    'qty_unique_clicks': 'int64',
    'qty_unsubscribes': 'int64',
    'qty_hardbounces': 'int64',
    'qty_softbounces': 'int64'
})

# Loading data into Google BigQuery

dataset_id = '{YOUR_DATASET}'
table_id = '{YOUR_TABLE_NAME}'
project_id = '{YOUR_PROJECT_ID}'

pandas_gbq.to_gbq(df_report,
                f'{dataset_id}.{table_id}',
                project_id=project_id,
                if_exists='append',
                table_schema = [{'name': 'extract_date', 'type': 'DATE'},
                                {'name': 'campaign_id', 'type': 'STRING'},
                                {'name': 'automation_id', 'type': 'STRING'},
                                {'name': 'campaign_name', 'type': 'STRING'},
                                {'name': 'qty_sent', 'type': 'INTEGER'},
                                {'name': 'qty_open', 'type': 'INTEGER'},
                                {'name': 'qty_unique_open', 'type': 'INTEGER'},
                                {'name': 'qty_clicks', 'type': 'INTEGER'},
                                {'name': 'qty_unique_clicks', 'type': 'INTEGER'},
                                {'name': 'qty_unsubscribes', 'type': 'INTEGER'},
                                {'name': 'qty_hardbounces', 'type': 'INTEGER'},
                                {'name': 'qty_softbounces', 'type': 'INTEGER'}
        ]
)
