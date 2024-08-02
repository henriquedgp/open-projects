# This code should be used after running {ac_automations_performance_report_1.py}

# Import libraries
import datetime as dt
import logging
import os
import pandas as pd
import pandas_gbq 
import tqdm
from google.cloud import bigquery

# Load credentials for BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'keys.json'

# Pegar dados no BQ
bigquery_dataset = {YOUR_DATASET}
bigquery_table = {YOUR_TABLE}
project_id = {YOUR_PROJECT_ID}

# Initialize BQ service
client = bigquery.Client()

# Since AC only gives us the aggregated results, we need to subtract ldate - before_ldate metrics values later.
# Store dates for queries
ldate = dt.date.today() - dt.timedelta(days=1)
before_ldate = yesterday - dt.timedelta(days=1)

# Getting only data from yesterday and transforming into df
query_df1 = f"""
    SELECT 
        *
    FROM 
        `{project_id}.{bigquery_dataset}.{bigquery_table}`
    WHERE
        extract_date = f'{ldate}'
    """

df_ldate = client.query(query_df1).to_dataframe()

# Getting data from two days ago and transforming into df
query_df2 = f"""
    SELECT 
        *
    FROM 
        `{project_id}.{bigquery_dataset}.{bigquery_table}`
    WHERE
        data_extracao = f'{before_ldate}'
    """

df_before_ldate = client.query(query_df2).to_dataframe()

# Compare data from each dataframe similar to double-entry accounting (DEBT x CREDIT).
merge_att = pd.merge(df_ldate,
                    df_before_ldate,
                    on='campaign_id',
                    suffixes=('', '2'))

merge_att = merge_att.rename(columns={'hardbounces': 'qty_hardbounces',
                          'softbounces': 'qty_softbounces'})

# New columns with the difference between ldate and before_ldate
merge_att['qty_envios'] = merge_att['qty_envios'] - merge_att['qtd_envios2']
merge_att['qty_aberturas'] = merge_att['qty_aberturas'] - merge_att['qtd_aberturas2']
merge_att['qty_aberturas_unicas'] = merge_att['qty_aberturas_unicas'] - merge_att['qtd_aberturas_unicas2']
merge_att['qty_cliques'] = merge_att['qty_cliques'] - merge_att['qtd_cliques2']
merge_att['qty_cliques_unicos'] = merge_att['qty_cliques_unicos'] - merge_att['qtd_cliques_unicos2']
merge_att['qty_unsubscribes'] = merge_att['qty_unsubscribes'] - merge_att['qtd_unsubscribes2']
merge_att['qty_hardbounces'] = merge_att['qtd_hardbounces'] - merge_att['hardbounces2']
merge_att['qty_softbounces'] = merge_att['qtd_softbounces'] - merge_att['softbounces2']

# Drop the additional columns that were there just for difference
clean_merge_att = merge_att.drop(merge_att.columns[12:23], axis=1)

# Check if there are new campaigns found
merge_news = pd.merge(df_ldate,
                        df_before_ldate,
                        on='campaign_id',
                        how='outer',
                        suffixes=('','2'),
                        indicator=True)

merge_news = merge_news.rename(columns={'hardbounces': 'qty_hardbounces',
                          'softbounces': 'qty_softbounces'})

merge_news = merge_news[merge_news['_merge'] != 'both']
merge_news = merge_news.drop('_merge', axis=1)

if len(merge_novas) > 0:
    clean_merge_news = merge_news.drop(merge_news.columns[12:23], axis=1)
    df_final = pd.concat([clean_merge_att, clean_merge_news])
    logging.info(f'Automations report finished with success, there were {len(merge_news)} new campaigns found with ID: {merge_novas['campaign_id']}.')
else:
    df_final = clean_merge_att
    logging.info('Automations report finished with success, there were no new campaigns found.')

# Sending report to BigQuery
pandas_gbq.to_gbq(dataframe=df_final,
                  destination_table=f'{bigquery_dataset}.automations_report',
                  project_id=project_id,
                  if_exists='append',
                  table_schema=[
                      {'name': 'extract_date', 'type': 'DATE'},
                      {'name': 'campaign_id', 'type': 'STRING'},
                      {'name': 'automation_id', 'type': 'STRING'},
                      {'name': 'campaign_name', 'type': 'STRING'},
                      {'name': 'qty_envios', 'type': 'INTEGER'},
                      {'name': 'qty_aberturas', 'type': 'INTEGER'},
                      {'name': 'qty_aberturas_unicas', 'type': 'INTEGER'},
                      {'name': 'qty_cliques', 'type': 'INTEGER'},
                      {'name': 'qty_cliques_unicos', 'type': 'INTEGER'},
                      {'name': 'qty_unsubscribes', 'type': 'INTEGER'},
                      {'name': 'qty_hardbounces', 'type': 'INTEGER'},
                      {'name': 'qty_softbounces', 'type': 'INTEGER'}]
)

logging.info('Automations report updated sucessfully!')
