import requests
import logging
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def crawl_to_bigquery(cid: str, keyfile_path: str, destination: str):

    url = f'https://datalab.naver.com/shoppingInsight/getCategory.naver?cid={cid}'
    header: dict = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Referer': 'https://datalab.naver.com/shoppingInsight/sCategory.naver',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }
    response = requests.get(url=url, headers=header)

    df = pd.json_normalize(response.json(), 'childList')
    df = df.loc[:, ['cid', 'pid', 'name', 'parentPath', 'level']]
    df['cid'] = df['cid'].astype('string')
    df['pid'] = df['pid'].astype('string')

    # Upload to BigQuery
    logging.info('Start uploading to BigQuery')
    print('Start uploading to BigQuery')

    client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_file(keyfile_path))

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("cid",
                                 bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("pid",
                                 bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("name",
                                 bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("parentPath",
                                 bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField("level",
                                 bigquery.enums.SqlTypeNames.INTEGER),
        ],
        write_disposition='WRITE_APPEND'
    )

    job = client.load_table_from_dataframe(
        df, destination, job_config=job_config)
    job.result()
    logging.info('Complete - Naver Shopping Insight Keyword CID')
    return {'result': f'{len(df)} records are updated'}
    