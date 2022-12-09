import requests
import logging
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from google.cloud import bigquery
from google.oauth2 import service_account

def crawl_to_bigquery(cid: str, end_date: str, date_type: str, keyfile_path: str, destination: str):

    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
    start_date_type: dict[str, datetime] = {
        'date': end_datetime,
        'week': end_datetime - relativedelta(weeks=1),
        'month': end_datetime - relativedelta(months=1)
    }
    start_datetime = start_date_type[date_type]
    start_date: str = datetime.strftime(start_datetime, '%Y-%m-%d')

    url = 'https://datalab.naver.com/shoppingInsight/getCategoryKeywordRank.naver'
    header: dict = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Referer': 'https://datalab.naver.com/shoppingInsight/sCategory.naver',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }

    df = pd.DataFrame(columns=['cid','start_date', 'end_date', 'date_type', 'keyword', 'rank'])
    for page in range(1, 26):
        payload: dict = {
            'cid': cid,
            'timeUnit': 'date',
            'startDate': start_date,
            'endDate': end_date,
            'page': page,
            'count': 25
        }

        response = requests.post(url=url, headers=header, data=payload)
        df = pd.concat([df, pd.json_normalize(response.json(), 'ranks')], axis=0, ignore_index=True, join='outer')
    
    df.drop(columns=['linkId'], inplace=True)
    df['cid'] = cid
    df['start_date'] = start_datetime
    df['end_date'] = end_datetime
    df['date_type'] = date_type
    df['rank'] = df['rank'].astype('int64')
    df['updated_at'] = datetime.utcnow()

    # Upload to BigQuery
    logging.info('Start uploading to BigQuery')
    print('Start uploading to BigQuery')

    client = bigquery.Client(
        credentials=service_account.Credentials.from_service_account_file(keyfile_path))

    job_config = bigquery.LoadJobConfig(
        # DateFrame의 datetime64 자료형을 자동 스키마 지정으로 두면 DATETIME으로 지정됨, DATE로 수동 지정
        schema=[
            bigquery.SchemaField("start_date",
                                 bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("end_date",
                                 bigquery.enums.SqlTypeNames.DATE),
            bigquery.SchemaField("updated_at",
                                 bigquery.enums.SqlTypeNames.TIMESTAMP),
    ],
        write_disposition='WRITE_APPEND'
    )

    job = client.load_table_from_dataframe(
        df, destination, job_config=job_config)
    job.result()
    logging.info('Complete - Naver Shopping Insight Keyword Rank')
    return {'result': f'{len(df)} records are updated'}


cid='50000155'
date_type='week'
keyfile_path = ''
destination = ''

start_datetime = datetime(2022, 6, 1)
end_datetime = datetime(2022, 10, 30)
(end_datetime - start_datetime).days + 1
date_list = [(start_datetime + relativedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_datetime - start_datetime).days + 1)]

for date in date_list:
    print(f"Date: {date}")
    crawl_to_bigquery(cid, date, date_type, keyfile_path, destination)
