import yfinance
import json
import boto3
import os
import subprocess
import sys

subprocess.check_call([sys.executable, "-m", "pip",
                       "install", "--target", "/tmp", 'yfinance'])
sys.path.append('/tmp')


def lambda_handler(event, context):
    start_date = '2020-05-14'
    end_date = '2020-05-15'
    interval = '1m'
    company_arr = ['fb', 'shop', 'bynd', 'nflx',
                   'pins', 'sq', 'ttd', 'okta', 'snap', 'ddog']
    fh = boto3.client("firehose", "us-east-2")

    for company in company_arr:
        download = yfinance.Ticker(company).history(
            start=start_date, end=end_date, interval=interval)

        for index, rows in download.iterrows():
            as_jsonstr = json.dumps({"high": rows.High, "low": rows.Low, "ts": str(
                index), 'name': company}, indent=1, separators=(',', ': '))
            fh.put_record(DeliveryStreamName="project-3-delivery-stream",
                          Record={"Data": as_jsonstr.encode('utf-8')})

    return {
        'statusCode': 200,
        'body': json.dumps(f'Done! Recorded')
    }
