import json
from turtle import clear
import httpx
from sys import stderr
from multiprocessing import cpu_count
from math import ceil
import base64

import asyncio
from aiomultiprocess import Pool
from typing import List
from pathlib import Path
from pydantic import BaseModel
import aiofiles
from urllib.parse import urlparse


class Batch(BaseModel):
    job_id: str
    batch_start: int
    batch_size: int
    api_version: str


url = "https://officedepot--oduat.my.salesforce.com"
token = "00D590000004ea0!AQ8AQNym1nFjNnW.kn8DCbqubB45h2LTWQL6BhdPk29Oa2uGmfs8I9arh0LsiVEKWV898EhDHp7YrejxF8vvK87o.nqeP_Ul"

def get_ingest_job(job_id: str, version: str):
    query_path = f"services/data/v{version}/jobs/ingest/{job_id}"
    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    return data.json()


def get_all_ingest_jobs(version: str):
    query_path = f"services/data/v{version}/jobs/ingest"
    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    return data.json()

def get_ingest_data(job_id: str, locator: int, max_records: int, version: str):
    query_path = f"services/data/v{version}/jobs/ingest/{job_id}/results"

    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        params={
            "maxRecords": max_records,
            "locator": base64.b64encode(str(locator).encode()).decode(),
        },
    )
    if data.status_code != 200 or data.status_code != 201:
        print(data.content.decode(), file=stderr)

    return data.content.decode()


def get_failed_ingest_job_result(job_id: str, version: str):
    query_path = f"services/data/v{version}/jobs/ingest/{job_id}/failedResults"
    data = httpx.get(
        f"{url}/{query_path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    print(url+query_path)
    if data.status_code != 200:
        print(data.content.decode(), file=stderr)
    
    url_content = data.content
    filename = 'fail_'+job_id+'.csv'
    csv_file = open(filename, 'wb')
    csv_file.write(url_content)
    csv_file.close()



if __name__ == "__main__":
    all_job_ids = []
    all_job_data = get_all_ingest_jobs(version="53.0")
    for job in all_job_data['records']:
        temp = get_ingest_job(job_id=job['id'], version="53.0")
        if(temp['state'] == 'JobComplete' and temp['numberRecordsFailed']>0):
            all_job_ids.append(job['id'])

    print(all_job_data)
    
    

    # for j in all_job_ids:
    #     get_failed_ingest_job_result(j, version="53.0")
    #     print(j,'done')

    

    
