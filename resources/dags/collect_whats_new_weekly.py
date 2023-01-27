import airflow
import math
import calendar
import boto3
import json
import asyncio
import aiohttp

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime, date
from boto3.dynamodb.conditions import Key, Attr
from functools import reduce

S3_BUCKET_NAME = "YOUR_BUCKET_NAME"
TABLE_NAME = "mwaa-cicd-whatsnew"
TOPIC_ARN = "YOUR_TOPIC_ARN"

default_args = {
    "owner": "aws",
    "depends_on_past": False,
    "start_date": airflow.utils.dates.days_ago(1),
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
    "provide_context": True
}

dag = DAG(
    "whatsnew",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval="0 3 * * *"
)


def get_contents(items: list, month: int, week_range: list) -> list:
    data = []

    for item in items:
        post_datetime = datetime.strptime(item["item"]["additionalFields"]["postDateTime"], "%Y-%m-%dT%H:%M:%SZ")
        if post_datetime.month == month and post_datetime.day in week_range:
            data.append(item)

    return data


async def fetch(session, url, month, week_range):
    async with session.get(url) as response:
        result = await response.json()
        source = result["items"]
        contents = get_contents(source, month, week_range)

    return contents


async def main(size: int = 20) -> list:
    weekly_data = []

    # week range
    today = date.today()
    calendar.setfirstweekday(calendar.SUNDAY)
    cal = calendar.monthcalendar(today.year, today.month)
    week_range = cal[math.floor(today.day / 7)]
    if today.day not in week_range:
        for days in cal:
            if today.day in days:
                week_range = days
                break

    url = f"https://aws.amazon.com/api/dirs/items/search?item.directoryId=whats-new&sort_by=item.additionalFields.postDateTime&sort_order=desc&size={size}&item.locale=en_US&tags.id=whats-new%23year%23{today.year}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            result = await response.json()
            source = result["items"]
            contents = get_contents(source, today.month, week_range)
            weekly_data += contents

            metadata = result["metadata"]
            total_hits = metadata["totalHits"]
            pages = math.ceil(total_hits / size)

            print(total_hits, pages)

        futures = [
            asyncio.ensure_future(fetch(session, url + f"&page={page}", today.month, week_range)) for page in
            range(1, pages)
        ]
        results = await asyncio.gather(*futures)
        weekly_data += reduce(lambda x, y: x + y, results)

        print(len(weekly_data))

    return weekly_data


def collect(size, bucket_name):
    weekly_data = asyncio.run(main(size))

    s3 = boto3.resource("s3")
    dt_now = datetime.now()
    file_name = f"whats_new_weekly_{dt_now.strftime('%Y_%m_%d_%H%M%S')}.json"

    s3_obj = s3.Object(bucket_name, f"data/weekly/{file_name}")
    s3_obj.put(Body=json.dumps(weekly_data).encode("UTF-8"))

    return file_name


collect_task = PythonOperator(
    task_id="collect",
    provide_context=True,
    python_callable=collect,
    op_args=[20, S3_BUCKET_NAME],
    dag=dag
)


def fill_table(bucket_name, **context):
    file_name = context["task_instance"].xcom_pull(task_ids="collect")

    s3 = boto3.resource("s3")
    s3_obj = s3.Object(bucket_name, f"data/weekly/{file_name}")
    file_obj = s3_obj.get()
    data = json.loads(file_obj["Body"].read().decode("UTF-8"))

    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)

    with table.batch_writer(overwrite_by_pkeys=["post_month", "post_id"]) as batch:
        for item in data:
            post_datetime = datetime.strptime(item["item"]["additionalFields"]["postDateTime"], "%Y-%m-%dT%H:%M:%SZ")
            batch.put_item(
                Item={
                    "post_month": f"{post_datetime.year}-{post_datetime.month}",
                    "post_id": item["item"]["id"],
                    "post_date": str(post_datetime.date()),
                    "headline": item["item"]["additionalFields"]["headline"],
                    "headline_url": "https://aws.amazon.com" + item["item"]["additionalFields"]["headlineUrl"],
                }
            )

    print("Loaded data into table")


fill_task = PythonOperator(
    task_id="fill",
    provide_context=True,
    python_callable=fill_table,
    op_kwargs={"bucket_name": S3_BUCKET_NAME},
    dag=dag
)


def send_whatsnew(topic_arn):
    today = date.today()
    calendar.setfirstweekday(calendar.SUNDAY)
    cal = calendar.monthcalendar(today.year, today.month)
    week_range = cal[math.floor(today.day/7)]
    if today.day not in week_range:
        for days in cal:
            if today.day in days:
                week_range = days
                break

    start_day, end_day = 0, 0
    for day in week_range:
        if day != 0:
            start_day = day
            break

    for day in reversed(week_range):
        if day != 0:
            end_day = day
            break

    print(start_day, end_day)
    start_date = today.replace(day=start_day)
    end_date = today.replace(day=end_day)

    subject = f"AWS Weekly Update [{today}]"

    # Message
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(TABLE_NAME)

    response = table.query(
        KeyConditionExpression=Key("post_month").eq(f"{today.year}-{today.month}"),
        FilterExpression=Attr("post_date").between(str(start_date), str(end_date))
    )
    items = response["Items"]

    sns = boto3.resource("sns")
    topic = sns.Topic(topic_arn)

    if len(items) > 0:
        msg = "\n".join([item["headline"] for item in items])
    else:
        msg = "No new items"

    topic.publish(
        Subject=subject,
        Message=msg
    )


send_task = PythonOperator(
    task_id="send",
    provide_context=True,
    python_callable=send_whatsnew,
    op_kwargs={"topic_arn": TOPIC_ARN},
    dag=dag
)


collect_task >> fill_task >> send_task
