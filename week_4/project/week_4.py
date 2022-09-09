from typing import List

from dagster import Nothing, asset, with_resources
from project.resources import redis_resource, s3_resource
from project.types import Aggregation, Stock


@asset(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    group_name='corise',
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    context.log.info(f's3_key is {s3_key}')
    s3 = context.resources.s3
    context.log.info(f's3 is {s3}')
    context.log.info(f'list of keys are: {s3.get_keys()}')
    return([Stock.from_list(x) for x in s3.get_data(s3_key)])


@asset(
    group_name="corise",
)
def process_data(get_s3_data):
    stocks = get_s3_data
    highest_value = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=highest_value.date, high= highest_value.high)

@asset(
    required_resource_keys={"redis"},
    group_name="corise",
    description="Take the list of stocks and determine the Stock with the greatest high value"
)
def put_redis_data(context, process_data):
    highest_value = process_data
    redis = context.resources.redis
    redis.put_data(
        name=highest_value.date.strftime("%m-%d-%Y"),
        value=highest_value.high
    )


get_s3_data_docker, process_data_docker, put_redis_data_docker = with_resources(
    definitions=[get_s3_data, process_data, put_redis_data],
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource
    },
    resource_config_by_key={
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    }
)
