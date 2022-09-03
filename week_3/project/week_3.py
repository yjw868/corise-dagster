from msvcrt import kbhit
from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    context.log.info(f's3_key is {s3_key}')
    s3 = context.resources.s3
    context.log.info(f's3 is {s3}')
    context.log.info(f'list of keys are: {s3.get_keys()}')
    return([Stock.from_list(x) for x in s3.get_data(s3_key)])


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"highest_value": Out(dagster_type=Aggregation)},
    tags={"kind": "python"},
    description="Take the list of stocks and determine the Stock with the greatest high value"
)
def process_data(stocks):
    highest_value = max(stocks, key=lambda stock: stock.high)
    return Aggregation(date=highest_value.date, high=highest_value.high)


@op(
    required_resource_keys={"redis"},
    ins={"highest_value": In(dagster_type=Aggregation)},
    tags={"kind": "redit"},
)
def put_redis_data(context, highest_value):
    redis = context.resources.redis
    redis.put_data(
        name=highest_value.date.strftime("%m-%d-%Y"),
        value=highest_value.high
    )


@graph
def week_3_pipeline():
    put_redis_data(process_data(get_s3_data()))
    

local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker = {
    "resources": {
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
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


def docker_config():
    pass


local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
)


local_week_3_schedule = None  # Add your schedule

docker_week_3_schedule = None  # Add your schedule


@sensor
def docker_week_3_sensor():
    pass
