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
    get_dagster_logger,
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
    tags={"kind": "redis"},
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

partition_keys = [str(i) for i in range(1, 11)]


@static_partitioned_config(partition_keys=partition_keys)
def docker_config(partition_key: str):
    key = f'prefix/stock_{partition_key}.csv'
    return {
        "resources": {**docker["resources"]},
        "ops": {"get_s3_data": {"config": {"s3_key": key}}}
    }


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
    op_retry_policy=RetryPolicy(max_retries=10, delay=1)
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *") 

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *") 

bucket, access_key, secrect_key, endpoint_url =  docker["resources"]["s3"]["config"].values()
print(endpoint_url)

@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=30)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(bucket=bucket, 
                            prefix='prefix', 
                            endpoint_url=endpoint_url)
    log = get_dagster_logger()
    log.info(f'RunRequest for {new_files}')
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config={
                "resources": {**docker["resources"]},
                "ops": {"get_s3_data": {"config": {"s3_key": new_file}}}
            }
        )
