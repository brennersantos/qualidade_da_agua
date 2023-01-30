import argparse
import importlib
from typing import Dict, List, Tuple

import yaml
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession


def load_conf_file(file_name: str = None) -> Dict:
    """Reads the confs/conf.yaml file and parse as a dictionary."""
    if file_name == None:
        return {}
    try:
        with open(f'{file_name}') as file:
            conf = yaml.unsafe_load(file)
        return conf

    except FileNotFoundError:
        raise FileNotFoundError(f'{file_name} Not found')


def fill_default_conf(job_conf, job_default_conf):
    default_conf = {'spark_conf': {}}

    return {**default_conf, **job_default_conf, **job_conf}


def parse_spark_conf(job_conf: str) -> List[Tuple[str, str]]:
    return [(key, value) for key, value in job_conf['spark_conf'].items()]


def check_required_conf(job_conf, job_required_conf):
    if not check_structure_(job_conf, job_required_conf):
        raise Exception('Não possui todas as configurações necessárias')


def check_structure_(struct, conf):
    if isinstance(struct, dict) and isinstance(conf, dict):
        return all(
            key in struct and check_structure_(struct[key], conf[key])
            for key in conf
        )
    if isinstance(struct, list) and isinstance(conf, list):
        return all(check_structure_(struct[0], c) for c in conf)
    else:
        return isinstance(struct, type(conf))


def create_spark_session(conf: List[Tuple[str, str]] = None):
    """Create spark session to run the job."""
    conf = conf or []
    conf = SparkConf().setAll(conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    logger = get_logger_(spark)
    return spark, logger


def get_logger_(spark: SparkSession):
    job_name = spark.conf.get('spark.app.name')
    app_id = spark.conf.get('spark.app.id')
    message_prefix = f'< {job_name} {app_id} >'
    log4j = spark._jvm.org.apache.log4j
    logger = log4j.LogManager.getLogger(message_prefix)
    return logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Job Submitter')
    parser.add_argument('--job-module', required=True, help='Job module name')
    parser.add_argument('--job-conf', help='Config job file path')
    args, unknown = parser.parse_known_args()

    job_module = args.job_module
    job = importlib.import_module(f'{args.job_module}')

    job_default_conf = job.default_conf or {}
    job_required_conf = job.required_conf or {}

    job_conf = load_conf_file(args.job_conf)
    job_conf = fill_default_conf(job_conf, job_default_conf)

    check_required_conf(job_conf, job_required_conf)

    spark_conf = parse_spark_conf(job_conf)
    spark, logger = create_spark_session(spark_conf)

    logger.warn(f'calling job {job_module}  with {job_conf}')

    job.run(spark=spark, conf=job_conf, logger=logger)

    spark.stop()
