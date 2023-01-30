import great_expectations as gx

default_conf = {
    'input_options': dict(format='parquet'),
    'gx_context_options': dict(
        runtime_environment=dict(force_reuse_spark_context='true')
    ),
}

required_conf = {
    'input_options': dict(path=str()),
    'data_quality_options': dict(
        checkpoint_name=str(), batch_request=dict(batch_identifiers=dict())
    ),
}


def run(spark, conf, logger):
    raw_data = extract(spark, conf, logger)
    return check_data_quality(raw_data, spark, conf, logger)


def extract(spark, conf, logger):
    return spark.read.load(**conf['input_options'])


def check_data_quality(raw_data, spark, conf, logger):
    context = gx.get_context(**conf['gx_context_options'])
    conf['data_quality_options']['batch_request']['runtime_parameters'] = {
        'batch_data': raw_data
    }
    results = context.run_checkpoint(**conf['data_quality_options'])
    return results.success
