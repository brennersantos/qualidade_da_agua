from pyspark.sql import functions as f

default_conf = {
    'include_metadata': dict(),
    'input_options': dict(format='parquet'),
    'output_options': dict(format='parquet', mode='errorifexists'),
}

required_conf = {
    'input_options': dict(path=str()),
    'output_options': dict(path=str()),
}


def run(spark, conf, logger):
    raw_data = extract(spark, conf, logger)
    transformed_data = transform(raw_data, spark, conf, logger)
    load(transformed_data, spark, conf, logger)
    return True


def extract(spark, conf, logger):
    return spark.read.load(**conf['input_options'])


def transform(raw_data, spark, conf, logger):
    for column, function in conf['include_metadata'].items():
        raw_data = raw_data.withColumn(column, eval(function))
    return raw_data


def load(transformed_data, spark, conf, logger):
    transformed_data.write.save(**conf['output_options'])
