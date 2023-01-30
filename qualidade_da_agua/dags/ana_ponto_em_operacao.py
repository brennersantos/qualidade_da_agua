from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='ana_ponto_em_operacao',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
) as dag:

    data_quality_landing = BashOperator(
        task_id='data_quality_landing',
        cwd='/home/brenner/Projetos/qualidade_da_agua/',
        bash_command="""
            source .venv/bin/activate;
            python3 qualidade_da_agua/jobs/job_submitter.py --job-module handlers.runtime_check_data_quality --job-conf configs/data_quality/landing/ana_ponto_em_operacao.yaml;
        """,
    )

    ingestion_bronze = BashOperator(
        task_id='ingestion_bronze',
        cwd='/home/brenner/Projetos/qualidade_da_agua/',
        bash_command="""
            source .venv/bin/activate;
            python3 qualidade_da_agua/jobs/job_submitter.py --job-module handlers.batch_ingestion --job-conf configs/bronze/ana_ponto_em_operacao.yaml;
        """,
    )

    data_quality_landing >> ingestion_bronze
