from airflow.hooks.base_hook import BaseHook
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import AwsGlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from dbt import create_dbt_model_run
from property import InsightsConfig
from notification import write_message_to_slack
from notification import report_failure_to_slack_wrapper
from airflow.models import Variable

REGION_NAME = "us-east-1"
PLATFORM = "facebook"


with DAG(
    dag_id="facebook_from_s3_to_postgres",
    default_args=InsightsConfig.PIPELINE_DEFAULT_ARGS,
    schedule_interval=InsightsConfig.SCHEDULE_INTERVAL,
    concurrency=InsightsConfig.CONCURRENCY,
    max_active_runs=InsightsConfig.MAX_ACTIVE_RUNS,
    catchup=InsightsConfig.CATCHUP,
    description="Data Integration Workflow for Facebook API",
    start_date=days_ago(2),  # dummy start_date
    tags=["facebook", "insights"],
    on_failure_callback=report_failure_to_slack_wrapper(
        Variable.get("slack_webhook_url")
    ),
) as dag:
    facebook_country_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_country_report_raw2curated",
        job_name="facebook_country_report_raw2curated",
        dag=dag,
    )

    facebook_adset_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_adset_report_raw2curated",
        job_name="facebook_adset_report_raw2curated",
        dag=dag,
    )

    facebook_age_gender_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_age_gender_report_raw2curated",
        job_name="facebook_age_gender_report_raw2curated",
        dag=dag,
    )

    facebook_device_platform_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_device_platform_report_raw2curated",
        job_name="facebook_device_platform_report_raw2curated",
        dag=dag,
    )

    facebook_dma_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_dma_report_raw2curated",
        job_name="facebook_dma_report_raw2curated",
        dag=dag,
    )

    facebook_hourly_advertiser_tz_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_hourly_advertiser_tz_report_raw2curated",
        job_name="facebook_hourly_advertiser_tz_report_raw2curated",
        dag=dag,
    )

    facebook_publisher_platform_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_publisher_platform_report_raw2curated",
        job_name="facebook_publisher_platform_report_raw2curated",
        dag=dag,
    )


    facebook_region_report_raw2curated = AwsGlueJobOperator(
        task_id="facebook_region_report_raw2curated",
        job_name="facebook_region_report_raw2curated",
        dag=dag,
    )



    # trigger crawler
    country_report_crawler = AwsGlueCrawlerOperator(
        task_id="country_report",
        retries=3,
        config={"Name": "facebook-country_report"},
        poll_interval=60,
    )

    device_platform_report_crawler = AwsGlueCrawlerOperator(
        task_id="device_platform_report",
        retries=3,
        config={"Name": "facebook-device_platform_report"},
        poll_interval=60,
    )

    age_gender_report_crawler = AwsGlueCrawlerOperator(
        task_id="age_gender_report",
        retries=3,
        config={"Name": "facebook-age_gender_report"},
        poll_interval=60,
    )

    region_report_crawler = AwsGlueCrawlerOperator(
        task_id="region_report",
        retries=3,
        config={"Name": "facebook-region_report"},
        poll_interval=60,
    )

    hourly_advertiser_tz_report_crawler = AwsGlueCrawlerOperator(
        task_id="hourly_advertiser_tz_report",
        retries=3,
        config={"Name": "facebook-hourly_advertiser_tz_report"},
        poll_interval=60,
    )

    dma_report_crawler = AwsGlueCrawlerOperator(
        task_id="dma_report",
        retries=3,
        config={"Name": "facebook-dma_report"},
        poll_interval=60,
    )

    publisher_platform_report_crawler = AwsGlueCrawlerOperator(
        task_id="publisher_platform_report",
        retries=3,
        config={"Name": "facebook-publisher_platform_report"},
        poll_interval=60,
    )

    adset_report_crawler = AwsGlueCrawlerOperator(
        task_id="adset_report",
        retries=3,
        config={"Name": "facebook-adset_report"},
        poll_interval=60,
    )

    # create data marts
    build_dev_age_gender_report = create_dbt_model_run(
        dag, "build_dev_age_gender_report", "dev", "facebook.facebook_age_gender_report"
    )
    build_prd_age_gender_report = create_dbt_model_run(
        dag, "build_prd_age_gender_report", "prd", "facebook.facebook_age_gender_report"
    )

    build_dev_region_report = create_dbt_model_run(
        dag, "build_dev_region_report", "dev", "facebook.facebook_region_report"
    )
    build_prd_region_report = create_dbt_model_run(
        dag, "build_prd_region_report", "prd", "facebook.facebook_region_report"
    )

    build_dev_adset_report = create_dbt_model_run(
        dag, "build_dev_adset_report", "dev", "facebook.facebook_adset_report"
    )
    build_prd_adset_report = create_dbt_model_run(
        dag, "build_prd_adset_report", "prd", "facebook.facebook_adset_report"
    )

    monitoring_adset_report = create_dbt_model_run(
        dag,
        "monitoring_adset_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_adset_report",
    )

    monitoring_age_gender_report = create_dbt_model_run(
        dag,
        "monitoring_age_gender_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_age_gender_report",
    )

    monitoring_country_report = create_dbt_model_run(
        dag,
        "monitoring_country_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_country_report",
    )

    monitoring_device_platform_report = create_dbt_model_run(
        dag,
        "monitoring_device_platform_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_device_platform_report",
    )

    monitoring_dma_report = create_dbt_model_run(
        dag,
        "monitoring_dma_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_dma_report",
    )

    monitoring_hourly_advertiser_tz_report = create_dbt_model_run(
        dag,
        "monitoring_hourly_advertiser_tz_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_hourly_advertiser_tz_report",
    )

    monitoring_publisher_platform_report = create_dbt_model_run(
        dag,
        "monitoring_publisher_platform_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_publisher_platform_report",
    )

    monitoring_region_report = create_dbt_model_run(
        dag,
        "monitoring_region_report",
        "dev",
        "monitoring.grafana_monitoring_facebook_region_report",
    )

# country_report
(
    facebook_country_report_raw2curated
    >> country_report_crawler
    >> monitoring_country_report
)

# device_platform_report
(
    facebook_device_platform_report_raw2curated
    >> device_platform_report_crawler
    >> monitoring_device_platform_report
)

# age_gender_report
(facebook_age_gender_report_raw2curated >> age_gender_report_crawler)
(
    facebook_age_gender_report_raw2curated
    >> age_gender_report_crawler
    >> build_dev_age_gender_report
    >> build_prd_age_gender_report
)
(
    facebook_age_gender_report_raw2curated
    >> age_gender_report_crawler
    >> monitoring_age_gender_report
)

# region_report
(facebook_region_report_raw2curated >> region_report_crawler)
(
    facebook_region_report_raw2curated
    >> region_report_crawler
    >> build_dev_region_report
    >> build_prd_region_report
)
facebook_region_report_raw2curated >> region_report_crawler >> monitoring_region_report

# hourly_advertiser_tz_report
(
    facebook_hourly_advertiser_tz_report_raw2curated
    >> hourly_advertiser_tz_report_crawler
    >> monitoring_hourly_advertiser_tz_report
)

# dma_report
facebook_dma_report_raw2curated >> dma_report_crawler >> monitoring_dma_report

# publisher_platform_report
(
    facebook_publisher_platform_report_raw2curated
    >> publisher_platform_report_crawler
    >> monitoring_publisher_platform_report
)

# adset_report
(facebook_adset_report_raw2curated >> adset_report_crawler)
(
    facebook_adset_report_raw2curated
    >> adset_report_crawler
    >> build_dev_adset_report
    >> build_prd_adset_report
)
facebook_adset_report_raw2curated >> adset_report_crawler >> monitoring_adset_report
