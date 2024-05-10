import pendulum
from airflow.decorators import dag, task
from config_const import ConfigConst 
from lib import ConnectionBuilder

from cdm.settlement_report import SettlementReportLoader
from cdm.schema_init import SchemaDdl


@dag(
    schedule_interval='0/30 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5','project', 'cdm', 'settlement'],
    is_paused_upon_creation=False
)
def sprint5_project_cdm_settlement_report():
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    @task
    def schema_init():
        schema_ddl = SchemaDdl(dwh_pg_connect)
        schema_ddl.init_schema()

    @task
    def settlement_daily_report_load():
        rest_loader = SettlementReportLoader(dwh_pg_connect)
        rest_loader.load_report_by_days()


    schema = schema_init()
    report = settlement_daily_report_load()

    schema >> report  # type: ignore


cdm_dag = sprint5_project_cdm_settlement_report()  # noqa
