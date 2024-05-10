import logging
import pendulum

from airflow import DAG
from airflow.decorators import task

from lib import ConnectionBuilder
from config_const import ConfigConst
from dds.dds_settings_repository import DdsEtlSettingsRepository
from schema_init import SchemaDdl
from dds.order_loader_repositories import OrderLoader
from dds.restaurant_loader import RestaurantLoader
from dds.timestamp_loader import TimestampLoader
from dds.courier_loader import CourierLoader 
from dds.fct_delivery_loader import DeliveryLoader


log = logging.getLogger(__name__)

with DAG(
    dag_id='sprint5_project_dds_delivery_snowflake',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'dds'],
    is_paused_upon_creation=False
) as dag:
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)

    settings_repository = DdsEtlSettingsRepository()

    @task(task_id="schema_init")
    def schema_init(ds=None, **kwargs):
        rest_loader = SchemaDdl(dwh_pg_connect)
        rest_loader.init_schema()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants(ds=None, **kwargs):
        rest_loader = RestaurantLoader(dwh_pg_connect, settings_repository)
        rest_loader.load_restaurants()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps(ds=None, **kwargs):
        ts_loader = TimestampLoader(dwh_pg_connect, settings_repository)
        ts_loader.load_timestamps()

    @task(task_id="dm_couriers_load")
    def load_dm_couriers(ds=None, **kwargs):
        courier_loader = CourierLoader(dwh_pg_connect, settings_repository)
        courier_loader.load_couriers()

    @task(task_id="fct_deliveries_load")
    def load_fct_deliveries(ds=None, **kwargs):
        fct_loader = DeliveryLoader(dwh_pg_connect, settings_repository)
        fct_loader.load_deliveries()

    init_schema = schema_init()
    dm_restaurants = load_dm_restaurants()
    dm_timestamps = load_dm_timestamps()
    dm_couriers = load_dm_couriers()
    fct_deliveries = load_fct_deliveries()

    init_schema >> [dm_restaurants, dm_timestamps, dm_couriers] >> fct_deliveries # type: ignore
    
