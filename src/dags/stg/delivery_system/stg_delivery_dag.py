import logging
import pendulum

from airflow.decorators import dag, task
from airflow.models.variable import Variable

from config_const import ConfigConst

from stg.delivery_system.schema_init import SchemaDdl
from stg.delivery_system.courier_loader import CourierLoader
from stg.delivery_system.delivery_loader import DeliveryLoader
from stg.delivery_system.restaurant_loader import RestaurantLoader
from lib.pg_connect import ConnectionBuilder
from stg.stg_settings_repository import StgEtlSettingsRepository
from lib.delivery_system_api import CouriersAPI, DeliveriesAPI, RestaurantsAPI

log = logging.getLogger(__name__)


@dag(
    dag_id='sprint5_project_stg_delivery',
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2024, 4, 2, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'stg', 'delivery_system_api'],
)
def sprint5_project_stg_delivery():
    dwh_pg_connect = ConnectionBuilder.pg_conn(ConfigConst.PG_WAREHOUSE_CONNECTION)
    settings_repository = StgEtlSettingsRepository()

    api_host = Variable.get("DELIVERY_API_HOST")
    api_key = Variable.get("DELIVERY_API_KEY")
    api_nickname = Variable.get("DELIVERY_API_NICKNAME")
    api_cohort = Variable.get("DELIVERY_API_COHORT")

    @task(task_id="schema_init")
    def schema_init():
        rest_loader = SchemaDdl(dwh_pg_connect)
        rest_loader.init_schema()

    @task(task_id="load_couriers")
    def load_couriers():
        source_api = CouriersAPI(api_host, api_key, api_nickname, api_cohort)
        courier_loader = CourierLoader(source_api, dwh_pg_connect, settings_repository)
        courier_loader.load()

    @task(task_id="load_deliveries")
    def load_deliveries(last_date: str):
        source_api = DeliveriesAPI(api_host, api_key, api_nickname, api_cohort)
        delivery_loader = DeliveryLoader(source_api, dwh_pg_connect, settings_repository)
        delivery_loader.load(last_date=last_date)

    @task(task_id="load_restaurants")
    def load_restaurants():
        source_api = RestaurantsAPI(api_host, api_key, api_nickname, api_cohort)
        delivery_loader = RestaurantLoader(source_api, dwh_pg_connect, settings_repository)
        delivery_loader.load()


    init_schema = schema_init()
    couriers = load_couriers()
    deliveries = load_deliveries('{{ ds }}')
    restaurants = load_restaurants()

    init_schema >> [couriers, deliveries, restaurants]
    


stg_delivery_dag = sprint5_project_stg_delivery()
