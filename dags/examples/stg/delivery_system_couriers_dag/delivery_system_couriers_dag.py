import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.delivery_system_couriers_dag.pg_saver import PgSaver
from examples.stg.delivery_system_couriers_dag.courier_loader import CourierLoader
from examples.stg.delivery_system_couriers_dag.courier_reader import CourierReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_example_stg_delivery_system_сouriers():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_сouriers():
        pg_saver = PgSaver()

        collection_reader = CourierReader()

        loader = CourierLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        loader.run_copy()

    сourier_loader = load_сouriers()

    сourier_loader  # type: ignore


сourier_stg_dag = sprint5_example_stg_delivery_system_сouriers()  # noqa
