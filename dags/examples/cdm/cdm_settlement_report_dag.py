import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.settlement_report import SettlementReportLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'settlement'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_case_cdm_settlement_report():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    @task
    def settlement_report_load():
        rest_loader = SettlementReportLoader(dwh_pg_connect)
        rest_loader.load_report()

    settlement_report_load()  # type: ignore


my_dag = sprint5_case_cdm_settlement_report()