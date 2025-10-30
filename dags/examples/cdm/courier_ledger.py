from lib import PgConnect
from psycopg import Connection


class CourierLedgerRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_courier_ledger(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
;WITH order_sums AS (
    SELECT
        courier_id,
        courier_name,
        settlement_year,
        settlement_month, 
        rate_avg,
        COUNT(DISTINCT order_id)          AS orders_count,
        SUM(total_sum)      AS orders_total_sum,
        SUM(tip_sum) AS courier_tips_sum,
        SUM(CASE 
            WHEN rate_avg < 4 THEN GREATEST(total_sum * 0.05, 100) 
            WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(total_sum * 0.07, 150)
            WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(total_sum * 0.08, 175)
            WHEN rate_avg >= 4.9 THEN GREATEST(total_sum * 0.10, 200)
            ELSE 0 
        END) AS courier_order_sum
    from
        (SELECT
            c.id                    AS courier_id,
            c.courier_name          AS courier_name,
            tss.year AS settlement_year,
            tss.month AS settlement_month,
            fct.order_id,
            fct.total_sum,
            AVG(fct.rate) OVER (PARTITION BY c.id,tss.year, tss.month) AS rate_avg,
            fct.tip_sum
        FROM dds.fct_product_sales as fct
            INNER JOIN dds.dm_orders AS orders
                ON fct.order_id = orders.id
            INNER JOIN dds.dm_timestamps as tss
                ON tss.id = orders.timestamp_id
            INNER JOIN dds.dm_couriers AS c
                on c.id = orders.courier_id
        WHERE orders.order_status = 'CLOSED') q
    group by 
        courier_id,
        courier_name,
        settlement_year,
        settlement_month, 
        rate_avg
)
INSERT INTO cdm.dm_courier_ledger(
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum
)
SELECT
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    s.orders_total_sum * 0.25 AS order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
FROM order_sums AS s
ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
SET
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    rate_avg = EXCLUDED.rate_avg,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_order_sum = EXCLUDED.courier_order_sum,
    courier_tips_sum = EXCLUDED.courier_tips_sum,
    courier_reward_sum = EXCLUDED.courier_reward_sum;
                    """
                )
                conn.commit()


class CourierLedgerLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = CourierLedgerRepository(pg)

    def load_report(self):
        self.repository.load_courier_ledger()
