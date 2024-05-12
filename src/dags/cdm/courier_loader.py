from lib import PgConnect


class SettlementRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_settlement_by_days(self) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH deliveries_describe AS (
                        SELECT
                            c.courier_id                    AS courier_id,
                            c.courier_name                  AS courier_name,
                            ts.year                         AS settlement_year,
                            ts.month                        AS settlement_month,
                            COUNT(DISTINCT fct.order_id)    AS orders_count,
                            SUM(fct.order_sum)              AS orders_total_sum,
                            AVG(fct.rate)                   AS rate_avg,
                            SUM(fct.tip_sum)                AS courier_tips_sum
                        FROM dds.fct_deliveries as fct
                            LEFT JOIN dds.dm_orders AS o
                                ON fct.order_id = o.id
                            LEFT JOIN dds.dm_timestamps as ts
                                ON ts.id = o.timestamp_id
                            LEFT JOIN dds.dm_couriers AS c
                                on fct.courier_id = c.id
                        WHERE c.active_to = '2099-12-31'::timestamp
                        GROUP BY
                            c.courier_id,
                            c.courier_name,
                            ts.year,
                            ts.month 
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
                    orders_total_sum * 0.25       AS order_processing_fee,
                    (case
                            WHEN rate_avg < 4 THEN
                                (case
                                    when orders_total_sum * 0.05 < 100 then 100
                                    else orders_total_sum * 0.05
                                end) 
                            WHEN rate_avg >= 4  and rate_avg < 4.5 THEN orders_total_sum * 0.07
                            WHEN rate_avg >= 4.5 and rate_avg < 4.9 THEN orders_total_sum * 0.08
                            WHEN rate_avg >= 4.9 THEN orders_total_sum * 0.1
                        end
                        )                          AS courier_order_sum,
                        courier_tips_sum,
                        courier_tips_sum * 0.95 AS courier_reward_sum
                    FROM deliveries_describe AS dd
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET    
                        courier_name=EXCLUDED.courier_name,
                        orders_count=EXCLUDED.orders_count,
                        orders_total_sum=EXCLUDED.orders_total_sum,
                        rate_avg=EXCLUDED.rate_avg,
                        order_processing_fee=EXCLUDED.order_processing_fee,
                        courier_order_sum=EXCLUDED.courier_order_sum,
                        courier_tips_sum=EXCLUDED.courier_tips_sum,
                        courier_reward_sum=EXCLUDED.courier_tips_sum;
                    """
                )
                conn.commit()


class SettlementReportLoader:

    def __init__(self, pg: PgConnect) -> None:
        self.repository = SettlementRepository(pg)

    def load_report_by_days(self):
        self.repository.load_settlement_by_days()