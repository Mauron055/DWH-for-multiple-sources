import logging 

from typing import List, Optional
from datetime import datetime

from lib import PgConnect
from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from dds.courier_loader import CourierDdsRepository
from dds.timestamp_loader import TimestampDdsRepository
from dds.order_loader_repositories import OrderDdsRepository
from dds.delivery_repositories import DeliveryStgObj, DeliveryStgRepository, DeliveryDdsObj, DeliveryDdsRepository



class DeliveryLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg: PgConnect, settings_repository: DdsEtlSettingsRepository) -> None:
        self.dwh = pg
        self.delivery_stg = DeliveryStgRepository()
        self.delivery_dds = DeliveryDdsRepository()
        self.order_dds = OrderDdsRepository()
        self.courier_dds = CourierDdsRepository()
        self.timestamp_dds = TimestampDdsRepository()
        self.settings_repository = settings_repository

    def parse_deliveries(self,conn: PgConnect, raws: List[DeliveryStgObj]) -> List[DeliveryDdsObj]:
        res = []
        
        for r in raws:
            order = self.order_dds.get_order(conn, r.order_id)
            courier = self.courier_dds.get_courier(conn, r.courier_id)
            timestamp = self.timestamp_dds.get_timestamp(conn, r.delivery_ts)
           
            if order and courier and timestamp:
                
                t = DeliveryDdsObj(id=0,
                                delivery_id=r.delivery_id,
                                order_id=order.id,
                                courier_id=courier.id,
                                timestamp_id=timestamp.id,
                                rate=r.delivery_json['rate'],
                                tip_sum=r.delivery_json['tip_sum'],
                                order_sum=r.delivery_json['sum']
                                )
                
                res.append(t)
            
        logging.info("Success parse {len(res)} deliveries from {len(raws)}")
        return res

    def load_deliveries(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    })

            last_loaded_ts = datetime.fromisoformat(wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY])
            logging.info(f"starting to load from last checkpoint: {last_loaded_ts}")
            
            load_queue = self.delivery_stg.load_stg_deliveries(conn, last_loaded_ts)
            logging.info(f"get {len(load_queue)} deliveries")
            
            if not load_queue:
                logging.info("Quitting.")
                return 0
            
            last_loaded_ts = load_queue[-1].update_ts.isoformat()
            print(f'NEW checkpoint: {last_loaded_ts}')
            
            deliveries_to_load = self.parse_deliveries(conn, load_queue)
            
            for r in deliveries_to_load:
                self.delivery_dds.insert_delivery(conn, r)

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts
                self.settings_repository.save_setting(conn, wf_setting)
