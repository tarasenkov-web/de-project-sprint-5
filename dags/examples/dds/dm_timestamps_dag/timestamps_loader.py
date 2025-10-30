import json
from typing import List, Optional
from datetime import datetime, date, time
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

# from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from examples.dds import EtlSetting, DdsEtlSettingsRepository


class TimestampJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class TimestampDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time 


class TimestampRawRepository:
    def load_raw_timestamps(self, conn: Connection, last_loaded_record_id: int) -> List[TimestampJsonObj]:
        with conn.cursor(row_factory=class_row(TimestampJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(last_loaded_record_id)s
                    AND object_value::jsonb ->> 'final_status' IN ('CLOSED', 'CANCELLED');
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs


class TimestampDdsRepository:
    def insert_timestamp(self, conn: Connection, timestamp: TimestampDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, date, time)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(date)s, %(time)s);
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "date": timestamp.date,
                    "time": timestamp.time,
                },
            )

    def get_timestamp(self, conn: Connection, ts: str) -> Optional[TimestampDdsObj]:
        with conn.cursor(row_factory=class_row(TimestampDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        ts,
                        year,
                        month,
                        day,
                        date,
                        time
                    FROM dds.dm_timestamps
                    WHERE ts = %(ts)s;
                """,
                {"ts": ts},
            )
            obj = cur.fetchone()
        return obj


class TimestampLoader:
    WF_KEY = "timestamps_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_Timestamp_id"

    def __init__(self, pg: PgConnect) -> None:
        self.dwh = pg
        self.raw = TimestampRawRepository()
        self.dds = TimestampDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()

    def parse_timestamps(self, raws: List[TimestampJsonObj]) -> List[TimestampDdsObj]:
        res = []
        for r in raws:
            timestamp_json = json.loads(r.object_value)
            ts = datetime.fromisoformat(timestamp_json['date'])
            t = TimestampDdsObj(id=r.id,
                            ts=ts,
                            year=ts.year,
                            month=ts.month,
                            day=ts.day,
                            date=ts.date(),
                            time=ts.time()
                           )

            res.append(t)
        return res

    def load_timestamps(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_timestamps(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            timestamps_to_load = self.parse_timestamps(load_queue)
            for u in timestamps_to_load:
                existing = self.dds.get_timestamp(conn, u.ts)
                if not existing:
                    self.dds.insert_timestamp(conn, u)
            
                settings_json = json.dumps(wf_setting.workflow_settings)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = u.id
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, settings_json)
