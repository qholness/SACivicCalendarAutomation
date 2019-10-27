from sqlalchemy.engine import RowProxy, Connection
from gcal.CalendarOps  import CalendarOps
from tqdm import tqdm
from dba import delete_old_events
from googleapiclient.errors import HttpError


def create_new_events(tgt: str, events: RowProxy, ops: CalendarOps, con: Connection, cfg=None):
    for event in tqdm(events, desc="Adding events to calendar"):
        ops.create_event(tgt, event, con, cfg=cfg)


def remove_old_events(tgt: str, events: RowProxy, ops: CalendarOps, con: Connection):
    for event in tqdm(events, desc="Removing old events"):
        try:
            ops.delete_event(tgt, event.calendarId)
        except HttpError as e:
            print(e)
    delete_old_events(con)
