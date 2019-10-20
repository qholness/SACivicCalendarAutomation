from sqlalchemy.engine import ResultProxy, Connection
from CalendarOps import CalendarOps
from tqdm import tqdm
from dba import delete_old_events


def create_new_events(tgt: str, events: ResultProxy, ops: CalendarOps, con: Connection, cfg=None):
    for event in tqdm(events, desc="Adding events to calendar"):
        ops.create_event(tgt, event, con, cfg=cfg)


def remove_old_events(tgt: str, events: ResultProxy, ops: CalendarOps, con: Connection):
    for event in tqdm(events, desc="Removing old events"):
        ops.delete_event(tgt, event.calendarId)
    delete_old_events(con)
