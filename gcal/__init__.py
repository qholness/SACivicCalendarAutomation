from gcal.CalendarOps  import CalendarOps
from sqlalchemy.engine import Connection
from tqdm import tqdm
from dba import delete_from_fct_by_calendarId


def cleanup_calendar(ops: CalendarOps, target_calendar: str, con: Connection):
    for event in tqdm(con.execute("SELECT * FROM fct_calendar_data").fetchall(), desc="Deleting events"):
        ops.delete_event(target_calendar, event['calendarId'])
        delete_from_fct_by_calendarId(event['calendarId'], con)