from sqlalchemy.engine import RowProxy, Connection
from sqlalchemy.exc import OperationalError
from data.CalendarData import CalendarData
from googleapiclient.discovery import build
from loguru import logger
from schema import fct_calendar_data


def check_nat_str(nat: str):
    if not isinstance(nat, str):
        return False
    return 'nat' in nat.lower()


class CalendarOps:

    def __init__(self, credentials):
        self.calendar_service = self.__init_calendar_service__(credentials)


    def __init_calendar_service__(self, credentials):
        return build('calendar', 'v3', credentials=credentials)
    

    def generate_event_json(
            row: RowProxy
            , name_key: str=fct_calendar_data.name
            , time_key: str=fct_calendar_data.time
            , date_key: str=fct_calendar_data.date
            , loc_key: str=fct_calendar_data.location
            , time_zone: str='America/Chicago'
            , calendarId: str=fct_calendar_data.id
            , cfg=None) -> dict:
        """perform event pre-processing and output a Google Calendar API-compliant JSON file"""
        def gen_datetime(d, t) -> str:
            return f"{ d }T{ t }"
        
        timestamp = row[time_key]
        date = row[date_key]
        desc = cfg['DEFAULT']['calendarLink'] if cfg else "San Antonio Civic Event"
        is_nat = check_nat_str(timestamp)

        if is_nat:
            start_time = CalendarData.__default_time__
        else:
            start_time = timestamp

        end_time = CalendarData.generate_end_time(start_time)

        return {
            'calendarId': row[calendarId],
            'summary': row[name_key],
            'location': row[loc_key],
            'description': desc,
            'start': {
                'dateTime': gen_datetime(date, start_time),
                'timeZone': time_zone
            },
            'end': {
                'dateTime': gen_datetime(date, end_time),
                'timeZone': time_zone
            },
            'reminders': {
                'useDefault': True,
            },
        }


    def delete_event(self, tgt_calendar: str, eventId: str):
        (self.calendar_service
            .events()
            .delete(
                calendarId=tgt_calendar,
                eventId=eventId)
            .execute())


    def update_calendarId(row: RowProxy, calendarId: str, conn: Connection):
        return conn.execute(f"""
            UPDATE fct_calendar_data
            SET calendarId='{ calendarId }'
            WHERE uniqueId='{ row.uniqueId }'
                AND calendarId is null
        """)


    def create_event(self, tgt_calendar: str, row: RowProxy, conn: Connection, cfg=None):
        body = CalendarOps.generate_event_json(row, cfg=cfg)
        
        event_exists_on_g_calendar = row.calendarId is None
        
        if event_exists_on_g_calendar is True:
            event = (
                self.calendar_service
                    .events()
                    .insert(calendarId=tgt_calendar, body=body)
                    .execute())
            try:
                CalendarOps.update_calendarId(row, event['id'], conn)
            except OperationalError as sqlOpError:
                logger.warning("Database update failed. Deleting event.", sqlOpError)
                self.delete_event(tgt_calendar, event['id'])
            return 0
        return 0
