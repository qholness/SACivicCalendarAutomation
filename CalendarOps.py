from sqlalchemy.engine import ResultProxy, Connection
from sqlalchemy.exc import OperationalError
from CalendarDataClass import CalendarData
from googleapiclient.discovery import build
from dba import update_calendarId
from loguru import logger
from schema import fct_calendar_data


class CalendarOps:

    def __init__(self, credentials):
        self.calendar_service = self.__init_calendar_service__(credentials)


    def __init_calendar_service__(self, credentials):
        return build('calendar', 'v3', credentials=credentials)
    

    def generate_event_json(row: ResultProxy,
        name_key: str=fct_calendar_data.name,
        time_key: str=fct_calendar_data.time,
        date_key: str=fct_calendar_data.date,
        loc_key: str=fct_calendar_data.location,
        time_zone: str='America/Chicago',
        calendarId: str=fct_calendar_data.id):
        """perform event pre-processing and output a Google Calendar API-compliant JSON file"""
        gen_datetime = lambda d, t: f"{ d }T{ t }"
        start_time = row[time_key]
        end_time = CalendarData.generate_end_time(row[time_key])
        date = row[date_key]
        return {
            'calendarId': row[calendarId],
            'summary': row[name_key],
            'location': row[loc_key],
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
        (
            self.calendar_service
                .events()
                .delete(
                    calendarId=tgt_calendar,
                    eventId=eventId)
                .execute())
        logger.info(f"DELTED EVENT: { eventId }.")


    def create_event(self, tgt_calendar: str, row: ResultProxy, conn: Connection):
        body = CalendarOps.generate_event_json(row)
        
        event_exists_on_g_calendar = row.calendarId is None
        
        if event_exists_on_g_calendar is True:
            logger.info("Event not found, creating a new one.")
            event = (
                self.calendar_service
                    .events()
                    .insert(calendarId=tgt_calendar, body=body)
                    .execute())
            logger.info(f"New event created: { event['id'] }")
            try:
                update_calendarId(row, event['id'], conn)
            except OperationalError as sqlOpError:
                logger.warning("Database update failed. Deleting event.", sqlOpError)
                self.delete_event(tgt_calendar, event['id'])
            return 0
        print(f"Event { row['calendarId'] } already exists")
        return 0
