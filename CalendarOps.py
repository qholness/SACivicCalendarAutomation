from sqlalchemy.engine import ResultProxy, Connection
from sqlalchemy.exc import OperationalError
from CalendarDataClass import CalendarData
from googleapiclient.discovery import build
from dba import update_calendarId, delete_from_fct_by_calendarId
from loguru import logger


class CalendarOps:

    def __init__(self, credentials):
        self.calendar_service = self.__create_calendar_service__(credentials)


    def __create_calendar_service__(self, credentials):
        return build('calendar', 'v3', credentials=credentials)


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
        body = CalendarData.generate_event_json(row)
        print(body)
        # Check to see if event exists first
        event_exists_on_g_calendar = row.calendarId is None
        
        if event_exists_on_g_calendar is True:
            event = (
                self.calendar_service
                    .events()
                    .insert(calendarId=tgt_calendar, body=body)
                    .execute())
            print(f"New event created: { event['id'] }")
            try:
                update_calendarId(row, event['id'], conn)
            except OperationalError as sqlOpError:
                print("Database update failed. Deleting event.", sqlOpError)
                self.delete_event(tgt_calendar, event['id'])
            return 0
        print(f"Event { row['calendarId'] } already exists")
        return 
