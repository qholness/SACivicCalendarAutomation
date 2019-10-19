from __future__ import print_function
import pandas as pd
import datetime
import pickle
import datetime
import os.path
import sys
import dba
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from CalendarDataClass import CalendarData
from sqlalchemy import create_engine
from sqlalchemy.engine import ResultProxy
from sqlalchemy.exc import OperationalError
from schema import fct_calendar_data
from CalendarOps import CalendarOps
from convert_xls_data_to_csv import csv_from_excel
from config import config

global CALENDARDATAPATH, TARGETCALENDARID, DF, CONN
engine_path = config['DB']['CON_STRING']
CONN = create_engine(engine_path).connect()
TARGETCALENDARID = config['CALENDAR']['TARGET']
SCOPES = [
    # 'https://www.googleapis.com/auth/calendar.readonly',
    'https://www.googleapis.com/auth/calendar' # Read/Write permissions
]

def convert_data():
    csv_from_excel(
        config['INPUTDATA']['SOURCEXLS'],
        config['INPUTDATA']['SOURCECSV'])


def data_intake():
    DF = CalendarData(source_path=config['INPUTDATA']['PATH'])
    dba.truncate_stg(CONN)
    dba.update_staging_table(DF.data, CONN)
    dba.migrate_stg_to_fct(CONN)


def cleanup_calendar(ops: CalendarOps):
    """Delete entries from calendar"""
    global CONN, TARGETCALENDARID
    for event in CONN.execute("SELECT * FROM fct_calendar_data").fetchall():
        try:
            ops.delete_event(TARGETCALENDARID, event['calendarId'])
        except:
            pass
        dba.delete_from_fct_by_calendarId(event['calendarId'], CONN)


def get_gcal_credentials():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('../token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                '../credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    return creds


def main():
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar.
    """
    global TARGETCALENDARID, CONN
    convert_data()
    data_intake()
    new_events = dba.fetch_from_fct_where_calId_is_null(CONN)
    ops = CalendarOps(credentials=get_gcal_credentials())
    old_events = dba.fetch_past_events_from_fct(CONN)

    for event in new_events:
        ops.create_event(TARGETCALENDARID, event, CONN)
    
    for event in old_events:
        ops.delete_event(TARGETCALENDARID, event.calendarId)
    
    dba.delete_old_events(CONN)


if __name__ == '__main__':
    main()
