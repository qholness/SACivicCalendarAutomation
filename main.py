import dba
import click
import atexit
import common
import gcal
from sqlalchemy import create_engine
from CalendarOps import CalendarOps
from config import config_factory
from security import get_gcal_credentials


@click.command()
@click.option('--config', default='config.ini', help="Path to config file.")
@click.option('--clear', is_flag=True, default=False, help='Clear the calendar')
def main(config, clear):
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar."""
    CONFIG = config_factory(config)
    CONN = create_engine(CONFIG['DB']['CON_STRING']).connect()
    TARGETCALENDARID = CONFIG['CALENDAR']['TARGET']
    
    # DBA Operations
    dba.build_schema(CONN, drop_all=clear)
    dba.convert_data(CONFIG)
    dba.data_intake(CONFIG, CONN)

    # Calendar and DB Ops
    ops = CalendarOps(credentials=get_gcal_credentials(CONFIG))
    common.create_new_events(TARGETCALENDARID, dba.fetch_from_fct_where_calendarId_is_null(CONN), ops, CONN)
    common.remove_old_events(TARGETCALENDARID, dba.fetch_past_events_from_fct(CONN), ops, CONN)

    if clear:
        gcal.cleanup_calendar(ops, TARGETCALENDARID, CONN)
    
    if CONFIG['DEFAULT']['ENV'] in ('DEV', 'TEST'):
        @atexit.register
        def dev_teardown():
            gcal.cleanup_calendar(ops, TARGETCALENDARID, CONN)


if __name__ == '__main__':
    main()
