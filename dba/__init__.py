import os.path
from pandas import DataFrame
from sqlalchemy.engine import Connection, ResultProxy
from sqlalchemy.orm import sessionmaker
from loguru import logger
from tqdm import tqdm
from data.CalendarData import CalendarData
from dba import to_csv


def get_file(_f):
    # return os.path.join(config['DB']['SQL_DIR'], _f) 
    return os.path.join('sql', _f) 


def create_stg(conn):
    with open(get_file('create_stg.sql'), 'r+') as _f:
        conn.execute(' '.join(_f.readlines()))


def create_fct(conn):
    with open(get_file('create_fct.sql'), 'r+') as _f:
        conn.execute(' '.join(_f.readlines()))


def drop_stg(conn):
    with open(get_file('drop_stg.sql'), 'r+') as _f:
        conn.execute(' '.join(_f.readlines()))


def drop_fct(conn):
    with open(get_file('drop_fct.sql'), 'r+') as _f:
        conn.execute(' '.join(_f.readlines()))


def build_schema(conn, drop_all: bool=False):
    if drop_all:
        drop_stg(conn)
        drop_fct(conn)
    create_stg(conn)
    create_fct(conn)


def truncate_stg(conn):
    with open(get_file('truncate_stg.sql'), 'r+') as _f:
        conn.execute(' '.join(_f.readlines()))
    logger.info("Stg table truncated")


def update_staging_table(data: DataFrame, conn: Connection):
    logger.info("Updating staging table")
    data.to_sql(
        name='stg_calendar_data', con=conn,
        if_exists='replace',index=False)
    logger.info("Update complete")


def migrate_stg_to_fct(conn):
    logger.info("Running migration from stg to fct")
    with open(get_file('migrate_stg_to_fct.sql'), 'r') as _f:
        query = ' '.join(_f.readlines())
        conn.execute(query)
    logger.info("Migration complete")


def fetch_from_fct_where_calendarId_is_null(conn):
    logger.info("Fetching fct data where calendarId is null")
    with open(get_file('get_fct_calendar_data_where_null.sql'), 'r') as _f:
        query = ' '.join(_f.readlines())
        res = conn.execute(query)
    logger.info(f"Returning { res.rowcount } rows.")
    return res.fetchall()


def delete_from_fct_by_calendarId(calendarId: str, conn: Connection):
    query = f"DELETE FROM fct_calendar_data WHERE calendarId='{ calendarId }'"
    conn.execute(query)


def delete_old_events(conn: Connection):
    logger.info(f"Deleting old events from database.")
    with open(get_file('delete_old_events_from_fct.sql'), 'r') as _f:
        query = ' '.join(_f.readlines())
        res = conn.execute(query)
    logger.info(f"{ res.rowcount } events deleted.")


def fetch_past_events_from_fct(conn):
    logger.info("Fetching past events from fct")
    with open(get_file('get_past_events_from_fct.sql'), 'r') as _f:
        query = ' '.join(_f.readlines())
        return conn.execute(query).fetchall()


def convert_data(cfg):
    logger.info("Converting from bad format")
    to_csv.csv_from_excel(
        cfg['INPUTDATA']['SOURCE'],
        cfg['INPUTDATA']['TARGET'])


def data_intake(cfg, con):
    logger.info("Running data intake")
    DF = CalendarData(source_path=cfg['INPUTDATA']['TARGET'])
    truncate_stg(con)
    update_staging_table(DF.data, con)
    migrate_stg_to_fct(con)