import atexit
import gcal
from gcal.CalendarOps import CalendarOps
from sqlalchemy.engine import Connection
import loguru


def teardown(cfg, ops: CalendarOps, tgt: str, con: Connection):
    if cfg['DEFAULT']['ENV'] in ('DEV', 'TEST'):
        @atexit.register
        def dev_teardown():
            loguru.logger.info("Performing dev teardown")
            gcal.cleanup_calendar(ops, tgt, con)
            loguru.logger.info("Teardown complete")