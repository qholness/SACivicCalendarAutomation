import sys, os
sys.path.append(os.path.abspath('..'))
from data.CalendarData import CalendarData
from datetime import datetime


def test_end_time_is_one_hour_greater():
   # Pandas datetime conversions result in permanent 24-hr time conversion
   # We will just test the time calculation
   test_time = '14:00'
   result_time = CalendarData.generate_end_time(test_time)
   assert result_time == '15:00'


def test_end_time_from_23_is_00():
   test_time = '23:00'
   result_time = CalendarData.generate_end_time(test_time)
   assert result_time == '0:00'


def test_bad_form_data():
   assert CalendarData.generate_end_time(1) == CalendarData.__default_time__
