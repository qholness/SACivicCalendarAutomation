import unittest
import sys, os
sys.path.append(os.path.abspath('..'))
from CalendarData import CalendarData
from numpy import nan
from datetime import datetime


def test_generate_end_time():
   # Pandas datetime conversions result in permanent 24-hr time conversion
   # We will just test the time calculation
   test_time = '14:00'
   result_time = CalendarData.generate_end_time(test_time)
   assert result_time == '15:00'


def test_bad_form_data():
   assert CalendarData.generate_end_time(1) == CalendarData.__default_time__
