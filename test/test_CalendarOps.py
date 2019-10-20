"""
Test cases
    
"""
import sys, os
sys.path.append(os.path.abspath('..'))
from gcal.CalendarOps  import CalendarOps, check_nat_str

def test_nat_contains_NaT():
    assert check_nat_str('lkasd') == False
    assert check_nat_str('nat') == True


def test_nat_receives_bad_type():
    assert check_nat_str(1) == False
