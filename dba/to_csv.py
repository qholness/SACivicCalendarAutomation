from pandas import to_datetime, read_html
from datetime import datetime


def drop_old_data(data):
    data['Meeting Date'] = to_datetime(data['Meeting Date'])
    today_mask = data['Meeting Date'] > datetime.today()
    return data[today_mask]


def csv_from_excel(input_data_path, output_data_path, sheet_target='Sheet 1'):
    data = read_html(input_data_path)[0]
    data = drop_old_data(data)
    data.to_csv(output_data_path, encoding='utf-8', index=False)
