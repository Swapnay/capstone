import os
from datetime import datetime


class UdfUtils:

    @staticmethod
    def split_str(metro_str):
        return metro_str.split(',')[0]

    @staticmethod
    def get_month(month):
        return month.split("M")[1]

    @staticmethod
    def get_code(text):
        if text.startswith('LAS'):
            return text[len('LAS'):-2]
        return text

    def convert_to_date(dateTimeStr):
        return datetime.strptime(dateTimeStr, '%m/%d/%Y')

    def convert_to_date_world(dateTimeStr):
        if isinstance(dateTimeStr,str):
            return datetime.strptime(dateTimeStr, '%Y-%m-%d')
        return dateTimeStr



