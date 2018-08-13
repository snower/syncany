# -*- coding: utf-8 -*-
# 18/8/9
# create by: snower

import datetime
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None
from .filter import Filter

class IntFilter(Filter):
    def filter(self, value):
        try:
            return int(value)
        except:
            return 0

class FloatFilter(Filter):
    def filter(self, value):
        try:
            return float(value)
        except:
            return 0.0

class StringFilter(Filter):
    def filter(self, value):
        if value is None:
            return ""
        try:
            return str(value)
        except:
            return ""

class ObjectIdFilter(Filter):
    def __init__(self, *args, **kwargs):
        if ObjectId is None:
            raise ImportError(u"bson required")

        super(ObjectIdFilter, self).__init__(*args, **kwargs)

    def filter(self, value):
        try:
            return ObjectId(value)
        except:
            return ObjectId("000000000000000000000000")

class DateTimeFilter(Filter):
    def filter(self, value):
        try:
            return datetime.datetime.strptime(value, self.args or "%Y-%m-%d %H:%M:%S")
        except:
            return "0000-00-00 00:00:00"

class DateFilter(Filter):
    def filter(self, value):
        try:
            dt = datetime.datetime.strptime(value, self.args or "%Y-%m-%d")
            return datetime.date(dt.year, dt.month, dt.day)
        except:
            return "0000-00-00"