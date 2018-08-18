# -*- coding: utf-8 -*-
# 18/8/9
# create by: snower

import time
import datetime
import binascii
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None
from .filter import Filter

class IntFilter(Filter):
    def filter(self, value):
        if value is None:
            return 0

        if isinstance(value, datetime.datetime):
            try:
                return time.mktime(value.timetuple())
            except:
                return 0

        if isinstance(value, datetime.date):
            try:
                return time.mktime(value.timetuple())
            except:
                return 0

        if isinstance(value, (list, tuple, set)):
            result = 0
            for cv in value:
                result += self.filter(cv)
            return result

        if isinstance(value, dict):
            result = 0
            for ck, cv in value.items():
                result += self.filter(cv)
            return result

        try:
            return int(value)
        except:
            return 0

class FloatFilter(Filter):
    def filter(self, value):
        if value is None:
            return 0.0

        if isinstance(value, datetime.datetime):
            try:
                return time.mktime(value.timetuple())
            except:
                return 0.0

        if isinstance(value, datetime.date):
            try:
                return time.mktime(value.timetuple())
            except:
                return 0.0

        if isinstance(value, (list, tuple, set)):
            result = 0.0
            for cv in value:
                result += self.filter(cv)
            return result

        if isinstance(value, dict):
            result = 0.0
            for ck, cv in value.items():
                result += self.filter(cv)
            return result

        try:
            return float(value)
        except:
            return 0.0

class StringFilter(Filter):
    def filter(self, value):
        if value is None:
            return ""

        if isinstance(value, datetime.datetime):
            try:
                return value.strftime(self.args or "%Y-%m-%d %H:%M:%S")
            except:
                return "0000-00-00 00:00:00"

        if isinstance(value, datetime.date):
            try:
                return value.strftime(self.args or "%Y-%m-%d")
            except:
                return "0000-00-00"

        if isinstance(value, int):
            try:
                return (self.args or "%d" % value)
            except:
                return 0

        if isinstance(value, float):
            try:
                return (self.args or "%f" % value)
            except:
                return 0.0

        if isinstance(value, bytes):
            try:
                if self.args == "hex":
                    return binascii.a2b_hex(value)
                return value.decode(self.args or "utf-8")
            except:
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
        if isinstance(value, (list, tuple, set)):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, dict):
            return value

        if isinstance(value, (int, float)):
            return ObjectId.from_datetime(datetime.datetime.fromtimestamp(int(value)))

        try:
            return ObjectId(value)
        except:
            return ObjectId("000000000000000000000000")

class DateTimeFilter(Filter):
    def filter(self, value):
        if isinstance(value, datetime.datetime):
            return value

        if isinstance(value, (int, float)):
            return datetime.datetime.utcfromtimestamp(int(value))

        if isinstance(value, (list, tuple, set)):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, dict):
            return value

        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)

        try:
            return datetime.datetime.strptime(value, self.args or "%Y-%m-%d %H:%M:%S")
        except:
            return "0000-00-00 00:00:00"

class DateFilter(Filter):
    def filter(self, value):
        if isinstance(value, datetime.date):
            return value

        if isinstance(value, (int, float)):
            dt = datetime.datetime.utcfromtimestamp(int(value))
            return datetime.date(dt.year, dt.month, dt.day)

        if isinstance(value, (list, tuple, set)):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, dict):
            return value

        if isinstance(value, datetime.datetime):
            return datetime.date(value.year, value.month, value.day)

        try:
            dt = datetime.datetime.strptime(value, self.args or "%Y-%m-%d")
            return datetime.date(dt.year, dt.month, dt.day)
        except:
            return "0000-00-00"