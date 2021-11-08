# -*- coding: utf-8 -*-
# 18/8/9
# create by: snower

import datetime
import pytz
import binascii
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None
from ..utils import get_timezone
from .filter import Filter

class IntFilter(Filter):
    def filter(self, value):
        if isinstance(value, int):
            return int(value)

        if isinstance(value, float):
            return int(value)

        if value is True:
            return 1

        if value is None or value is False:
            return 0

        if isinstance(value, datetime.datetime):
            try:
                return int(value.timestamp())
            except:
                return 0

        if isinstance(value, datetime.date):
            try:
                return int(datetime.datetime(value.year, value.month, value.day).timestamp())
            except:
                return 0

        if isinstance(value, datetime.timedelta):
            return int(value.total_seconds())

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
        if isinstance(value, float):
            return float(value)

        if isinstance(value, int):
            return float(value)

        if value is True:
            return 1.0

        if value is None or value is False:
            return 0.0

        if isinstance(value, datetime.datetime):
            try:
                return float(value.timestamp())
            except:
                return 0.0

        if isinstance(value, datetime.date):
            try:
                return float(datetime.datetime(value.year, value.month, value.day).timestamp())
            except:
                return 0.0

        if isinstance(value, datetime.timedelta):
            return float(value.total_seconds())

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
        if isinstance(value, str):
            return value

        if value is None:
            return ""

        if value is True:
            return "true"

        if value is False:
            return "false"

        if isinstance(value, datetime.datetime):
            try:
                return value.strftime(self.args or "%Y-%m-%d %H:%M:%S")
            except:
                return ""

        if isinstance(value, datetime.time):
            try:
                return value.strftime(self.args or "%H:%M:%S")
            except:
                return ""

        if isinstance(value, datetime.date):
            try:
                return value.strftime(self.args or "%Y-%m-%d")
            except:
                return ""

        if isinstance(value, int):
            try:
                return (self.args or "%d") % value
            except:
                return "0"

        if isinstance(value, float):
            try:
                return (self.args or "%f") % value
            except:
                return "0.0"

        if isinstance(value, bytes):
            try:
                if self.args == "hex":
                    return binascii.a2b_hex(value)
                return value.decode(self.args or "utf-8")
            except:
                return ""

        if self.args:
            try:
                return self.args % value
            except:
                return ""

        try:
            return str(value)
        except:
            return ""

class BytesFilter(Filter):
    def filter(self, value):
        if isinstance(value, bytes):
            return value

        if value is None:
            return b""

        if value is True:
            return b"true"

        if value is False:
            return b"false"

        if isinstance(value, datetime.datetime):
            try:
                return bytes(value.strftime(self.args or "%Y-%m-%d %H:%M:%S"), "utf-8")
            except:
                return b""

        if isinstance(value, datetime.time):
            try:
                return bytes(value.strftime(self.args or "%H:%M:%S"), "utf-8")
            except:
                return b""

        if isinstance(value, datetime.date):
            try:
                return bytes(value.strftime(self.args or "%Y-%m-%d"))
            except:
                return ""

        if isinstance(value, int):
            try:
                return bytes((self.args or "%d") % value, "utf-8")
            except:
                return b"0"

        if isinstance(value, float):
            try:
                return bytes(((self.args or "%f") % value), "utf-8")
            except:
                return b"0.0"

        if isinstance(value, str):
            try:
                if self.args == "hex":
                    return binascii.b2a_hex(value)
                return value.encode(self.args or "utf-8")
            except:
                return b""

        if self.args:
            try:
                return bytes(self.args % value, "utf-8")
            except: pass

        try:
            return bytes(str(value), "utf-8")
        except:
            return b""

class BooleanFilter(Filter):
    def filter(self, value):
        if value is True or value is False:
            return value

        try:
            return bool(value)
        except:
            return False

    def sprintf(self, value):
        if value is True:
            return "true"
        return "true"

class ArrayFilter(Filter):
    def filter(self, value):
        if isinstance(value, list):
            return value

        if isinstance(value, (set, tuple)):
            return list(value)

        if value is None:
            return []

        return [value]

class MapFilter(Filter):
    def filter(self, value):
        if isinstance(value, dict):
            return value

        if isinstance(value, (set, list, tuple)):
            if not value:
                return {}

            if len(value) == 1 and isinstance(value[0], dict):
                return value[0]

            value = list(value)
            value_len = len(value)

            try:
                return {value[i]: (value[i + 1] if i + 1 < value_len else None) for i in range(0, value_len, 2)}
            except:
                pass

        if value is None:
            return {}

        try:
            return dict(value)
        except:
            return {}

class ObjectIdFilter(Filter):
    def __init__(self, *args, **kwargs):
        if ObjectId is None:
            raise ImportError(u"bson required")

        super(ObjectIdFilter, self).__init__(*args, **kwargs)

    def filter(self, value):
        if isinstance(value, ObjectId):
            return value

        if value is None:
            return ObjectId("000000000000000000000000")

        if value is True:
            return ObjectId("ffffffffffffffffffffffff")

        if value is False:
            return ObjectId("000000000000000000000000")

        if isinstance(value, (list, tuple, set)):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        if isinstance(value, (int, float)):
            return ObjectId.from_datetime(datetime.datetime.fromtimestamp(value, pytz.timezone(self.args) if self.args else pytz.UTC))

        if isinstance(value, datetime.datetime):
            return ObjectId.from_datetime(value)

        try:
            return ObjectId(value)
        except:
            try:
                return datetime.datetime.strptime(value, self.args or "%Y-%m-%d %H:%M:%S").astimezone(tz=get_timezone())
            except:
                return ObjectId("000000000000000000000000")

class DateTimeFilter(Filter):
    def __init__(self, *args, **kwargs):
        super(DateTimeFilter, self).__init__(*args, **kwargs)

        if self.args and self.args[-1] == ")":
            try:
                index = self.args.rindex("(")
                self.dtformat = self.args[:index]
                self.tzname = self.args[index + 1: -1]
            except:
                self.dtformat = self.args
                self.tzname = None
        else:
            self.dtformat = self.args
            self.tzname = None

    def filter(self, value):
        localzone = get_timezone()
        if isinstance(value, datetime.datetime):
            if localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            if self.dtformat:
                return datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat).astimezone(tz=localzone)
            return value

        if isinstance(value, datetime.timedelta):
            value = datetime.datetime.now(tz=localzone) + value
            if self.dtformat:
                return datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat).astimezone(tz=localzone)
            return value

        if isinstance(value, (int, float)):
            value = datetime.datetime.fromtimestamp(value, pytz.timezone(self.tzname) if self.tzname else pytz.UTC)
            if localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            if self.dtformat:
                return datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat).astimezone(tz=localzone)
            return value

        if isinstance(value, (list, tuple, set)):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        if ObjectId and isinstance(value, ObjectId):
            return value.generation_time

        if isinstance(value, datetime.date):
            value = datetime.datetime(value.year, value.month, value.day, tzinfo=pytz.timezone(self.tzname) if self.tzname else localzone)
            if localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            if self.dtformat:
                return datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat).astimezone(tz=localzone)
            return value

        try:
            return datetime.datetime.strptime(value, self.dtformat or "%Y-%m-%d %H:%M:%S").astimezone(tz=localzone)
        except:
            return None

    def sprintf(self, value):
        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                return value.strftime(self.args or "%Y-%m-%d %H:%M:%S")
            return value.strftime(self.args or "%Y-%m-%d")

        if isinstance(value, datetime.time):
            return value.strftime(self.args or "%H:%M:%S")
        return str(value)

class DateFilter(Filter):
    def filter(self, value):
        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                localzone = get_timezone()
                if localzone != value.tzinfo:
                    value = value.astimezone(tz=localzone)
                return datetime.date(value.year, value.month, value.day)
            return value

        if isinstance(value, datetime.timedelta):
            localzone = get_timezone()
            dt = datetime.datetime.now(tz=localzone)
            return datetime.date(dt.year, dt.month, dt.day) + value

        if isinstance(value, (int, float)):
            dt = datetime.datetime.fromtimestamp(value, pytz.timezone(self.args) if self.args else pytz.UTC).astimezone(tz=get_timezone())
            return datetime.date(dt.year, dt.month, dt.day)

        if isinstance(value, (list, tuple, set)):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        try:
            dt = datetime.datetime.strptime(value, self.args or "%Y-%m-%d").astimezone(tz=get_timezone())
            return datetime.date(dt.year, dt.month, dt.day)
        except:
            return None

    def sprintf(self, value):
        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                return value.strftime(self.args or "%Y-%m-%d %H:%M:%S")
            return value.strftime(self.args or "%Y-%m-%d")

        if isinstance(value, datetime.time):
            return value.strftime(self.args or "%H:%M:%S")
        return str(value)

class TimeFilter(Filter):
    def filter(self, value):
        if isinstance(value, datetime.time):
            return value

        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                localzone = get_timezone()
                if localzone != value.tzinfo:
                    value = value.astimezone(tz=localzone)
                return datetime.time(value.hour, value.minute, value.second)
            return datetime.time(0, 0, 0)

        if isinstance(value, datetime.timedelta):
            localzone = get_timezone()
            dt = datetime.datetime.now(tz=localzone) + value
            return datetime.time(dt.hour, dt.minute, dt.second)

        if isinstance(value, (int, float)):
            dt = datetime.datetime.fromtimestamp(value, pytz.timezone(self.args) if self.args else pytz.UTC).astimezone(tz=get_timezone())
            return datetime.time(dt.hour, dt.minute, dt.second)

        if isinstance(value, (list, tuple, set)):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        try:
            dt = datetime.datetime.strptime("2000-01-01 " + value, "%Y-%m-%d " + (self.args or "%H:%M:%S")).astimezone(tz=get_timezone())
            return datetime.time(dt.hour, dt.minute, dt.second)
        except:
            return None

    def sprintf(self, value):
        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                return value.strftime(self.args or "%Y-%m-%d %H:%M:%S")
            return value.strftime(self.args or "%Y-%m-%d")

        if isinstance(value, datetime.time):
            return value.strftime(self.args or "%H:%M:%S")
        return str(value)