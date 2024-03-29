# -*- coding: utf-8 -*-
# 18/8/9
# create by: snower

import datetime
import time
import types
from decimal import Decimal
import pytz
import binascii
import uuid
try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None
from ..utils import NumberTypes, SequenceTypes, get_timezone, parse_datetime, parse_date, parse_time
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

        if isinstance(value, SequenceTypes):
            result = []
            for cv in value:
                result.append(self.filter(cv))
            return result

        if isinstance(value, set):
            result = set([])
            for cv in value:
                result.add(self.filter(cv))
            return result

        if isinstance(value, dict):
            result = {}
            for ck, cv in value.items():
                result[ck] = self.filter(cv)
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

        if isinstance(value, SequenceTypes):
            result = []
            for cv in value:
                result.append(self.filter(cv))
            return result

        if isinstance(value, set):
            result = set([])
            for cv in value:
                result.add(self.filter(cv))
            return result

        if isinstance(value, dict):
            result = {}
            for ck, cv in value.items():
                result[ck] = self.filter(cv)
            return result

        try:
            return float(value)
        except:
            return 0.0


class DecimalFilter(Filter):
    def filter(self, value):
        if isinstance(value, Decimal):
            return value

        if isinstance(value, float):
            return Decimal(value)

        if isinstance(value, int):
            return Decimal(value)

        if value is True:
            return Decimal(1.0)

        if value is None or value is False:
            return Decimal(0.0)

        if isinstance(value, datetime.datetime):
            try:
                return Decimal(value.timestamp())
            except:
                return Decimal(0)

        if isinstance(value, datetime.date):
            try:
                return Decimal(datetime.datetime(value.year, value.month, value.day).timestamp())
            except:
                return Decimal(0)

        if isinstance(value, datetime.timedelta):
            return Decimal(value.total_seconds())

        if isinstance(value, SequenceTypes):
            result = []
            for cv in value:
                result.append(self.filter(cv))
            return result

        if isinstance(value, set):
            result = set([])
            for cv in value:
                result.add(self.filter(cv))
            return result

        if isinstance(value, dict):
            result = {}
            for ck, cv in value.items():
                result[ck] = self.filter(cv)
            return result

        try:
            return Decimal(value)
        except:
            return Decimal(0)


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

        if isinstance(value, SequenceTypes):
            result = []
            for cv in value:
                result.append(self.filter(cv))
            return result

        if isinstance(value, set):
            result = set([])
            for cv in value:
                result.add(self.filter(cv))
            return result

        if isinstance(value, dict):
            result = {}
            for ck, cv in value.items():
                result[ck] = self.filter(cv)
            return result

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
                    return binascii.b2a_hex(value.encode(self.args or "utf-8"))
                return value.encode(self.args or "utf-8")
            except:
                return b""

        if isinstance(value, SequenceTypes):
            result = []
            for cv in value:
                result.append(self.filter(cv))
            return result

        if isinstance(value, set):
            result = set([])
            for cv in value:
                result.add(self.filter(cv))
            return result

        if isinstance(value, dict):
            result = {}
            for ck, cv in value.items():
                result[ck] = self.filter(cv)
            return result

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

        if isinstance(value, SequenceTypes):
            result = []
            for cv in value:
                result.append(self.filter(cv))
            return result

        if isinstance(value, set):
            result = set([])
            for cv in value:
                result.add(self.filter(cv))
            return result

        if isinstance(value, dict):
            result = {}
            for ck, cv in value.items():
                result[ck] = self.filter(cv)
            return result

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

        if isinstance(value, tuple):
            return list(value)

        if isinstance(value, set):
            return list(value)

        if value is None:
            return []

        if isinstance(value, types.GeneratorType):
            values = []
            while True:
                try:
                    values.append(value.send(None))
                except StopIteration:
                    break
            return values
        return [value]


class SetFilter(Filter):
    def filter(self, value):
        if isinstance(value, set):
            return value

        if isinstance(value, SequenceTypes):
            return set(value)

        if value is None:
            return set([])

        return {value}


class MapFilter(Filter):
    def filter(self, value):
        if not value:
            return {}

        if isinstance(value, dict):
            if self.args:
                return {value[self.args]: value} if self.args in value else {}
            return value

        if isinstance(value, set):
            value = list(value)
        if isinstance(value, SequenceTypes):
            if self.args:
                result = {}
                for v in value:
                    if not isinstance(v, dict) or self.args not in v:
                        continue
                    vk = v[self.args]
                    if vk in result:
                        if not isinstance(result[vk], list):
                            result[vk] = [result[vk], v]
                        else:
                            result[vk].append(v)
                    else:
                        result[vk] = v
                return result

            if len(value) == 1 and isinstance(value[0], dict):
                return value[0]

            if all([isinstance(v, dict) for v in value]):
                return {"index" + str(i): value[i] for i in range(len(value))}

            if all([isinstance(v, SequenceTypes) and len(v) == 2 for v in value]):
                return {v[0]: v[1] for v in value}

            try:
                return {v: None for v in value}
            except:
                return {"index" + str(i): value[i] for i in range(len(value))}

        try:
            return {value: None}
        except:
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

        if isinstance(value, SequenceTypes):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, set):
            results = set([])
            for cv in value:
                results.add(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        if isinstance(value, NumberTypes):
            return ObjectId.from_datetime(datetime.datetime.fromtimestamp(int(value), pytz.timezone(self.args) if self.args else pytz.UTC))

        if isinstance(value, datetime.datetime):
            return ObjectId.from_datetime(value)

        try:
            return ObjectId(value)
        except:
            try:
                return ObjectId.from_datetime(parse_datetime(value, self.args, get_timezone()))
            except:
                return ObjectId("000000000000000000000000")


class UUIDFilter(Filter):
    def filter(self, value):
        if isinstance(value, uuid.UUID):
            return value

        if value is None:
            return uuid.UUID("00000000-0000-0000-0000-000000000000")

        if value is True:
            return uuid.UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")

        if value is False:
            return uuid.UUID("00000000-0000-0000-0000-000000000000")

        if isinstance(value, SequenceTypes):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, set):
            results = set([])
            for cv in value:
                results.add(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        if isinstance(value, datetime.datetime):
            value = value.timestamp()
        if isinstance(value, NumberTypes):
            if value <= 0xffffffff:
                timestamp = int(time.time())
                return uuid.UUID(fields=(timestamp & 0xffffffff, (timestamp >> 32) & 0xffff,
                                         (timestamp >> 48) & 0x0fff, 0, 0, 0), version=1)
            return uuid.UUID(int=int(value))
        try:
            return uuid.UUID(value)
        except:
            return uuid.UUID("00000000-0000-0000-0000-000000000000")


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
        if value is None:
            return None
        localzone = get_timezone()
        if isinstance(value, datetime.datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=localzone)
            elif localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            if self.dtformat:
                value = datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat)
                if value.tzinfo is None:
                    value = value.replace(tzinfo=localzone)
                elif localzone != value.tzinfo:
                    value = value.astimezone(tz=localzone)
            return value

        if isinstance(value, datetime.timedelta):
            value = datetime.datetime.now(tz=localzone) + value
            if self.dtformat:
                value = datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat)
                if value.tzinfo is None:
                    value = value.replace(tzinfo=localzone)
                elif localzone != value.tzinfo:
                    value = value.astimezone(tz=localzone)
            return value

        if isinstance(value, NumberTypes):
            value = datetime.datetime.fromtimestamp(int(value), pytz.timezone(self.tzname) if self.tzname else pytz.UTC)
            if value.tzinfo is None:
                value = value.replace(tzinfo=localzone)
            elif localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            if self.dtformat:
                value = datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat)
                if value.tzinfo is None:
                    value = value.replace(tzinfo=localzone)
                elif localzone != value.tzinfo:
                    value = value.astimezone(tz=localzone)
            return value

        if isinstance(value, SequenceTypes):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, set):
            results = set([])
            for cv in value:
                results.add(self.filter(cv))
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
            if value.tzinfo is None:
                value = value.replace(tzinfo=localzone)
            elif localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            if self.dtformat:
                value = datetime.datetime.strptime(value.strftime(self.dtformat), self.dtformat)
                if value.tzinfo is None:
                    return value.replace(tzinfo=localzone)
                if value.tzinfo != localzone:
                    return value.astimezone(tz=localzone)
            return value

        try:
            return parse_datetime(value, self.dtformat, localzone)
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
        if value is None:
            return None

        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                localzone = get_timezone()
                if value.tzinfo is None:
                    value = value.replace(tzinfo=localzone)
                elif localzone != value.tzinfo:
                    value = value.astimezone(tz=localzone)
                return datetime.date(value.year, value.month, value.day)
            return value

        if isinstance(value, datetime.timedelta):
            localzone = get_timezone()
            dt = datetime.datetime.now(tz=localzone)
            return datetime.date(dt.year, dt.month, dt.day) + value

        if isinstance(value, NumberTypes):
            localzone = get_timezone()
            value = datetime.datetime.fromtimestamp(int(value), pytz.timezone(self.args) if self.args else pytz.UTC)
            if value.tzinfo is None:
                value = value.replace(tzinfo=localzone)
            elif localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            return datetime.date(value.year, value.month, value.day)

        if isinstance(value, SequenceTypes):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, set):
            results = set([])
            for cv in value:
                results.add(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        try:
            dt = parse_date(value, self.args, get_timezone())
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
        if value is None:
            return None
        if isinstance(value, datetime.time):
            return value

        if isinstance(value, datetime.date):
            if isinstance(value, datetime.datetime):
                localzone = get_timezone()
                if value.tzinfo is None:
                    value = value.replace(tzinfo=localzone)
                elif localzone != value.tzinfo:
                    value = value.astimezone(tz=localzone)
                return datetime.time(value.hour, value.minute, value.second)
            return datetime.time(0, 0, 0)

        if isinstance(value, datetime.timedelta):
            localzone = get_timezone()
            dt = datetime.datetime.now(tz=localzone) + value
            return datetime.time(dt.hour, dt.minute, dt.second)

        if isinstance(value, NumberTypes):
            localzone = get_timezone()
            value = datetime.datetime.fromtimestamp(int(value), pytz.timezone(self.args) if self.args else pytz.UTC)
            if value.tzinfo is None:
                value = value.replace(tzinfo=localzone)
            elif localzone != value.tzinfo:
                value = value.astimezone(tz=localzone)
            return datetime.time(value.hour, value.minute, value.second)

        if isinstance(value, SequenceTypes):
            results = []
            for cv in value:
                results.append(self.filter(cv))
            return results

        if isinstance(value, set):
            results = set([])
            for cv in value:
                results.add(self.filter(cv))
            return results

        if isinstance(value, dict):
            results = {}
            for ck, cv in value.items():
                results[ck] = self.filter(cv)
            return value

        try:
            dt = parse_time(value, self.args, get_timezone())
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