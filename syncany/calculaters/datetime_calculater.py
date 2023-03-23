# -*- coding: utf-8 -*-
# 2023/2/19
# create by: snower

import datetime
import pytz
from ..utils import get_timezone, parse_datetime
from .calculater import Calculater


class TimeWindowCalculater(Calculater):
    def calculate(self, *args):
        if not args:
            return datetime.datetime.now(tz=get_timezone())

        time_period = args[0]
        if len(args) >= 2 and args[1]:
            if isinstance(args[1], datetime.datetime):
                dt = args[1]
            elif isinstance(args[1], str):
                try:
                    dt = parse_datetime(args[1], None, get_timezone())
                except:
                    dt = datetime.datetime.now(tz=get_timezone())
            else:
                dt = datetime.datetime.now(tz=get_timezone())
        else:
            dt = datetime.datetime.now(tz=get_timezone())
        offset = args[1] if len(args) >= 2 and isinstance(args[1], (int, float)) else \
            (args[2] if len(args) >= 3 and isinstance(args[2], (int, float)) else None)

        if time_period[-1] == "d":
            dt = datetime.datetime(dt.year, dt.month, dt.day - dt.day % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(days=int(time_period[:-1]) * offset)
            return dt
        if time_period[-1] == "H":
            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour - dt.hour % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(hours=int(time_period[:-1]) * offset)
            return dt
        if time_period[-1] == "M":
            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute - dt.minute % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(minutes=int(time_period[:-1]) * offset)
            return dt
        if time_period[-1] == "S":
            dt = datetime.datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second - dt.second % int(time_period[:-1]), tzinfo=dt.tzinfo)
            if offset:
                dt = dt + datetime.timedelta(seconds=int(time_period[:-1]) * offset)
            return dt
        if time_period[-1] == "w":
            windex = int(dt.strftime("%W"))
            dt = datetime.datetime(dt.year, dt.month, dt.day, tzinfo=dt.tzinfo)
            dt = dt - datetime.timedelta(days=((windex % int(time_period[:-1])) + (int(time_period[:-1]) * offset if offset else 0)) * 7)
            if len(args) >= 4 and isinstance(args[3], (int, float)) and 2 <= args[3] <= 7:
                dt = dt - datetime.timedelta(days=dt.weekday() - args[3] + 1)
            else:
                dt = dt - datetime.timedelta(days=dt.weekday())
            return dt
        return None


class DateTimeCalculater(Calculater):
    def calculate(self, *args):
        if not args:
            return None

        func_name = self.name[10:]
        if isinstance(args[0], (datetime.date, datetime.time)):
            if hasattr(args[0], func_name):
                return getattr(args[0], func_name)(*tuple(args[1:]))
            return None

        if func_name == "on":
            return self.on(*tuple(args))
        if func_name == "at":
            return self.at(*tuple(args))
        if func_name == "startofday":
            return self.at(args[0], 0)
        if func_name == "astimezone":
            if len(args) >= 2 and isinstance(args[1], str):
                return args[0].astimezone(pytz.timezone(args[1]))
            return args[0].astimezone(args[1])
        if hasattr(args[0], func_name):
            return getattr(args[0], func_name)(*tuple(args[1:]))
        return None

    def on(self, dt, year=None, month=None, day=None):
        return datetime.datetime(year if year is not None else dt.year, month if month is not None else dt.month,
                                 day if day is not None else dt.day, dt.hour, dt.minute, dt.second,
                                 dt.microsecond, tzinfo=dt.tzinfo)

    def at(self, dt, hour=0, minute=0, second=0, microsecond=0):
        return datetime.datetime(dt.year, dt.month, dt.day,
                                 hour if hour is not None else dt.hour, minute if minute is not None else dt.minute,
                                 second if second is not None else dt.second, microsecond if microsecond is not None else dt.microsecond,
                                 tzinfo=dt.tzinfo)
