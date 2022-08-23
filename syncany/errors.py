# -*- coding: utf-8 -*-
# 2020/7/31
# create by: snower

class SyncanyException(Exception):
    pass


class ConfigReaderUnknownException(SyncanyException):
    pass


class LoaderUnknownException(SyncanyException):
    pass


class OutputerUnknownException(SyncanyException):
    pass


class ValuerUnknownException(SyncanyException):
    pass


class FilterUnknownException(SyncanyException):
    pass


class CalculaterUnknownException(SyncanyException):
    pass


class DatabaseUnknownException(SyncanyException):
    pass


class CacheUnknownException(SyncanyException):
    pass


class SourceUnknownException(SyncanyException):
    pass