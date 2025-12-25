# -*- coding: utf-8 -*-
# 2020/7/31
# create by: snower

import logging
import threading
from .utils import beautify_print

__logger = logging
__verbose_logger = None


def get_logger():
    return __logger


def set_logger(logger):
    global __logger
    __logger = logger


def set_verbose_logger(verbose_logger):
    global __verbose_logger
    if __verbose_logger is None:
        __verbose_logger = threading.local()
    __verbose_logger.verbose_logger = verbose_logger


def get_verbose_logger():
    if __verbose_logger is None:
        return beautify_print
    try:
        return __verbose_logger.verbose_logger
    except AttributeError:
        return beautify_print