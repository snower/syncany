# -*- coding: utf-8 -*-
# 2023/2/22
# create by: snower

class TaskerIterator(object):
    def close(self):
        raise NotImplementedError()


class TaskerDataIterator(TaskerIterator):
    def __init__(self, datas):
        self._datas = datas

    @property
    def datas(self):
        return self._datas

    def close(self):
        self._datas = None


class TaskerFileIterator(TaskerIterator):
    def __init__(self, fp, fields):
        self._fp = fp
        self.fields = fields
        self.offset = 0

    @property
    def fp(self):
        return self._fp

    def close(self):
        if not self._fp:
            return
        self._fp.close()
        self._fp = None
