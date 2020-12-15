# -*- coding: utf-8 -*-
# 2020/11/10
# create by: snower

class Hooker(object):
    def compiled(self, tasker):
        pass

    def queried(self, tasker, datas):
        return datas

    def loaded(self, tasker, datas):
        return datas

    def outputed(self, takser, datas):
        pass

class PipelinesHooker(Hooker):
    def __init__(self, compiled_valuers=None, queried_valuers=None, loaded_valuers=None, outputed_valuers=None):
        self.compiled_valuers = compiled_valuers
        self.queried_valuers = queried_valuers
        self.loaded_valuers = loaded_valuers
        self.outputed_valuers = outputed_valuers

    def compiled(self, tasker):
        if not self.compiled_valuers:
            return None

        datas = None
        for valuer in self.compiled_valuers:
            valuer.fill(datas)
            datas = valuer.get()
        return datas

    def queried(self, tasker, datas):
        if not self.queried_valuers:
            return datas

        for valuer in self.queried_valuers:
            valuer.fill(datas)
            datas = valuer.get()
        return datas

    def loaded(self, tasker, datas):
        if not self.loaded_valuers:
            return datas

        for valuer in self.loaded_valuers:
            valuer.fill(datas)
            datas = valuer.get()
        return datas

    def outputed(self, takser, datas):
        if not self.outputed_valuers:
            return datas

        for valuer in self.outputed_valuers:
            valuer.fill(datas)
            datas = valuer.get()
        return datas