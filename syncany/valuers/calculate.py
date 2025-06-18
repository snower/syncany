# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

from .valuer import Valuer


class CalculateValuer(Valuer):
    def __init__(self, calculater, args_valuers, return_valuer, inherit_valuers, *args, **kwargs):
        self.calculater = calculater
        self.args_valuers = args_valuers
        self.return_valuer = return_valuer
        self.inherit_valuers = inherit_valuers
        super(CalculateValuer, self).__init__(*args, **kwargs)

    def new_init(self):
        super(CalculateValuer, self).new_init()
        self.args_wait_loaded = False
        for valuer in self.args_valuers:
            if valuer.require_loaded():
                self.args_wait_loaded = True
                break
        self.wait_loaded = True if self.return_valuer and self.return_valuer.require_loaded() else False
        if not self.args_wait_loaded and not self.wait_loaded and not self.filter:
            self.optimize_fast_calculate()

    def clone_init(self, from_valuer):
        super(CalculateValuer, self).clone_init(from_valuer)
        self.args_wait_loaded = from_valuer.args_wait_loaded
        self.wait_loaded = from_valuer.wait_loaded
        if not self.args_wait_loaded and not self.wait_loaded and not self.filter:
            self.optimize_fast_calculate()

    def optimize_fast_calculate(self):
        args_count = len(self.args_valuers)
        if args_count == 0:
            if not self.inherit_valuers and not self.return_valuer:
                self.fill_get = lambda _: self.calculater.calculate()
            else:
                self.fill_get = self.fill_get0
        elif args_count == 1:
            self.args_fill_get0 = self.args_valuers[0].fill_get
            if not self.inherit_valuers and not self.return_valuer:
                self.fill_get = lambda data: self.calculater.calculate(self.args_fill_get0(data))
            else:
                self.fill_get = self.fill_get1
        elif args_count == 2:
            self.args_fill_get0 = self.args_valuers[0].fill_get
            self.args_fill_get1 = self.args_valuers[1].fill_get
            if not self.inherit_valuers and not self.return_valuer:
                self.fill_get = lambda data: self.calculater.calculate(self.args_fill_get0(data), self.args_fill_get1(data))
            else:
                self.fill_get = self.fill_get2
        elif args_count == 3:
            self.args_fill_get0 = self.args_valuers[0].fill_get
            self.args_fill_get1 = self.args_valuers[1].fill_get
            self.args_fill_get2 = self.args_valuers[2].fill_get
            if not self.inherit_valuers and not self.return_valuer:
                self.fill_get = lambda data: self.calculater.calculate(self.args_fill_get0(data), self.args_fill_get1(data),
                                                                       self.args_fill_get2(data))
            else:
                self.fill_get = self.fill_get3
        elif args_count == 4:
            self.args_fill_get0 = self.args_valuers[0].fill_get
            self.args_fill_get1 = self.args_valuers[1].fill_get
            self.args_fill_get2 = self.args_valuers[2].fill_get
            self.args_fill_get3 = self.args_valuers[3].fill_get
            if not self.inherit_valuers and not self.return_valuer:
                self.fill_get = lambda data: self.calculater.calculate(self.args_fill_get0(data), self.args_fill_get1(data),
                                                                       self.args_fill_get2(data), self.args_fill_get3(data))
            else:
                self.fill_get = self.fill_get4

    def add_inherit_valuer(self, valuer):
        self.inherit_valuers.append(valuer)

    def mount_scoper(self, scoper=None, is_return_getter=True,**kwargs):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.mount_scoper(scoper=scoper, is_return_getter=False,**kwargs)
        for valuer in self.args_valuers:
            valuer.mount_scoper(scoper=scoper, is_return_getter=False,**kwargs)
        if self.return_valuer:
            self.return_valuer.mount_scoper(scoper=self, is_return_getter=is_return_getter and True, **kwargs)

    def clone(self, contexter=None, **kwargs):
        inherit_valuers = [inherit_valuer.clone(contexter, **kwargs)
                           for inherit_valuer in self.inherit_valuers] if self.inherit_valuers else None
        args_valuers = []
        for valuer in self.args_valuers:
            args_valuers.append(valuer.clone(contexter, **kwargs))
        return_valuer = self.return_valuer.clone(contexter, **kwargs) if self.return_valuer else None
        if contexter is not None:
            return ContextCalculateValuer(self.calculater, args_valuers, return_valuer, inherit_valuers,
                                          self.key, self.filter, from_valuer=self, contexter=contexter)
        if isinstance(self, ContextCalculateValuer):
            return ContextCalculateValuer(self.calculater, args_valuers, return_valuer, inherit_valuers,
                                          self.key, self.filter, from_valuer=self, contexter=self.contexter)
        return self.__class__(self.calculater, args_valuers, return_valuer, inherit_valuers,
                              self.key, self.filter, from_valuer=self)

    def fill(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if not self.args_wait_loaded:
            values = (valuer.fill_get(data) for valuer in self.args_valuers)
            if self.return_valuer:
                if not self.wait_loaded:
                    self.value = self.return_valuer.fill_get(self.do_filter(self.calculater.calculate(*values)))
                    return self
                self.return_valuer.fill(self.do_filter(self.calculater.calculate(*values)))
            else:
                self.value = self.do_filter(self.calculater.calculate(*values))
            return self

        for valuer in self.args_valuers:
            valuer.fill(data)
        return self

    def get(self):
        if self.args_wait_loaded:
            values = (valuer.get() for valuer in self.args_valuers)
            if self.return_valuer:
                return self.return_valuer.fill_get(self.do_filter(self.calculater.calculate(*values)))
            return self.do_filter(self.calculater.calculate(*values))
        if self.return_valuer:
            if not self.wait_loaded:
                return self.value
            return self.return_valuer.get()
        return self.value

    def fill_get(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        values = (valuer.fill_get(data) for valuer in self.args_valuers)
        if self.return_valuer:
            return self.return_valuer.fill_get(self.do_filter(self.calculater.calculate(*values)))
        return self.do_filter(self.calculater.calculate(*values))

    def fill_get0(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get(self.calculater.calculate())
        return self.calculater.calculate()

    def fill_get1(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get(self.calculater.calculate(self.args_fill_get0(data)))
        return self.calculater.calculate(self.args_fill_get0(data))

    def fill_get2(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get(self.calculater.calculate(self.args_fill_get0(data),
                                                                         self.args_fill_get1(data)))
        return self.calculater.calculate(self.args_fill_get0(data),
                                         self.args_fill_get1(data))

    def fill_get3(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get(self.calculater.calculate(self.args_fill_get0(data),
                                                                         self.args_fill_get1(data),
                                                                         self.args_fill_get2(data)))
        return self.calculater.calculate(self.args_fill_get0(data),
                                         self.args_fill_get1(data),
                                         self.args_fill_get2(data))

    def fill_get4(self, data):
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                inherit_valuer.fill(data)

        if self.return_valuer:
            return self.return_valuer.fill_get(self.calculater.calculate(self.args_fill_get0(data),
                                                                         self.args_fill_get1(data),
                                                                         self.args_fill_get2(data),
                                                                         self.args_fill_get3(data)))
        return self.calculater.calculate(self.args_fill_get0(data),
                                         self.args_fill_get1(data),
                                         self.args_fill_get2(data),
                                         self.args_fill_get3(data))

    def childs(self):
        childs = []
        if self.args_valuers:
            for args_valuer in self.args_valuers:
                childs.append(args_valuer)
        if self.return_valuer:
            childs.append(self.return_valuer)
        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                childs.append(inherit_valuer)
        return childs

    def get_fields(self):
        fields = []
        for valuer in self.args_valuers:
            for field in valuer.get_fields():
                fields.append(field)

        if self.inherit_valuers:
            for inherit_valuer in self.inherit_valuers:
                for field in inherit_valuer.get_fields():
                    fields.append(field)
        return fields

    def get_final_filter(self):
        if self.return_valuer:
            return self.return_valuer.get_final_filter()

        if self.filter:
            return self.filter

        final_filter = self.calculater.get_final_filter()
        if final_filter is not None:
            return final_filter
        for valuer in self.args_valuers:
            filter = valuer.get_final_filter()
            if filter is None:
                continue

            if final_filter is not None and final_filter.__class__ != filter.__class__:
                return None
            final_filter = filter
        return final_filter

    def is_const(self):
        if hasattr(self, "_cached_is_const"):
            return self._cached_is_const
        if self.calculater.is_realtime_calculater():
            setattr(self, "_cached_is_const", False)
            return False
        return super(CalculateValuer, self).is_const()


class ContextCalculateValuer(CalculateValuer):
    def __init__(self, *args, **kwargs):
        self.contexter = kwargs.pop("contexter")
        self.value_context_id = id(self) * 10
        super(ContextCalculateValuer, self).__init__(*args, **kwargs)

        if not self.args_wait_loaded and not self.wait_loaded:
            self.fill = self.defer_fill
            self.get = self.defer_get

    @property
    def value(self):
        try:
            return self.contexter.values[self.value_context_id]
        except KeyError:
            return None

    @value.setter
    def value(self, v):
        if v is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return
        self.contexter.values[self.value_context_id] = v

    def defer_fill(self, data):
        if data is None:
            if self.value_context_id in self.contexter.values:
                self.contexter.values.pop(self.value_context_id)
            return self
        self.contexter.values[self.value_context_id] = data
        return self

    def defer_get(self):
        try:
            data = self.contexter.values[self.value_context_id]
        except KeyError:
            data = None
        return self.fill_get(data)