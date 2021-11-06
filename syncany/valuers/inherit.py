# -*- coding: utf-8 -*-
# 2020/6/28
# create by: snower

import weakref
from .valuer import Valuer

class InheritValuer(Valuer):
    def __init__(self, value_valuer, *args, **kwargs):
        self.child_valuer = InheritChildValuer(self, value_valuer, *args, **kwargs)
        self.value_valuer = value_valuer
        super(InheritValuer, self).__init__(*args, **kwargs)

        self.filled = False
        self.cloned_child_valuer = None

    def get_inherit_child_valuer(self):
        return self.child_valuer

    def clone(self):
        if self.filled:
            return self

        if self.child_valuer.cloned_inherit_valuer:
            inherit_valuer = self.child_valuer.cloned_inherit_valuer
            self.child_valuer.cloned_inherit_valuer = None
            return inherit_valuer

        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        inherit_valuer = self.__class__(value_valuer, self.key, self.filter)
        self.cloned_child_valuer = inherit_valuer.get_inherit_child_valuer()
        return inherit_valuer

    def fill(self, data):
        if self.value_valuer:
            self.value_valuer.fill(self.do_filter(data))
        else:
            super(InheritValuer, self).fill(data)
        self.filled = True
        return self

    def get(self):
        return None

    def childs(self):
        if not self.value_valuer:
            return []
        return [self.value_valuer]

    def get_fields(self):
        if not self.value_valuer:
            return []
        return self.value_valuer.get_fields()

    def get_final_filter(self):
        return None


class InheritChildValuer(Valuer):
    def __init__(self, inherit_valuer, value_valuer, *args, **kwargs):
        self.inherit_valuer = weakref.proxy(inherit_valuer)
        self.value_valuer = value_valuer
        super(InheritChildValuer, self).__init__(*args, **kwargs)

        self.cloned_inherit_valuer = None

    def clone(self):
        if self.inherit_valuer.filled:
            return self

        if self.inherit_valuer.cloned_child_valuer:
            child_valuer = self.inherit_valuer.cloned_child_valuer
            self.inherit_valuer.cloned_child_valuer = None
            return child_valuer

        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        self.cloned_inherit_valuer = InheritValuer(value_valuer, self.key, self.filter)
        return self.cloned_inherit_valuer.get_inherit_child_valuer()

    def fill(self, data):
        return self

    def get(self):
        if self.value_valuer:
            return self.value_valuer.get()
        return self.value

    def childs(self):
        if not self.value_valuer:
            return []
        return [self.value_valuer]

    def get_fields(self):
        return []

    def get_final_filter(self):
        if not self.value_valuer:
            if self.filter:
                return self.filter
            return None
        return self.value_valuer.get_final_filter()

    def require_loaded(self):
        return False