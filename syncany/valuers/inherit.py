# -*- coding: utf-8 -*-
# 2020/6/28
# create by: snower


import weakref
from .valuer import Valuer

class InheritValuer(Valuer):
    def __init__(self, child_valuer, value_valuer, *args, **kwargs):
        super(InheritValuer, self).__init__(*args, **kwargs)

        self.child_valuer = child_valuer if child_valuer else InheritChildValuer(self, value_valuer, *args, **kwargs)
        self.value_valuer = value_valuer
        self.cloned_child_valuer = None

    def get_inherit_child_valuer(self):
        return self.child_valuer

    def clone(self):
        if self.child_valuer.cloned_inherit_valuer:
            child_valuer = self.child_valuer.cloned_inherit_valuer
            self.child_valuer.cloned_inherit_valuer = None
        else:
            child_valuer = self.child_valuer.clone()
            self.cloned_child_valuer = child_valuer
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return self.__class__(child_valuer, value_valuer, self.key, self.filter)

    def fill(self, data):
        if self.value_valuer:
            self.value_valuer.fill(data)
        else:
            super(InheritValuer, self).fill(data)
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
        super(InheritChildValuer, self).__init__(*args, **kwargs)

        self.inherit_valuer = weakref.ref(inherit_valuer)
        self.value_valuer = value_valuer
        self.cloned_inherit_valuer = None

    def clone(self):
        if self.inherit_valuer.cloned_child_valuer:
            inherit_valuer = self.inherit_valuer.cloned_child_valuer
            self.inherit_valuer.cloned_child_valuer = None
        else:
            inherit_valuer = self.inherit_valuer.clone()
            self.cloned_inherit_valuer = inherit_valuer
        value_valuer = self.value_valuer.clone() if self.value_valuer else None
        return self.__class__(inherit_valuer, value_valuer, self.key, self.filter)

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
        if self.filter:
            return self.filter

        if not self.value_valuer:
            return None
        return self.value_valuer.get_final_filter()