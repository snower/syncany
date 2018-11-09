# -*- coding: utf-8 -*-
# 18/8/15
# create by: snower

try:
    import elasticsearch
    import elasticsearch.helpers
except ImportError:
    elasticsearch = None

from .database import QueryBuilder, InsertBuilder, UpdateBuilder, DeleteBuilder, DataBase

class ElasticsearchQueryBuilder(QueryBuilder):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchQueryBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["gt"] = value

    def filter_gte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["gte"] = value

    def filter_lt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["lt"] = value

    def filter_lte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["lte"] = value

    def filter_eq(self, key, value):
        self.query[key] = value

    def filter_ne(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["ne"] = value

    def filter_in(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["in"] = value

    def filter_limit(self, count, start=None):
        if start:
            self.limit = (0, count)
        else:
            self.limit = (start, count)

    def order_by(self, key, direct=1):
        self.orders.append((key, 1 if direct else -1))

    def commit(self):
        raise NotImplementedError()

class ElasticsearchInsertBuilder(InsertBuilder):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchInsertBuilder, self).__init__(*args, **kwargs)

        if isinstance(self.datas, dict):
            self.datas = [self.datas]
        self.sql = None

    def get_fields(self):
        fields = None
        for data in self.datas:
            if fields is None:
                fields = set(data.keys())
            else:
                fields = fields & set(data.keys())
        return tuple(fields) if fields else tuple()

    def get_data_primary_key(self, data):
        if len(self.primary_keys) == 1:
            return data.get(self.primary_keys[0], '')
        return ".".join([data.get(pk, '') for pk in self.primary_keys])

    def commit(self):
        fields = self.get_fields()
        datas = []
        for data in self.datas:
            datas.append({
                "_index": self.name,
                "_type": self.name,
                "_id": self.get_data_primary_key(data),
                "_source": {field: data[field] for field in fields}
            })

        connection = self.db.ensure_connection()
        return elasticsearch.helpers.bulk(connection, datas, raise_on_exception=False, raise_on_error=False)

class ElasticsearchUpdateBuilder(UpdateBuilder):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchUpdateBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["gt"] = value

    def filter_gte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["gte"] = value

    def filter_lt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["lt"] = value

    def filter_lte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["lte"] = value

    def filter_eq(self, key, value):
        self.query[key] = value

    def filter_ne(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["ne"] = value

    def filter_in(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["in"] = value

    def commit(self):
        raise NotImplementedError()

class ElasticsearchDeleteBuilder(DeleteBuilder):
    def __init__(self, *args, **kwargs):
        super(ElasticsearchDeleteBuilder, self).__init__(*args, **kwargs)

        self.query = []
        self.query_values = []
        self.sql = None

    def filter_gt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["gt"] = value

    def filter_gte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["gte"] = value

    def filter_lt(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["lt"] = value

    def filter_lte(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["lte"] = value

    def filter_eq(self, key, value):
        self.query[key] = value

    def filter_ne(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["ne"] = value

    def filter_in(self, key, value):
        if key not in self.query:
            self.query[key] = {}
        self.query[key]["in"] = value

    def commit(self):
        raise NotImplementedError()

class ElasticsearchDB(DataBase):
    DEFAULT_CONFIG = {
        "hosts": "127.0.0.1",
    }

    def __init__(self, config):
        if "host" in config:
            config["hosts"] = config.pop("host")

        all_config = {}
        all_config.update(self.DEFAULT_CONFIG)
        all_config.update(config)

        super(ElasticsearchDB, self).__init__(all_config)

        self.connection = None

    def ensure_connection(self):
        if not self.connection:
            if elasticsearch is None:
                raise ImportError("elasticsearch>=6.3.1 is required")

            self.connection = elasticsearch.Elasticsearch(**self.config)
        return self.connection

    def query(self, name, primary_keys = None, fields = ()):
        return ElasticsearchQueryBuilder(self, name, primary_keys, fields)

    def insert(self, name, primary_keys = None, fields =(), datas = None):
        return ElasticsearchInsertBuilder(self, name, primary_keys, fields, datas)

    def update(self, name, primary_keys= None, fields = (), update = None):
        return ElasticsearchUpdateBuilder(self, name, primary_keys, fields, update)

    def delete(self, name, primary_keys = None):
        return ElasticsearchDeleteBuilder(self, name, primary_keys)

    def close(self):
        self.connection = None