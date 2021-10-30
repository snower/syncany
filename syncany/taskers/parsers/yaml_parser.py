# -*- coding: utf-8 -*-
# 2021/10/29
# create by: snower

from .parser import Parser


class ReturnValue(Exception):
    def __init__(self, config):
        self.config = config


class YamlParser(Parser):
    def parse_return(self, config):
        config = self.parse(config)
        if isinstance(config, str):
            config = ":" + config
        elif isinstance(config, list):
            if len(config) == 1 and isinstance(config[0], list):
                config = config[0]
            if config and isinstance(config[0], str):
                config[0] = ":" + config[0]
            else:
                config = [":"] + config
        else:
            config = [":", config]
        return config

    def parse_keyword(self, key, config):
        if isinstance(config, dict):
            config = self.parse(config)
        elif isinstance(config, list):
            config = self.parse(config)
        if isinstance(config, list):
            return [key] + list(config)
        return [key, config]

    def parse_case(self, config):
        if isinstance(config, dict):
            if len(config) > 1:
                config["#case"] = config.pop("<<")
                if ">>" in config:
                    config["#end"] = config.pop(">>")
                return config
            config = list(config.values())[0]
        if isinstance(config, list):
            case_config = {"#case": self.parse(config[0])} if config else {}
            for value in config[1:-1]:
                if isinstance(value, dict):
                    for kk, kv in value.items():
                        case_config[kk] = self.parse(kv)
                elif isinstance(value, list):
                    if len(value) >= 2:
                        case_config[value[0]] = self.parse(value[1])
            if isinstance(config[-1], dict):
                for kk, kv in config[-1].items():
                    case_config[kk] = self.parse(kv)
            else:
                case_config["#end"] = self.parse(config[-1])
            return case_config
        return config

    def parse_match(self, config):
        if isinstance(config, dict):
            if len(config) > 1:
                config["#match"] = config.pop("<<")
                if ">>" in config:
                    config["#end"] = config.pop(">>")
                return config
            config = list(config.values())[0]
        if isinstance(config, list):
            case_config = {"#match": self.parse(config[0])} if config else {}
            for value in config[1:-1]:
                if isinstance(value, dict):
                    for kk, kv in value.items():
                        case_config[kk] = self.parse(kv)
                elif isinstance(value, list):
                    if len(value) >= 2:
                        case_config[value[0]] = self.parse(value[1])
            if isinstance(config[-1], dict):
                for kk, kv in config[-1].items():
                    case_config[kk] = self.parse(kv)
            else:
                case_config["#end"] = self.parse(config[-1])
            return case_config
        return config

    def parse(self, config):
        if isinstance(config, dict):
            if "#case" in config:
                return self.parse_case(config)
            if "#match" in config:
                return self.parse_match(config)

            if len(config) == 1:
                for key, value in config.items():
                    if key == "return":
                        raise ReturnValue(self.parse_return(value))

                    if not key or key[0] not in ("#", "@"):
                        break
                    return self.parse_keyword(key, value)

            for key, value in config.items():
                config[key] = self.parse(value)
            return config

        if isinstance(config, list):
            configs = []
            for value in config:
                try:
                    configs.append(self.parse(value))
                except ReturnValue as e:
                    if configs:
                        if isinstance(configs[-1], list):
                            configs[-1].append(e.config)
                        elif isinstance(configs[-1], dict) and ("#case" in configs[-1] or "#match" in configs[-1]):
                            configs[-1]["::"] = e.config
                        else:
                            configs.append(e.config)
                    else:
                        configs.append(e.config)
            return configs
        return config

    def load(self):
        import yaml
        with open(self.filename, "r") as fp:
            config = yaml.load(fp, yaml.Loader)
        config = self.parse(config)
        return config