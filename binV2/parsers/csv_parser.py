import json

from .base_parser import BaseParser


class CSVParser(BaseParser):
    def validate(self):
        pass

    def parse(self):
        parsed_data = {}
        for i in range(0, len(self.data)):
            parsed_data[self.data_format[i]] = self.data[i]
        return parsed_data
