from .base_parser import BaseParser


class TxtParser(BaseParser):
    def validate(self):
        pass

    def parse(self):
        parsed_data = {}
        raw_events = self.data.split(',')
        for i in range(0, len(self.data_format), 1):
            parsed_data[self.data_format[i]] = raw_events[i]
        return parsed_data
