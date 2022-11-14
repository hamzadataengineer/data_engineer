from .txt_parser import TxtParser
from .xml_parser import XMLParser
from .csv_parser import CSVParser


class GenericParser:
    get_parser = {
        'txt': TxtParser,
        'xml': XMLParser,
        'csv': CSVParser
    }

    def __init__(self, data, data_format, data_type):
        self.data_type = data_type
        self.data = data
        data_parser_class = self.get_parser.get(data_type, None)
        if data_parser_class is None:
            print(f"Data Type {data_type} Not supported")
        self.data_parser = data_parser_class(data, data_format)

    def parse(self):
        self.data_parser.validate()
        return self.data_parser.parse()
