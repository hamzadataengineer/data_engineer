import json
import xmltodict

import xml.etree.ElementTree as elementTree

from .base_parser import BaseParser


class XMLParser(BaseParser):
    def validate(self):
        try:
            elementTree.fromstring(self.data)
        except elementTree.ParseError:
            raise Exception(f"Data is not valid XML tree: {self.data}")

    def parse(self):
        obj = xmltodict.parse(self.data)
        return json.dumps(obj)
