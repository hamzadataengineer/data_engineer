from abc import ABC, abstractmethod


class BaseParser(ABC):

    def __init__(self, data, data_format):
        self.data = data
        self.data_format = data_format

    @abstractmethod
    def validate(self):
        raise NotImplementedError

    @abstractmethod
    def parse(self):
        raise NotImplementedError
