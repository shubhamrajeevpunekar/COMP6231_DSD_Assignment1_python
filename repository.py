import statistics
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
consoleHandler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.DEBUG)
logger.addHandler(consoleHandler)

# TODO: Add debug logs
class Repository:
    def __init__(self):
        self.data = dict()

    def add(self, key, value):
        """If key exists, add value to the list associated with this key"""
        if key not in self.data:
            self.data[key] = list()
        self.data[key].append(value)

    def set(self, key, values: list):
        """If key exists, set the given list for the given key"""
        self.data[key] = values

    def delete(self, key):
        """If key exists, delete the key and associated list"""
        try:
            del self.data[key]
        except KeyError:
            logger.warning("Tried to delete a non existent key")

    def keys(self):
        """List the keys"""
        # TODO: if entries contain quotes, properly escape them
        return list(self.data.keys())

    def getValue(self, key):
        """If key exists, return the first value in the list for the given key"""
        if key in self.data:
            return self.data[key][0]
        else:
            return None

    def getValues(self, key):
        """If key exists, return the list"""
        if key in self.data:
            return self.data[key]
        else:
            return None

    def aggregate(self, key, func):
        """Aggregate all elements associated with a key with specified function"""
        funcs = {"max": max, "min": min, "sum": sum, "avg": statistics.mean}
        if key in self.data and func in funcs:
            return funcs[func](self.data[key])
        else:
            return None

    def reset(self):
        self.data.clear()
