from datetime import timedelta


class acdTime(object):
    __slots__ = ('data')

    def __init__(self, data):
        self.data = data

    def average(self):
        t = sum(self.data, timedelta())
        result = t / len(self.data)
        return result.total_seconds()

    def maximum(self):
        result = max(self.data)
        return result.total_seconds()
