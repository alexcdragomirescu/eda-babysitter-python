


class acdDict(dict):

    def __init__(self):
        super(acdDict, self).__init__()

    def __getitem__(self, keys):
        if not isinstance(keys, basestring):
            try:
                node = self
                for key in keys:
                    node = dict.__getitem__(node, key)
                return node
            except TypeError:
                pass
        try:
            return dict.__getitem__(self, keys)
        except KeyError:
            raise KeyError(keys)

    def __setitem__(self, keys, value):
        if not isinstance(keys, basestring):
            try:
                node = self
                for key in keys[:-1]:
                    try:
                        node = dict.__getitem__(node, key)
                    except KeyError:
                        node = node[key] = type(self)()
                return dict.__setitem__(node, keys[-1], value)
            except TypeError:
                pass
        dict.__setitem__(self, keys, value)

    def __missing__(self, key):
        node = self[key] = type(self)()
        return node

    def __add__(self, other):
        return other

    def __radd__(self, other):
        return self.__add__(other)

    def __iadd__(self, other):
        return other

