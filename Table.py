# Table class:

# Dict of column_name -> (type, column id)
# Dict of primary_key -> [rows]
# (why dict? insert o(1), delete o(1), update o(1)
#  but list is delete o(n), update o(n))

# schema is [(column_name, type_converter)]

# methods:
# 1. init(schema)
# 2. insert_row(row)
# 3. ....
from collections import defaultdict

class Table:
    def __init__(self, schema):
        if len(schema) == 0:
            raise AssertionError("empty schema")
        # schema is [(column_name, type_converter)]
        self._schema = list(schema)
        # cname is for fast lookup column_name to its id
        self._cname = dict()
        self._primary_key = schema[0]
        for column_id, tp in enumerate(schema):
            if tp[0] in self._cname:
                raise AssertionError("schema contains duplicated column definition")
            self._cname[tp[0]] = column_id
        self._data = defaultdict(list)

    # row should be a list of data based on column order
    def insert_row(self, row):
        if type(row) is not list:
            raise TypeError("Should be either dict or list")

        # first convert data to correct type
        row_convert = [tp[1](row[i]) for i, tp in enumerate(self._schema)]
        # then check primary key exists
        primary_key = row_convert[0]
        if primary_key in self._data:
            raise KeyError("duplicate primary id")
        self._data[primary_key] = row_convert

    def all_data(self):
        res = ' | '.join(['{0}:{1}'.format(tp[0], tp[1]) for tp in self._schema])
        res += '\n'
        for k in sorted(self._data.keys()):
            res += '{}\n'.format(self._data[k])
        return res

    def __str__(self):
        res = "Schema:\n"
        pattern = 'Column id: {0}, Column name: {1}, type: {2}\n'
        for i, tp in enumerate(self._schema):
            if i == 0:
                res += "[Primary key]: "
            res += pattern.format(i, tp[0], tp[1])
        return res

    # start to think select xxx from
    # return a list of (column_names, type) and a list of rows containing specified columns
    def get_columns(self, column_names=None, conds=None, orderKeys=None):
        res = []
        if column_names is None:
            column_ids = [id for id, _ in self._schema]
        else:
            column_ids = sorted([self._cname[n] for n in column_names])

        for row in self._data.values():
            res.append([row[i] for i in column_ids])
        # append column metadata
        return [self._schema[i] for i in column_ids], res



if __name__ == '__main__':
    schema = [('student_id', str), ('gradeA', int), ('gradeB', int)]
    tb = Table(schema)
    print(tb)