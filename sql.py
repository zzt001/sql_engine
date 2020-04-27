# Design a lightweight sql engine that parses cvs file and support query like select, where
# First let's design the data structure to store data in memory

# Table class:

# Dict of column_name -> (type, column id)
# Dict of primary_key -> [rows]
# (why dict? insert o(1), delete o(1), update o(1)
#  but list is delete o(n), update o(n))
# methods:
# 1. init(schema)
# 2. insert_row(row)
# 3. ....


# sql engine class apis:
 #    dict: table_name -> Table
 #    1. create_table(table_name, schema)
 #    schema is tuple that contains (conlumn name, type)
 #    2. load_data(table_name, cvs_file)
 #    3. load_data(table_name, row)
 #    4. query(sql_clause)
 #    5. delete_table(table_name)
 #    ...
import Table as tb
import csv
from _collections import defaultdict

class SqlEngine:
    def __init__(self):
        self._tables = dict()

    def create_table(self, table_name, schema):
        if table_name in self._tables:
            raise KeyError("table {} already exists".format(table_name))
        self._tables[table_name] = tb.Table(schema)

    def load_data(self, table_name, csv_file):
        if table_name not in self._tables:
            raise KeyError("table {} not defined".format(table_name))
        with open(csv_file) as file:
            csv_reader = csv.reader(file, delimiter=',', skipinitialspace=True)
            it = iter(csv_reader)
            #skip first line containting name
            next(it)
            for id, row in enumerate(it, 1):
                to_insert = [id] + row
                self.insert_data(table_name, to_insert)

    def insert_data(self, table_name, row):
        if table_name not in self._tables:
            raise KeyError("table {} not defined".format(table_name))
        self._tables[table_name].insert_row(row)

    def show_data(self, table_name):
        if table_name not in self._tables:
            raise KeyError("table {} not defined".format(table_name))
        print(self._tables[table_name].all_data())
    ## try load data at this point

    #
    def parse(self, table_name, sql_clause):
        table = self._tables[table_name]
        c_metas, data = table.get_columns(['Last name', 'First name'])
        print(c_metas)
        for d in data:
            print(d)

if __name__ == '__main__':
    ## try load data at this point
    db = SqlEngine()
    schema = [('id', int), ('Last name', str), ('First name', str), ('SSN', str), ('Test1', float), ('Test2', float),
              ('Test3', float), ('Test4', float), ('Final', float), ('Grade', str)]
    db.create_table("grade", schema)
    db.load_data("grade", 'grades.csv')
    db.show_data("grade")
    db.parse("grade", "")

