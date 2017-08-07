import MySQLdb as _mysql
from collections import namedtuple

class MySQLDatabase(object):

    def __init__(self, database_name, username,password,host='localhost'):
        try:
            self.db = _mysql.connect(db=database_name, host=host, user=username, passwd = password)
            self.database_name = database_name
            print "Connected to MySQL!"
        except _mysql.Error, e:
            print e

    def __del__(self):
        if hasattr(self, 'db'):
            self.db.close()
            print "MySQL Connection Closed"

    def get_available_tables(self):
        cursor=self.db.cursor()
        cursor.execute("SHOW TABLES;")
        self.tables = cursor.fetchall()
        cursor.close()
        return self.tables

    def convert_to_named_tuples(self, cursor):
        results = None
        names = " ".join(d[0] for d in cursor.description)
        klass = namedtuple('Results', names)

        try:
            results = map(klass._make, cursor.fetchall())
        except _mysql.ProgrammingError, e:
            print e

        return results

    def get_columns_for_tables(self, table_name):
        cursor = self.db.cursor()
        cursor.execute("SHOW COLUMNS FROM `%s`" % table_name)
        self.columns = cursor.fetchall()
        cursor.close()
        return self.columns


    def select(self, table, columns=None, named_tuples=True, **kwargs):
        """
        We'll create our `select` method in order
        to make it simpler for extracting data from
        the database.
        select(table_name, [list_of_column_names])
        """
        sql_str = "SELECT "

        # add columns or just use the wildcard
        if not columns:
            sql_str += " * "
        else:
            for column in columns:
                sql_str += "%s, " % column

            sql_str = sql_str[:-2]  # remove the last comma!

        # add the table to the SELECT query
        sql_str += " FROM `%s`.`%s`" % (self.database_name, table)

        # if there's a JOIN clause attached
        if kwargs.has_key('join'):
            sql_str += " JOIN %s" % kwargs.get('join')

        # if there's a WHERE clause attached
        if kwargs.has_key('where'):
            sql_str += " WHERE %s " % kwargs.get('where')

        # if there's a LIIMIT clause attached
        if kwargs.has_key('limit'):
            sql_str += " LIMIT %s " % kwargs.get('limit')

        # if there's an ORDER BY clause attached
        if kwargs.has_key('order_by'):
            sql_str += " ORDER BY %s " % kwargs.get('order_by')

        sql_str += ";"  # Finalise SQL string

        cursor = self.db.cursor()
        cursor.execute(sql_str)

        if named_tuples:
            results = self.convert_to_named_tuples(cursor)
        else:
            results = cursor.fetchall()

        cursor.close()

        return results

