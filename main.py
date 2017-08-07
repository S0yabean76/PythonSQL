from database.mysql import MySQLDatabase
from settings import db_config

db = MySQLDatabase(db_config.get('db_name'),
                   db_config.get('user'),
                   db_config.get('pass'),
                   db_config.get('host'))

tables = db.get_available_tables()
print tables
columns = db.get_columns_for_tables('articles')
print columns

#Get all the records from the people table
all_records = db.select('people')
print "All recordsL %s" %str(all_records)

#Get all the records from the people table but only the 'id' and 'first_name' columns
column_specific_records = db.select('people', ['id', 'first_name'])
print "Column specific records: %s" % str(column_specific_records)

#Select data using the WHERE clause
where_expression_records = db.select('people', ['first_name'], where = "first_name = 'John'")
print "Where Records: %s" % str(where_expression_records)

#Select data using the WHERE clause and the JOIN clause
joined_records = db.select('people', ['first_name'],    where="people.id = 3",
                                                        join="orders ON people.id  = orders.person_id")
print "Joined records: %s" % str(joined_records)

#Select data using the LIMIT clause
limit_records = db.select('orders', limit = 5)
print "Limited records: %s" % str(limit_records)

#Select data using the ORDER BY clause
ordered_records = db.select('people', order_by="second_name")
print "Ordered records: %s" % str (ordered_records)

