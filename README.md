sqlShort
========

sqlShort.py is a tiny wrapper for the Python modules MySQLdb and Sqlite.
The aim is to simplify the interactions with a MySQL or Sqlite database.

Example use:

```python
from sqlShort import sqlShort

db = sqlShort(host="db.toto.org", user="toto", passwd="totopass", db="the_db_of_toto")

db.query("DROP TABLE IF EXISTS `friends`")
sql = """CREATE TABLE `friends` (
         id_friend int(11) NOT NULL AUTO_INCREMENT,
         long_name text CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
         weight float NOT NULL,
         UNIQUE KEY `id_friend` (`id_friend`))
         ENGINE='MyISAM' CHARACTER SET utf8 COLLATE utf8_general_ci;"""
db.query(sql)

sql  = "INSERT INTO `friends` "
sql += db.make_insert({'long_name': "Toto de la Scabra di Leon", 'weight': 72.3})
db.query(sql)
```

Default database type is MySQL so to connect to Sqlite:

```python
from sqlShort import sqlShort

db = sqlShort(host="mydb.sqlite3", type="sqlite")
```

To SELECT items:

```python
from sqlShort import sqlShort

db = sqlShort(host="mydb.sqlite3", type="sqlite")

name, address = db.query("SELECT name, address FROM contacts")
```

Then name and address will be lists. If you have numeric data, you can retrieve it as Numpy array:

```python
name, address, size = db.query("SELECT name, address, size FROM contacts", array=True, dtype='f8')
```

The non-numeric data won't be touched. Note that because Sqlite typing is weak, sqlShort tries
to guess the type from the first element of the list (for efficiency). With MySQL, the column types are used.

This module is kind of stupidly simple and is definitely intended for specific uses and SQL-syntax lovers.
If you want a more thought-through thing, you should have a look at http://www.sqlalchemy.org/.

