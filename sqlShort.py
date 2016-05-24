#!/usr/bin/env python
# -*- coding: utf-8 -*-

#-------------------------------------------------------------------------
#    sqlShort - A tiny wrapper for MySQLdb and Sqlite
#    Version 0.3 - 2010-09-23
#    $Revision$ $Date$
#-------------------------------------------------------------------------
#
#    Copyright 2010 Etienne Gaudrain <egaudrain@gmail.com>
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, version 3 of the License.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program. If not, see <http://www.gnu.org/licenses/>.
#
#-------------------------------------------------------------------------

from numpy import *

class sqlShort:
	def __init__(self, **kwarg):
		"""Creates a connection. Example:
		db = sqlShort(host="sql.server.com", user="toto", passwd="secret", db="the_toto_jokes", type="mysql")
		
		Supported database types are "mysql" and "sqlite". For sqlite, only the "host" argument is considered.
		"""
		
		self.dbtype = (kwarg.pop('type', 'mysql')).lower()
		
		if self.dbtype=='mysql':
			self.dbModule = __import__('MySQLdb')
			self.db = self.dbModule.connect(**kwarg)
		elif self.dbtype=='sqlite':
			self.dbModule = __import__('sqlite3')
			self.db = self.dbModule.connect(kwarg['host'])
			self.db.create_aggregate("STD", 1, std_sqlite)
			self.db.create_function("SQRT", 1, sqrt)
		else:
			raise ValueError("'%s' database type is not handled." % self.dbtype)
		
		self.dbh = self.db.cursor()
		
		if self.dbtype=='sqlite':
			self.db.execute("PRAGMA journal_mode=OFF;")
			self.db.execute("PRAGMA synchronous=0;")
	
	def __del__(self):
		self.db.commit()
		self.dbh.close()
		self.db.close()
	
	def query(self, sql, vals=None, **kwarg):
		"""Executes a SQL query.
		In case of SELECT query, a tuple of lists is returned.
		If array=True, the returned lists are numpy arrays (if the type is numeric).
		In that cas, the dtype of the array can be provided.
		"""
		
		is_array = kwarg.pop('Array', False) or kwarg.pop('array', False)
		dtype = kwarg.pop('dtype', 'f8')
		
		try:
			if vals is None:
				self.dbh.execute(sql)
			else:
				self.dbh.execute(sql, vals)
		except:
			print sql
			raise
		
		result  = self.dbh.fetchall()
		description = self.dbh.description
		
		if description==None:
			return ()
		
		#~ print description
		n = len(description)
		
		if len(result)==0:
			if is_array:
				return tuple([  array([],dtype=dtype) for i in range(n) ])
			else:
				return tuple([ () for i in range(n) ])
		
		t = list()
		for i in range(n):
			t.append([])
		for row in result:
			for i in range(n) :
				t[i].append(row[i])
		if is_array:
			if self.dbtype=='mysql':
				for i in range(n) :
					if (description[i][1] not in self.dbModule.NUMBER) and (description[i][1]!=246):
						continue
					else:
						t[i] = array(t[i], dtype=dtype)
			else:
				numtypes = [type(int()), type(float()), type(double())]
				for i in range(n) :
					if type(t[i][0]) in numtypes:
						t[i] = array(t[i], dtype=dtype)
					else:
						continue
		
		return tuple(t)
	
	def lastrowid(self):
		"""Returns the last inserted id."""
		return self.dbh.lastrowid
	
	def make_insert(self, arg):
		"""Converts a dict into an INSERT type syntax, dealing with the types.
		If a dictionnary is provided, returns: SET `key`=value, ...
		If a list of dictionnaries is given, the long INSERT syntax is used: (`key`, ...) VALUES (value, ...), (...), ...
		In that case, the list of fields is read from the keys of the first row.
		"""
		if self.dbtype=='mysql':
		
			if type(arg)==type(dict()):
				fields = "SET "+", ".join(["`%s`='%s'" % (k, self.str(v)) for k, v in arg.items()])
			elif type(arg)==type(list()):
				n = len(arg[0].keys())
				fields  = "("+", ".join(["`%s`" % k for k in arg[0].keys()])+")"
				fields += " VALUES "
				L = list()
				for i, r in enumerate(arg):
					if len(r.values()) != n:
						print r
						raise ValueError('On row %d, %d arguments found, %d expected.' % (i, len(r.values()), n))
					L.append("("+", ".join([self.str(v) for v in r.values()])+")")
				fields += ", ".join(L)
			else:
				raise TypeError('sqlShort.make_insert() argument should be dict or list (%s given).' % type(arg))
			
			return fields
		
		elif self.dbtype=='sqlite':
			if type(arg)==type(dict()):
				arg = [arg]
			if type(arg)==type(list()):
				n = len(arg[0].keys())
				fields  = "("+", ".join(["`%s`" % k for k in arg[0].keys()])+")"
				fields += " VALUES "
				L = list()
				vals = list()
				for i, r in enumerate(arg):
					if len(r.values()) != n:
						print r
						raise ValueError('On row %d, %d arguments found, %d expected.' % (i, len(r.values()), n))
					L.append("("+", ".join("?"*n)+")")
					vals.extend(r.values())
				fields += ", ".join(L)
			else:
				raise TypeError('sqlShort.make_insert() argument should be dict or list (%s given).' % type(arg))
		
			return fields, vals
	
	def insert(self, table, arg):
		ins = ("INSERT INTO `%s` " % table)
		if self.dbtype=='mysql':
			q = self.make_insert(arg)
			self.query(ins+q)
		elif self.dbtype=='sqlite':
			q, v = self.make_insert(arg)
			self.query(ins+q, tuple(v))
	
	def str(self, v):
		"""Converts a Python variable into SQL string to insert it in a query."""
		if v is None:
			return "NULL"
		elif type(v)==type(str()) or type(v)==type(unicode()):
			if self.dbtype=="mysql":
				return "\""+self.db.escape_string(v)+"\""
			else:
				return "'"+(v.replace("\\", "\\\\").replace("\"", "\\\"").replace("'", "\\'"))+"'"
		else:
			return str(v)
		
		

#------------ Add Math Functions to SQLite

class std_sqlite:
	def __init__(self):
		self.x = list()
	
	def step(self, v):
		self.x.append(v)
	
	def finalize(self):
		return std(array(self.x, dtype='f8'))
