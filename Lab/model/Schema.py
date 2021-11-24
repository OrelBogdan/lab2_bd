#!/usr/bin/env python3


import Lab.utils

from . import DynamicSearch
from .AutoSchema import *
import collections
import psycopg2
import datetime
from Lab.view import View

import re
import Lab.utils
import collections


import psycopg2.extensions
import psycopg2.sql

import Lab.utils.psql_types


class Categories(SchemaTable):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.primary_key_name = f"Category_id"
		#print(self.colums())

	def columns(self):
		row_type= collections.namedtuple("row_type",'column_name data_type')
		q1=row_type('Category_id','bigserial')
		q2=row_type('Category_name','character varying')
		result=(q1,q2)

		
		return result

	def addData(self, data: dict[collections.namedtuple] = None):
		#print(data)
		
		#print(self.columns())
		#print(type(self.columns()))

		if data is None:
			#View.printInfo("None")
			return Lab.utils.menuInput(self.addData, [a for a in self.columns()\
				if a.column_name not in [f"{self.primary_key_name}"]])

		NewName=next(a for a in data if a.column_name in ["Category_name"])
		#print("Data_name", data[NewName])

		
		sql = f"""
			INSERT INTO "Shop"."Categories" ("Category_name") VALUES (\'{data[NewName]}\');
		"""

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				View.printInfo(sql)
				
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				#print(f"Something went wrong: {e}")
				View.printInfo(f"Something went wrong: {e}")
				#print(type({e}))
				# raise e
			else:
				#print(f"{dbcursor.rowcount} rows added")
				#print(type({dbcursor.rowcount}))
				View.printInfo(f"{dbcursor.rowcount} rows added")



	def editData(self, data: dict[collections.namedtuple] = None):
		if data is None:
			return Lab.utils.menuInput(self.editData, [a for a in self.columns() if a.column_name not in []])

		
		NewName=next(a for a in data if a.column_name in ["Category_name"])
		NewId=next(a for a in data if a.column_name in ["Category_id"])
		#print("Data_name", data[NewName])
		#print("Data_id", data[NewId])
		sql = f"""UPDATE "Shop"."Categories" SET "Category_name" = \'{data[NewName]}\' 
		WHERE "Category_id" = {data[NewId]};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows changed")


	def removeData(self, rowid=None):
		# rowid = click.prompt(f"{self.primary_key_name}", type=int)
		if rowid is None:
			return Lab.utils.menuInput(self.removeData, [a for a in self.columns()\
				if a.column_name in [f"{self.primary_key_name}"]])

		if isinstance(rowid, dict):
			#printInfo(rowid)
			#NewId=next(a for a in rowid if a.column_name in ["Category_id"])
			#NewName=next(a for a in data if a.column_name in ["Category_name"])
			rowid = rowid[next(a for a in rowid if a.column_name in ["Category_id"])]
			#printInfo(rowid)

		sql =f"""DELETE FROM "Shop"."Categories" WHERE "Category_id" = {rowid};"""

		#sql = f"""DELETE FROM {self} WHERE "{self.primary_key_name}" = {rowid};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows deleted")

	def showData(self, sql=None):
		# print(showDataCreator)
		if sql is None:
			sql = f"""SELECT * FROM "Shop"."Categories";"""
			View.printInfo(sql)
		return self.schema.showData(sql=sql)

	def randomFill(self, instances: int = None, str_len: int = 10, sql_replace: str = None):
 		if instances is None:
 			#instances = 100
 			return Lab.utils.menuInput(self.randomFill, [collections.namedtuple("instances", \
 				["column_name", "data_type", "default"])("instances", "int", lambda: 100)])

 		if isinstance(instances, dict):
				instances = instances[next(a for a in instances if a.column_name in ["instances"])]


 		sql = f"""
 		INSERT INTO "Shop"."Categories"("Category_name")
				SELECT
					substr(characters, (random() * length(characters) + 1)::integer, 10)
				FROM
					(VALUES('qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM')) as symbols(characters),
					generate_series(1, {instances});

 		"""
 		with self.schema.dbconn.cursor() as dbcursor:
 			try:
 				View.printInfo(sql)
 				t1 = datetime.datetime.now()
 				dbcursor.execute(sql)
 				t2 = datetime.datetime.now()
 				self.schema.dbconn.commit()
 			except Exception as e:
 				self.schema.dbconn.rollback()
 				View.printInfo(f"Something went wrong: {e}")
 			else:
 				View.printInfo(f"{self} {dbcursor.rowcount} rows added, execution time: {t2 - t1}")








class Manufacturer(SchemaTable):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.primary_key_name = f"Manufacturer_id"

	def columns(self):
		row_type= collections.namedtuple("row_type",'column_name data_type')
		q1=row_type('Manufacturer_id','bigint')
		q2=row_type('Manufacturer_name','character varying')
		result=(q1,q2)
		return result

	def addData(self, data: dict[collections.namedtuple] = None):
		#View.printInfo(data)
		if data is None:
			#View.printInfo("None")
			return Lab.utils.menuInput(self.addData, [a for a in self.columns()\
				if a.column_name not in [f"{self.primary_key_name}"]])

		NewName=next(a for a in data if a.column_name in ["Manufacturer_name"])
		#print("Data_name", data[NewName])

		
		sql = f"""
			INSERT INTO "Shop"."Manufacturer" ("Manufacturer_name") VALUES (\'{data[NewName]}\');
		"""

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				View.printInfo(sql)
				#print((psycopg2.extensions.AsIs(", ".join(map(lambda x: f'"{x}"', columns))), values))
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				#print(f"Something went wrong: {e}")
				View.printInfo(f"Something went wrong: {e}")
				#print(type({e}))
				# raise e
			else:
				#print(f"{dbcursor.rowcount} rows added")
				#print(type({dbcursor.rowcount}))
				View.printInfo(f"{dbcursor.rowcount} rows added")



	def editData(self, data: dict[collections.namedtuple] = None):
		if data is None:
			return Lab.utils.menuInput(self.editData, [a for a in self.columns() if a.column_name not in []])

		
		NewName=next(a for a in data if a.column_name in ["Manufacturer_name"])
		NewId=next(a for a in data if a.column_name in ["Manufacturer_id"])
		#View.printInfoInfo("Data_name", data[NewName])
		#printInfo("Data_id", data[NewId])
		sql = f"""UPDATE "Shop"."Manufacturer" SET "Manufacturer_name" = \'{data[NewName]}\'
		 WHERE "Manufacturer_id" = {data[NewId]};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows changed")


	def removeData(self, rowid=None):
		# rowid = click.prompt(f"{self.primary_key_name}", type=int)
		if rowid is None:
			return Lab.utils.menuInput(self.removeData, [a for a in self.columns() if a.column_name in\
				[f"{self.primary_key_name}"]])

		if isinstance(rowid, dict):
			#View.printInfo(rowid)
			#NewId=next(a for a in rowid if a.column_name in ["Category_id"])
			#NewName=next(a for a in data if a.column_name in ["Category_name"])
			rowid = rowid[next(a for a in rowid if a.column_name in ["Manufacturer_id"])]
			#View.printInfo(rowid)

		sql =f"""DELETE FROM "Shop"."Manufacturer" WHERE "Manufacturer_id" = {rowid};"""

		#sql = f"""DELETE FROM {self} WHERE "{self.primary_key_name}" = {rowid};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows deleted")

	def showData(self, sql=None):
		# print(showDataCreator)
		if sql is None:
			sql = f"""SELECT * FROM "Shop"."Manufacturer";"""
			View.printInfo(sql)
		return self.schema.showData(sql=sql)

	def randomFill(self, instances: int = None, str_len: int = 10, sql_replace: str = None):
 		if instances is None:
 			#instances = 100
 			return Lab.utils.menuInput(self.randomFill, [collections.namedtuple("instances",\
 				["column_name", "data_type", "default"])("instances", "int", lambda: 100)])

 		if isinstance(instances, dict):
				instances = instances[next(a for a in instances if a.column_name in ["instances"])]


 		sql = f"""
 		INSERT INTO "Shop"."Manufacturer"("Manufacturer_name")
				SELECT
					substr(characters, (random() * length(characters) + 1)::integer, 10)
				FROM
					(VALUES('qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM')) as symbols(characters),
					generate_series(1, {instances});

 		"""
 		with self.schema.dbconn.cursor() as dbcursor:
 			try:
 				View.printInfo(sql)
 				t1 = datetime.datetime.now()
 				dbcursor.execute(sql)
 				t2 = datetime.datetime.now()
 				self.schema.dbconn.commit()
 			except Exception as e:
 				self.schema.dbconn.rollback()
 				View.printInfo(f"Something went wrong: {e}")
 			else:
 				View.printInfo(f"{self} {dbcursor.rowcount} rows added, execution time: {t2 - t1}")

	

class Products(SchemaTable):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.primary_key_name = f"Product_id"

	def columns(self):
		row_type= collections.namedtuple("row_type",'column_name data_type')
		q1=row_type('Product_id','bigserial')
		q2=row_type('Manufacturer_id','bigserial')
		q3=row_type('Category_id','bigserial')
		q4=row_type('Product_name','character varying')
		q5=row_type('Price','money')
		q6=row_type('Amount','integer')
		result=(q1,q2,q3,q4,q5,q6)
		return result


	def addData(self, data: dict[collections.namedtuple] = None):
	 	#View.printInfo(data)
	 	if data is None:
	 		#View.printInfo("None")
	 		return Lab.utils.menuInput(self.addData, [a for a in self.columns()\
	 			if a.column_name not in [f"{self.primary_key_name}"]])

	 	NewCategory_id=next(a for a in data if a.column_name in ["Category_id"])
	 	NewManufacturer_id=next(a for a in data if a.column_name in ["Manufacturer_id"])
	 	NewProduct_name=next(a for a in data if a.column_name in ["Product_name"])
	 	NewPrice=next(a for a in data if a.column_name in ["Price"])
	 	NewAmount=next(a for a in data if a.column_name in ["Amount"])
	 		 	

	 	sql = f"""
	 		INSERT INTO "Shop"."Products" ("Category_id", "Manufacturer_id", "Product_name", "Price", "Amount") 
	 		VALUES ({data[NewCategory_id]}, {data[NewManufacturer_id]},\'{data[NewProduct_name]}\', 
	 		{data[NewPrice]}, {data[NewAmount]});
	 	"""

	 	with self.schema.dbconn.cursor() as dbcursor:
	 		try:
	 			
	 			View.printInfo(sql)
	 			dbcursor.execute(sql)
	 			self.schema.dbconn.commit()
	 		except Exception as e:
	 			self.schema.dbconn.rollback()
	 			#print(f"Something went wrong: {e}")
	 			#raise(e)
	 			View.printInfo(f"Something went wrong: {e}")
	 			#print(type({e}))
	 			# raise e
	 		else:
	 			#print(f"{dbcursor.rowcount} rows added")
	 			#print(type({dbcursor.rowcount}))
	 			View.printInfo(f"{dbcursor.rowcount} rows added")


	def editData(self, data: dict[collections.namedtuple] = None):
	 	if data is None:
	 		return Lab.utils.menuInput(self.editData, [a for a in self.columns() if a.column_name not in []])

	 	NewCategory_id=next(a for a in data if a.column_name in ["Category_id"])
	 	NewManufacturer_id=next(a for a in data if a.column_name in ["Manufacturer_id"])
	 	NewProduct_name=next(a for a in data if a.column_name in ["Product_name"])
	 	NewPrice=next(a for a in data if a.column_name in ["Price"])
	 	NewAmount=next(a for a in data if a.column_name in ["Amount"])
	 	NewProduct_id=next(a for a in data if a.column_name in ["Product_id"])
	 	#View.printInfoInfo("Data_name", data[NewName])
	 	#printInfo("Data_id", data[NewId])
	 	sql = f"""UPDATE "Shop"."Products" SET "Category_id" = {data[NewCategory_id]}, 
	 	"Manufacturer_id" = {data[NewManufacturer_id]}, 
	 	"Product_name" = \'{data[NewProduct_name]}\', "Price" = {data[NewPrice]}, 
	 	"Amount" = {data[NewAmount]} WHERE "Product_id" = {data[NewProduct_id]};"""
	 	View.printInfo(sql)

	 	with self.schema.dbconn.cursor() as dbcursor:
	 		try:
	 			dbcursor.execute(sql)
	 			self.schema.dbconn.commit()
	 		except Exception as e:
	 			self.schema.dbconn.rollback()
	 			View.printInfo(f"Something went wrong: {e}")
	 		else:
	 			View.printInfo(f"{dbcursor.rowcount} rows changed")


	def removeData(self, rowid=None):
		# rowid = click.prompt(f"{self.primary_key_name}", type=int)
		if rowid is None:
			return Lab.utils.menuInput(self.removeData, [a for a in self.columns()\
				if a.column_name in [f"{self.primary_key_name}"]])

		if isinstance(rowid, dict):
			#View.printInfo(rowid)
			#NewId=next(a for a in rowid if a.column_name in ["Category_id"])
			#NewName=next(a for a in data if a.column_name in ["Category_name"])
			rowid = rowid[next(a for a in rowid if a.column_name in ["Product_id"])]
			#View.printInfo(rowid)

		sql =f"""DELETE FROM "Shop"."Products" WHERE "Product_id" = {rowid};"""

		#sql = f"""DELETE FROM {self} WHERE "{self.primary_key_name}" = {rowid};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows deleted")

	def showData(self, sql=None):
		# print(showDataCreator)
		if sql is None:
			sql = f"""SELECT * FROM "Shop"."Products";"""
			View.printInfo(sql)
		return self.schema.showData(sql=sql)

	def randomFill(self, instances: int = None, str_len: int = 10, sql_replace: str = None):
 		if instances is None:
 			#instances = 100
 			return Lab.utils.menuInput(self.randomFill, [collections.namedtuple("instances",\
 				["column_name", "data_type", "default"])("instances", "int", lambda: 100)])

 		if isinstance(instances, dict):
				instances = instances[next(a for a in instances if a.column_name in ["instances"])]


 		sql = f"""
  		INSERT INTO "Shop"."Products"("Category_id", "Manufacturer_id", "Product_name", "Price", "Amount")
				SELECT
					
					(SELECT "Category_id" FROM "Shop"."Categories" ORDER BY random()*q LIMIT 1),
					(SELECT "Manufacturer_id" FROM "Shop"."Manufacturer" ORDER BY random()*q LIMIT 1),
					substr(characters, (random() * length(characters) + 1)::integer, 10),
					trunc(random() * 100)::int,
					trunc(random() * 100)::int
				FROM
					(VALUES('qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM')) as symbols(characters),
					generate_series(1, {instances}) as q;				
					

 		"""
 		#{instances}
 		with self.schema.dbconn.cursor() as dbcursor:
 			try:
 				View.printInfo(sql)
 				t1 = datetime.datetime.now()
 				dbcursor.execute(sql)
 				t2 = datetime.datetime.now()
 				self.schema.dbconn.commit()
 			except Exception as e:
 				self.schema.dbconn.rollback()
 				View.printInfo(f"Something went wrong: {e}")
 			else:
 				View.printInfo(f"{self} {dbcursor.rowcount} rows added, execution time: {t2 - t1}")


class User(SchemaTable):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.primary_key_name = f"User_data_id"

	def columns(self):
		row_type= collections.namedtuple("row_type",'column_name data_type')
		q1=row_type('User_data_id','bigserial')
		q2=row_type('Name','character varying')
		q3=row_type('Surname','character varying')
		q4=row_type('Patronymic','character varying')
		q5=row_type('Email','character varying')
		result=(q1,q2,q3,q4,q5)
		return result

	def addData(self, data: dict[collections.namedtuple] = None):
	 	#View.printInfo(data)
	 	if data is None:
	 		#View.printInfo("None")
	 		return Lab.utils.menuInput(self.addData, [a for a in self.columns()\
	 			if a.column_name not in [f"{self.primary_key_name}"]])

	 	NewName=next(a for a in data if a.column_name in ["Name"])
	 	NewSurname=next(a for a in data if a.column_name in ["Surname"])
	 	NewPatronymic=next(a for a in data if a.column_name in ["Patronymic"])
	 	NewEmail=next(a for a in data if a.column_name in ["Email"])
	 		

	 	sql = f"""
	 		INSERT INTO "Shop"."User" ("Name", "Surname", "Patronymic", "Email") 
	 		VALUES (\'{data[NewName]}\',\'{data[NewSurname]}\',\'{data[NewPatronymic]}\',\'{data[NewEmail]}\');
	 	"""

	 	with self.schema.dbconn.cursor() as dbcursor:
	 		try:
	 			View.printInfo(sql)
	 			#print((psycopg2.extensions.AsIs(", ".join(map(lambda x: f'"{x}"', columns))), values))
	 			dbcursor.execute(sql)
	 			self.schema.dbconn.commit()
	 		except Exception as e:
	 			self.schema.dbconn.rollback()
	 			#print(f"Something went wrong: {e}")
	 			#raise(e)
	 			View.printInfo(f"Something went wrong: {e}")
	 			#print(type({e}))
	 			# raise e
	 		else:
	 			#print(f"{dbcursor.rowcount} rows added")
	 			#print(type({dbcursor.rowcount}))
	 			View.printInfo(f"{dbcursor.rowcount} rows added")


	def editData(self, data: dict[collections.namedtuple] = None):
	 	if data is None:
	 		return Lab.utils.menuInput(self.editData, [a for a in self.columns() if a.column_name not in []])

	 	NewName=next(a for a in data if a.column_name in ["Name"])
	 	NewSurname=next(a for a in data if a.column_name in ["Surname"])
	 	NewPatronymic=next(a for a in data if a.column_name in ["Patronymic"])
	 	NewEmail=next(a for a in data if a.column_name in ["Email"])
	 	NewId=next(a for a in data if a.column_name in ["User_data_id"])
	 	#View.printInfoInfo("Data_name", data[NewName])
	 	#printInfo("Data_id", data[NewId])
	 	sql = f"""UPDATE "Shop"."User" SET "Name" = \'{data[NewName]}\', "Surname" = \'{data[NewSurname]}\', 
	 	"Patronymic" = \'{data[NewPatronymic]}\', "Email" = \'{data[NewEmail]}\' 
	 	WHERE "User_data_id" = {data[NewId]};"""
	 	View.printInfo(sql)

	 	with self.schema.dbconn.cursor() as dbcursor:
	 		try:
	 			dbcursor.execute(sql)
	 			self.schema.dbconn.commit()
	 		except Exception as e:
	 			self.schema.dbconn.rollback()
	 			View.printInfo(f"Something went wrong: {e}")
	 		else:
	 			View.printInfo(f"{dbcursor.rowcount} rows changed")


	def removeData(self, rowid=None):
		# rowid = click.prompt(f"{self.primary_key_name}", type=int)
		if rowid is None:
			return Lab.utils.menuInput(self.removeData, [a for a in self.columns()\
				if a.column_name in [f"{self.primary_key_name}"]])

		if isinstance(rowid, dict):
			#View.printInfo(rowid)
			#NewId=next(a for a in rowid if a.column_name in ["Category_id"])
			#NewName=next(a for a in data if a.column_name in ["Category_name"])
			rowid = rowid[next(a for a in rowid if a.column_name in ["User_data_id"])]
			#View.printInfo(rowid)

		sql =f"""DELETE FROM "Shop"."User" WHERE "User_data_id" = {rowid};"""

		#sql = f"""DELETE FROM {self} WHERE "{self.primary_key_name}" = {rowid};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows deleted")

	def showData(self, sql=None):
		# print(showDataCreator)
		if sql is None:
			sql = f"""SELECT * FROM "Shop"."User";"""
			View.printInfo(sql)
		return self.schema.showData(sql=sql)

	def randomFill(self, instances: int = None, str_len: int = 10, sql_replace: str = None):
 		if instances is None:
 			#instances = 100
 			return Lab.utils.menuInput(self.randomFill, [collections.namedtuple("instances",\
 				["column_name", "data_type", "default"])("instances", "int", lambda: 100)])

 		if isinstance(instances, dict):
				instances = instances[next(a for a in instances if a.column_name in ["instances"])]


 		sql = f"""
  		INSERT INTO "Shop"."User"("Name", "Surname", "Patronymic", "Email")
				SELECT
					substr(characters, (random() * length(characters) + 1)::integer, 10),
					substr(characters, (random() * length(characters) + 1)::integer, 10),
					substr(characters, (random() * length(characters) + 1)::integer, 10),
					substr(characters, (random() * length(characters) + 1)::integer, 10)
				FROM
					(VALUES('qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM')) as symbols(characters),
					generate_series(1, {instances});
					


 		"""
 		with self.schema.dbconn.cursor() as dbcursor:
 			try:
 				View.printInfo(sql)
 				t1 = datetime.datetime.now()
 				dbcursor.execute(sql)
 				t2 = datetime.datetime.now()
 				self.schema.dbconn.commit()
 			except Exception as e:
 				self.schema.dbconn.rollback()
 				View.printInfo(f"Something went wrong: {e}")
 			else:
 				View.printInfo(f"{self} {dbcursor.rowcount} rows added, execution time: {t2 - t1}")


class Order(SchemaTable):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.primary_key_name = f"Order_id"

	def columns(self):
		row_type= collections.namedtuple("row_type",'column_name data_type')
		q1=row_type('Order_id','bigserial')
		q2=row_type('User_data_id','bigserial')
		result=(q1,q2)
		return result

	def addData(self, data: dict[collections.namedtuple] = None):
		#View.printInfo(data)
		if data is None:
			#View.printInfo("None")
			return Lab.utils.menuInput(self.addData, [a for a in self.columns()\
				if a.column_name not in [f"{self.primary_key_name}"]])

		NewUser_data_id=next(a for a in data if a.column_name in ["User_data_id"])
		

		sql = f"""
			INSERT INTO "Shop"."Order" ("User_data_id") VALUES ({data[NewUser_data_id]});
		"""

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				View.printInfo(sql)
				#print((psycopg2.extensions.AsIs(", ".join(map(lambda x: f'"{x}"', columns))), values))
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				#print(f"Something went wrong: {e}")
				View.printInfo(f"Something went wrong: {e}")
				#print(type({e}))
				# raise e
			else:
				#print(f"{dbcursor.rowcount} rows added")
				#print(type({dbcursor.rowcount}))
				View.printInfo(f"{dbcursor.rowcount} rows added")



	def editData(self, data: dict[collections.namedtuple] = None):
		if data is None:
			return Lab.utils.menuInput(self.editData, [a for a in self.columns() if a.column_name not in []])

		
		NewUser_data_id=next(a for a in data if a.column_name in ["User_data_id"])
		NewOrder_id=next(a for a in data if a.column_name in ["Order_id"])
		
		sql = f"""UPDATE "Shop"."Order" SET "User_data_id" = {data[NewUser_data_id]} 
		WHERE "Order_id" = {data[NewOrder_id]};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows changed")


	def removeData(self, rowid=None):
		# rowid = click.prompt(f"{self.primary_key_name}", type=int)
		if rowid is None:
			return Lab.utils.menuInput(self.removeData, [a for a in self.columns()\
				if a.column_name in [f"{self.primary_key_name}"]])

		if isinstance(rowid, dict):
			
			rowid = rowid[next(a for a in rowid if a.column_name in ["Order_id"])]
			

		sql =f"""DELETE FROM "Shop"."Order" WHERE "Order_id" = {rowid};"""

		#sql = f"""DELETE FROM {self} WHERE "{self.primary_key_name}" = {rowid};"""
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows deleted")

	def showData(self, sql=None):
		# print(showDataCreator)
		if sql is None:
			sql = f"""SELECT * FROM "Shop"."Order";"""
			View.printInfo(sql)
		return self.schema.showData(sql=sql)

	def randomFill(self, instances: int = None, str_len: int = 10, sql_replace: str = None):
 		if instances is None:
 			#instances = 100
 			return Lab.utils.menuInput(self.randomFill, [collections.namedtuple("instances",\
 				["column_name", "data_type", "default"])("instances", "int", lambda: 100)])

 		if isinstance(instances, dict):
				instances = instances[next(a for a in instances if a.column_name in ["instances"])]


 		sql = f"""
 		INSERT INTO "Shop"."Order"("User_data_id")
				SELECT
					
					(SELECT "User_data_id" FROM "Shop"."User" ORDER BY random()*q LIMIT 1)
				
				FROM
					generate_series(1, {instances}) as q;
 		"""
 		with self.schema.dbconn.cursor() as dbcursor:
 			try:
 				View.printInfo(sql)
 				t1 = datetime.datetime.now()
 				dbcursor.execute(sql)
 				t2 = datetime.datetime.now()
 				self.schema.dbconn.commit()
 			except Exception as e:
 				self.schema.dbconn.rollback()
 				View.printInfo(f"Something went wrong: {e}")
 			else:
 				View.printInfo(f"{self} {dbcursor.rowcount} rows added, execution time: {t2 - t1}")


class Ordered_product(SchemaTable):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.primary_key_name = f"Ordered_product_id"

	def columns(self):
		row_type= collections.namedtuple("row_type",'column_name data_type')
		q1=row_type('Ordered_product_id','bigserial')
		q2=row_type('Product_id','bigserial')
		q3=row_type('Ordered_amount','integer')
		q4=row_type('Order_id','bigserial')
		result=(q1,q2,q3,q4)
		return result

	def addData(self, data: dict[collections.namedtuple] = None):
	 	#View.printInfo(data)
	 	if data is None:
	 		#View.printInfo("None")
	 		return Lab.utils.menuInput(self.addData, [a for a in self.columns()\
	 			if a.column_name not in [f"{self.primary_key_name}"]])

	 	NewProduct_id=next(a for a in data if a.column_name in ["Product_id"])
	 	NewOrdered_amount=next(a for a in data if a.column_name in ["Ordered_amount"])
	 	NewOrder_id=next(a for a in data if a.column_name in ["Order_id"])
	 	 	

	 	sql = f"""
	 		INSERT INTO "Shop"."Ordered_product" ("Product_id", "Ordered_amount", "Order_id") 
	 		VALUES ({data[NewProduct_id]},{data[NewOrdered_amount]},{data[NewOrder_id]});
	 	"""

	 	with self.schema.dbconn.cursor() as dbcursor:
	 		try:
	 			View.printInfo(sql)
	 			
	 			dbcursor.execute(sql)
	 			self.schema.dbconn.commit()
	 		except Exception as e:
	 			self.schema.dbconn.rollback()
	 			
	 			View.printInfo(f"Something went wrong: {e}")
	 			
	 		else:
	 			
	 			View.printInfo(f"{dbcursor.rowcount} rows added")


	def editData(self, data: dict[collections.namedtuple] = None):
	 	if data is None:
	 		return Lab.utils.menuInput(self.editData, [a for a in self.columns() if a.column_name not in []])

	 	NewProduct_id=next(a for a in data if a.column_name in ["Product_id"])
	 	NewOrdered_amount=next(a for a in data if a.column_name in ["Ordered_amount"])
	 	NewOrder_id=next(a for a in data if a.column_name in ["Order_id"])
	 	NewOrdered_product_id=next(a for a in data if a.column_name in ["Ordered_product_id"])
	 	#View.printInfoInfo("Data_name", data[NewName])
	 	#printInfo("Data_id", data[NewId])
	 	sql = f"""UPDATE "Shop"."Ordered_product" SET "Product_id" = {data[NewProduct_id]}, 
	 	"Ordered_amount" = {data[NewOrdered_amount]}, 
	 	"Order_id" = {data[NewOrder_id]} WHERE "Ordered_product_id" = {data[NewOrdered_product_id]};"""
	 	View.printInfo(sql)

	 	with self.schema.dbconn.cursor() as dbcursor:
	 		try:
	 			dbcursor.execute(sql)
	 			self.schema.dbconn.commit()
	 		except Exception as e:
	 			self.schema.dbconn.rollback()
	 			View.printInfo(f"Something went wrong: {e}")
	 		else:
	 			View.printInfo(f"{dbcursor.rowcount} rows changed")


	def removeData(self, rowid=None):
		# rowid = click.prompt(f"{self.primary_key_name}", type=int)
		if rowid is None:
			return Lab.utils.menuInput(self.removeData, [a for a in self.columns() \
				if a.column_name in [f"{self.primary_key_name}"]])

		if isinstance(rowid, dict):
			#View.printInfo(rowid)
			#NewId=next(a for a in rowid if a.column_name in ["Category_id"])
			#NewName=next(a for a in data if a.column_name in ["Category_name"])
			rowid = rowid[next(a for a in rowid if a.column_name in ["Ordered_product_id"])]
			#View.printInfo(rowid)

		sql =f"""DELETE FROM "Shop"."Ordered_product" WHERE "Ordered_product_id" = {rowid};"""

		
		View.printInfo(sql)

		with self.schema.dbconn.cursor() as dbcursor:
			try:
				dbcursor.execute(sql)
				self.schema.dbconn.commit()
			except Exception as e:
				self.schema.dbconn.rollback()
				View.printInfo(f"Something went wrong: {e}")
			else:
				View.printInfo(f"{dbcursor.rowcount} rows deleted")

	def showData(self, sql=None):
		# print(showDataCreator)
		if sql is None:
			sql = f"""SELECT * FROM "Shop"."Ordered_product";"""
			View.printInfo(sql)
		return self.schema.showData(sql=sql)

	def randomFill(self, instances: int = None, str_len: int = 10, sql_replace: str = None):
 		if instances is None:
 			#instances = 100
 			return Lab.utils.menuInput(self.randomFill, [collections.namedtuple("instances", \
 				["column_name", "data_type", "default"])("instances", "int", lambda: 100)])

 		if isinstance(instances, dict):
				instances = instances[next(a for a in instances if a.column_name in ["instances"])]


 		sql = f"""
  		
					INSERT INTO "Shop"."Ordered_product"("Product_id", "Ordered_amount", "Order_id")
				SELECT
					
					(SELECT "Product_id" FROM "Shop"."Products" ORDER BY random()*q LIMIT 1),
					trunc(random() * 100)::int,
					(SELECT "Order_id" FROM "Shop"."Order" ORDER BY random()*q LIMIT 1)
				
				FROM
					generate_series(1,{instances} ) as q;


 		"""
 		#{instances}
 		with self.schema.dbconn.cursor() as dbcursor:
 			try:
 				View.printInfo(sql)
 				t1 = datetime.datetime.now()
 				dbcursor.execute(sql)
 				t2 = datetime.datetime.now()
 				self.schema.dbconn.commit()
 			except Exception as e:
 				self.schema.dbconn.rollback()
 				View.printInfo(f"Something went wrong: {e}")
 			else:
 				View.printInfo(f"{self} {dbcursor.rowcount} rows added, execution time: {t2 - t1}")


class Shop(Schema):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._dynamicsearch = {a.name: a for a in [DynamicSearch.CategoriesProductsDynamicSearch(self),\
		DynamicSearch.ManufacturerProductsDynamicSearch(self),\
		DynamicSearch.UserOrderedProductsDynamicSearch(self),]}
		# self.reoverride()

	def reoverride(self):
		# Table override
		# self._tables.Loan = LoanTable(self, f"Loan")
		# print(f"self")
		self.tables.Categories = Categories(self, f"Categories")
		self.tables.Manufacturer = Manufacturer(self, f"Manufacturer")
		self.tables.Products = Products(self, f"Products")
		self.tables.User = User(self, f"User")
		self.tables.Order = Order(self, f"Order")
		self.tables.Ordered_product = Ordered_product(self, f"Ordered_product")

	def reinit(self):
		# sql = f"""
		# 	SELECT table_name FROM information_schema.tables
		# 	WHERE table_schema = '{self}';

		# """
		with self.dbconn.cursor() as dbcursor:
			# dbcursor.execute(sql)
			for a in self.refresh_tables():  # tuple(dbcursor.fetchall()):
				q = f"""DROP TABLE IF EXISTS {a} CASCADE;"""
				# print(q)
				dbcursor.execute(q)

		tables = [
			f"""CREATE SCHEMA IF NOT EXISTS "{self}";""",
			f"""CREATE TABLE IF NOT EXISTS "{self}"."Categories"(
				"Category_id" bigserial,
				"Category_name" character varying(70) NOT NULL,
				PRIMARY KEY ("Category_id")
				);
			""",
			f"""CREATE TABLE IF NOT EXISTS "{self}"."Manufacturer"(
				"Manufacturer_id" bigserial,
				"Manufacturer_name" character varying(70) NOT NULL,
				PRIMARY KEY ("Manufacturer_id")
			);
			""",
			f"""CREATE TABLE IF NOT EXISTS "{self}"."Products"(
				"Product_id" bigserial,
				"Category_id" bigint NOT NULL,
				"Manufacturer_id" bigint NOT NULL,
				"Product_name" character varying(70) NOT NULL,
				"Price" money NOT NULL,
				"Amount" integer NOT NULL,
				PRIMARY KEY ("Product_id"),
				FOREIGN KEY ("Category_id") REFERENCES "{self}"."Categories"("Category_id"),
				FOREIGN KEY ("Manufacturer_id") REFERENCES "{self}"."Manufacturer"("Manufacturer_id")
			);
			""",
			f"""CREATE TABLE IF NOT EXISTS "{self}"."User"(
				"User_data_id" bigserial,
				"Name" character varying(40) NOT NULL,
				"Surname" character varying(40) NOT NULL,
				"Patronymic" character varying(50) NOT NULL,
				"Email" character varying(255) NOT NULL,
				PRIMARY KEY ("User_data_id")
			);
			""",
			f"""CREATE TABLE IF NOT EXISTS "{self}"."Order"(
				"Order_id" bigserial,
				"User_data_id" bigint NOT NULL,
				PRIMARY KEY ("Order_id"),
				FOREIGN KEY ("User_data_id") REFERENCES "{self}"."User"("User_data_id")
			);
			""",
			f"""CREATE TABLE IF NOT EXISTS "{self}"."Ordered_product"(
				"Ordered_product_id" bigserial,
				"Product_id" bigint NOT NULL,
				"Ordered_amount" integer NOT NULL,
				"Order_id" bigint NOT NULL,
				PRIMARY KEY ("Ordered_product_id"),
				FOREIGN KEY ("Product_id") REFERENCES "{self}"."Products"("Product_id"),
				FOREIGN KEY ("Order_id") REFERENCES "{self}"."Order"("Order_id")
			);
			""",
		]

		with self.dbconn.cursor() as dbcursor:
			for a in tables:
				dbcursor.execute(a)

		self.dbconn.commit()

		self.refresh_tables()
		# self.reoverride()

	def randomFill(self):
		self.tables.Categories.randomFill(1_000)
		self.tables.Manufacturer.randomFill(1_000)
		self.tables.Products.randomFill(1_000)
		self.tables.User.randomFill(1_000)
		self.tables.Order.randomFill(1_000)
		self.tables.Ordered_product.randomFill(1_000)

	

def _test():
	pass


if __name__ == "__main__":
	_test()
