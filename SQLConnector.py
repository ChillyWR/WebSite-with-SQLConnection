import mysql.connector
from datetime import datetime

class Connector:
	def __init__(self, host_name, user_name, user_password, database_name):
		self._connection: mysql.connector.MySQLConnection = None
		self._cursor: mysql.connector.cursor.MySQLCursor = None
		self.__selected: bool = False
		self._table_names: list = None
		self._current_table_info: dict = None
		try:
			self._connection = mysql.connector.connect(
				host=host_name,
				user=user_name,
				passwd=user_password,
				database=database_name,
			)
			self._cursor = self._connection.cursor()
		except mysql.connector.Error as err:
			raise Exception(err)
		self._cursor.execute("SHOW tables")
		self._table_names = [i[0] for i in self._cursor.fetchall()]

	def get_table_names(self):
		return self._table_names

	def select_table(self, table_name: str):
		if table_name in self._table_names:
			self._cursor.execute("SELECT * FROM `" + table_name + '`')
			self._cursor.fetchall()
			self._current_table_info = {
				table_name: [
					list(self._cursor.column_names),
					[mysql.connector.FieldType.get_info(desc[1]) for desc in self._cursor.description]
				]
			}
			self.__selected = True
		else:
			raise Exception("Unknown table")

	def selected_check(func):
		def wrapper(self, *args, **kwargs):
			if self.__selected:
				return func(self, *args, **kwargs)
			else:
				raise Exception("Unknown table")
		return wrapper

	def dtypes_check(self, row: list):
		try:
			dtypes = list(self._current_table_info.values())[0][1]
			if len(row) == len(dtypes) - 1:
				for elem, dtype in zip(row, dtypes[1:]):
					if dtype == 'LONG':
						int(elem)
					elif dtype == 'FLOAT':
						float(elem)
					elif dtype == 'VAR_STRING':
						pass
					elif dtype == 'DATE':
						datetime.strptime(elem, r'%Y-%m-%d')
				return True
			else:
				return False
		except ValueError:
			return False

	@selected_check
	def get_column_names(self):
		return list(self._current_table_info.values())[0][0]

	@selected_check
	def select_table_data(self, indices: list = None, from_to: bool = False, rows_to_display: int = -1):
		current_table_name = list(self._current_table_info.keys())[0]
		if indices:
			id_name = list(self._current_table_info.values())[0][0][0]
			if from_to:
				self._cursor.execute(
					"SELECT * FROM `" + current_table_name + '`'
					" WHERE `" + id_name + '`' +
					" BETWEEN " + indices[0] + " AND " + indices[1]
				)
				return self._cursor.fetchall()
			else:
				result = []
				for index in indices:
					self._cursor.execute(
						"SELECT * FROM `" + current_table_name + '`' +
						" WHERE " + id_name + " = " + index)
					result.append(*self._cursor.fetchall())
				return result
		else:
			if rows_to_display == -1:
				self._cursor.execute("SELECT * FROM `" + current_table_name + '`')
				return self._cursor.fetchall()
			self._cursor.execute("SELECT * FROM `" + current_table_name + '`' + " LIMIT 0, " + str(rows_to_display))
			return self._cursor.fetchmany(rows_to_display)

	# only students table
	@selected_check
	def insert_table_data(self, row: list):
		if self.dtypes_check(row):
			self._cursor.execute(
				"INSERT INTO `" + list(self._current_table_info.keys())[0] + '` ' +
				"(`name`, `date_of_birth`, `average_score`, `groups_idgroups`)" +
				" VALUES " +
				f'("{row[0]}", "{row[1]}", {row[2]}, {row[3]});'
			)
			self._connection.commit()
		else:
			raise Exception("Invalid row format")

	@selected_check
	def update_table_data(self, index: int, row: list):
		if self.dtypes_check(row):
			self._cursor.execute(
				"UPDATE `" + list(self._current_table_info.keys())[0] + '` ' + "SET "
				f'`name` = "{row[0]}", '
				f'`date_of_birth` = "{row[1]}", '
				f'`average_score` = {row[2]}, '
				f'`groups_idgroups` = {row[3]} '
				f"WHERE `{list(self._current_table_info.values())[0][0][0]}` = {index};"
			)
			self._connection.commit()
		else:
			raise Exception("Invalid row format")

	@selected_check
	def delete_table_data(self, indices: list, from_to: bool = False):
		current_table_name = list(self._current_table_info.keys())[0]
		id_name = list(self._current_table_info.values())[0][0][0]
		if from_to:
			self._cursor.execute(
				"DELETE * FROM " + current_table_name
				+ " WHERE " + id_name
				+ "BETWEEN" + indices[0] + " AND " + indices[1]
			)
		else:
			for index in indices:
				self._cursor.execute(
					"DELETE FROM " + current_table_name
					+ " WHERE " + id_name + " = " + index)
		self._connection.commit()
	
	# get students with ave sc < group ave sc  laggards

	@selected_check
	def get_best(self):
		self._cursor.execute(
			f"SELECT * FROM `knu`.`{list(self._current_table_info.keys())[0]}` "
			"ORDER BY `average_score` DESC LIMIT 5;"
		)
		return self._cursor.fetchall()

	def get_laggards(self):
		self._cursor.execute(
			"SELECT `students`.`idstudents`, `students`.`name`, `date_of_birth`, `students`.`average_score`, `groups`.`name`, `groups`.`average_score` "
			"FROM `students` "
			"INNER JOIN `groups` "
			"ON `students`.`groups_idgroups` = `groups`.`idgroups` "
			"WHERE `groups`.`average_score` - `students`.`average_score` > 10; "
		)
		return self._cursor.fetchall()

	def group_leaders(self):
		self._cursor.execute(
			"SELECT `groups`.`name`, `groups`.`students_amount`, `groups`.`average_score`, "
			"`students`.`name`, `students`.`date_of_birth`, `students`.`average_score` "
			"FROM `groups` "
			"JOIN `group_leaders` ON `groups`.`idgroups` = `group_leaders`.`idgroup_leaders` "
			"JOIN `students` ON `students`.`idstudents` = `group_leaders`.`students_idstudents`;"
		)
		return self._cursor.fetchall()

	def group_teacher_cathedra(self):
		self._cursor.execute(
			"SELECT `groups`.`name`, `groups`.`students_amount`, `groups`.`average_score`, "
			"`teachers`.`name`, "
			"`cathedras`.`name` "
			"FROM `groups` "
			"JOIN `teachers_groups` ON `groups`.`idgroups` = `teachers_groups`.`groups_idgroups` "
			"JOIN `teachers` ON `teachers`.`idteachers` = `teachers_groups`.`teachers_idteachers` "
			"JOIN `cathedras` ON `cathedras`.`idcathedras` = `teachers`.`cathedras_idcathedras` "
			"ORDER BY `groups`.`name`;"
		)
		return self._cursor.fetchall()

	@staticmethod
	def output_formatter(data):
		output: str = ""
		max_len = []
		for i in range(len(data[0])):
			max_len.append(max([len(str(row[i])) for row in data]))
		for row in data:
			for i, elem in enumerate(row):
				current_len = len(str(elem))
				output += str(elem) + ' ' * (max_len[i] - current_len) + ' '
			output += '\n'
		return output

	# def __del__(self):
	# 	if self._connection:
	# 		self._connection.close()


# умеет создавать новые записи
# умеет редактировать существующие записи
# умеет удалять существующие записи
# умеет выводить данные на экран(Select-ы с Join-ами, с условиями и сортировками
# умеет выводить статистику(какую придумаете)
