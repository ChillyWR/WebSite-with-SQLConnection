from loader import *
from SQLConnector import Connector
import logging
from flask import Flask, render_template, redirect, request

app = Flask(__name__)
connection: Connector = None
logging.basicConfig(level=logging.INFO)

def fields_check(form):
	emptyfields: bool = False
	for i in form.values():
		emptyfields = emptyfields or i == ''
	return emptyfields

@app.route('/')
def index():
	return render_template('index.html')

# TODO: button to order data (return redirect(url_for('index')))
@app.route('/select')
def select():
	data = connection.output_formatter(connection.select_table_data())
	logging.info(f"Data selected:\n{data}")
	return render_template('select.html', message=data)

@app.route('/insert', methods=['GET', 'POST'])
def insert():
	if request.method == 'POST':
		if fields_check(request.form):
			return render_template('insert.html', message='Fill all fields')
		connection.select_table('students')
		received = list(request.form.values())
		logging.info(f"Data received: {received}")
		group = received[3]
		received[3] = str(int(group[len(group) - 1]) + 1)
		connection.insert_table_data(received)
		return render_template('success.html')
	return render_template('insert.html')

@app.route('/update', methods=['GET', 'POST'])
def update():
	if request.method == 'POST':
		if fields_check(request.form):
			return render_template('update.html', message='Fill all fields')
		connection.select_table('students')
		received = list(request.form.values())
		logging.info(f"Data received: {received}")
		group = received[4]
		received[4] = str(int(group[len(group) - 1]) + 1)
		connection.update_table_data(int(received[0]), received[1:])
		return render_template('success.html')
	return render_template('update.html')

@app.route('/delete', methods=['GET', 'POST'])
def delete():
	if request.method == 'POST':
		if fields_check(request.form):
			return render_template('delete.html', message='Fill all fields')
		connection.select_table('students')
		received = list(request.form.values())
		logging.info(f"Data received: {received}")
		connection.delete_table_data(received)
		return render_template('success.html')
	return render_template('delete.html')

@app.route('/get_best_students', methods=['GET', 'POST'])
def best_student():
	data = connection.output_formatter(connection.get_best())
	logging.info(f"Data selected:\n{data}")
	return render_template('select.html', message=data)

@app.route('/get_best_groups', methods=['GET', 'POST'])
def best_groups():
	connection.select_table('groups')
	data = connection.output_formatter(connection.get_best())
	logging.info(f"Data selected:\n{data}")
	return render_template('select.html', message=data)

@app.route('/get_laggards', methods=['GET', 'POST'])
def get_laggards():
	data = connection.output_formatter(connection.get_laggards())
	logging.info(f"Data selected:\n{data}")
	return render_template('select.html', message=data)

@app.route('/group_leaders', methods=['GET', 'POST'])
def group_leaders():
	data = connection.output_formatter(connection.group_leaders())
	logging.info(f"Data selected:\n{data}")
	return render_template('select.html', message=data)

@app.route('/group_teacher_cathedra', methods=['GET', 'POST'])
def group_teacher_cathedra():
	data = connection.output_formatter(connection.group_teacher_cathedra())
	logging.info(f"Data selected:\n{data}")
	return render_template('select.html', message=data)



def open_connection():
	global connection
	connection = Connector(host, user, password, database_name)
	logging.info("Connected to MySQL database " + database_name)
	connection.select_table('students')


if __name__ == '__main__':
	open_connection()
	app.run(debug=True)