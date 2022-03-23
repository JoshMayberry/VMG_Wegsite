""" These functions are specifically for the programs in this directory. """

import io
import os
import sys
import json
import time
import logging
import datetime
import requests
import itertools
import collections
import configparser

import numpy
import pandas
import dropbox
import psycopg2
import psycopg2.extras
from webflowpy.Webflow import Webflow

import common
import storage_util

# pip install
	# pandas
	# dropbox
	# psycopg2
	# Webflowpy
	# azure-storage-blob

debugging = False
logger = logging.getLogger(__name__)
def logger_debug():
	""" Changes the log level to debug.
	See: https://docs.python.org/3/howto/logging.html

	Example Input: logger_debug()
	"""
	global debugging

	debugging = True # Use this to short-circuit expensive f-strings
	logging.basicConfig(level=logging.DEBUG)

def logger_info():
	""" Changes the log level to info.
	See: https://docs.python.org/3/howto/logging.html

	Example Input: logger_info()
	"""

	logging.basicConfig(level=logging.INFO)

logger_timers = collections.defaultdict(dict)
def logger_timer(label=None):
	""" A decorator that records how long a function took to execute.
	If the logger level is not info or debug, will not do anything.
	See: https://docs.python.org/3/howto/logging.html#optimization

	label (str) - What the timer is called
		- If None: Will assign a unique number as the label for this timer

	EXAMPLE USE
		@logger_timer()
		def longFunction():
			pass

	EXAMPLE USE
		@logger_timer("lorem")
		def longFunction():
			pass
	"""

	if (not logger.isEnabledFor(logging.INFO)):
		def decorator(myFunction):
			return myFunction
		return decorator

	def decorator(myFunction):
		def wrapper(*args, **kwargs):
			nonlocal label

			if (label is None):
				label = len(logger_timers)
				while label in logger_timers:
					label += 1

			catalogue = logger_timers[label]

			catalogue["start"] = time.perf_counter()
			answer = myFunction(*args, **kwargs)
			catalogue["end"] = time.perf_counter()
			return answer
		return wrapper
	return decorator

def logger_timer_print(label=None):
	""" Prints the times for timers.

	label (str) - Which timer to print the time for
		- If None: Will print for all timers
		- If tuple: WIll print for each timer in the list

	Example Input: logger_timer_print()
	Example Input: logger_timer_print("lorem")
	"""

	for _label in common.ensure_container(label, convertNone=False, returnForNone=lambda: logger_timers.keys()):
		catalogue = logger_timers[_label]
		print(f"{_label}: {catalogue.get('end', time.perf_counter()) - catalogue['start']:.2f}")

def tryExcept(myFunction):
	""" A decorator the surrounds the inner function with a try/except

	EXAMPLE USE
		@tryExcept
		def fragileFunction():
			pass
	"""

	def wrapper(*args, **kwargs):
		try:
			return myFunction(*args, **kwargs)
		except Exception as error:
			print(error)
	return wrapper

def config(filename="database.ini", section="postgresql", **kwargs):
	""" Reads in the database.ini file and returns the connection parameters as a dictionary.
	Use: http://www.postgresqltutorial.com/postgresql-python/connect/

	filename (str) - Where the ini file is located
	section (str) - Which ini section to read from

	Example Input: config()
	Example Input: config("lorem.ini")
	Example Input: config(section="ipsum")
	"""

	parser = configparser.ConfigParser()
	parser.read(filename)
 
	if not parser.has_section(section):
		raise ValueError(f"Section '{section}' not found in the '{filename}' file")

	return {key: value for (key, value) in parser.items(section)}

def _ma_getToken(login_user="VinApi", login_password="9@%sZqRxDeZ@38s", **kwargs):
	""" Function to return MA access token.
	See: https://docs.python-requests.org/en/v1.0.0/api/#main-interface

	login_user (str) - The username to use for logging in
	login_password (str) - The password to use for logging in

	Example Input: _ma_getToken()
	Example Input: _ma_getToken("Lorem", "Ipsum")
	"""

	logging.info("Getting MA token...")
	response = requests.request("POST", "https://api.manageamerica.com/api/account/signin",
		headers = {"Content-Type": "application/json"},
		data = json.dumps({"login": login_user, "password": login_password}),
	)
	token = response.json()["token"]
	logging.debug(f"ma_token: '{token}'")
	return token

def ma_getData(url, *, token=None, alias=None, modifyData=None, filterData=None, customReport=False, **kwargs):
	""" Returns data from Manage America.

	url (str or tuple) - The URL to pull the data from
		- If tuple: Will get the data from each URL in the list
	alias (dict of str) - Key names to replace in the data source if they exist
	modifyData (function) - A function which modifies the data before it is returned
	filterData (function) - A function which filters the data before it is modified or returned
	customReport (bool) - If the result should be parsed as a manage america 'adds report'

	Example Input: ma_getData("https://n6.manageamerica.com/api/property/?companyId=233")
	Example Input: ma_getData("https://n6.manageamerica.com/api/property/?companyId=233", modifyData=lambda data: [*data, {"lorem": "ipsum"}])
	Example Input: ma_getData("https://n6.manageamerica.com/api/property/?companyId=233", filterData=lambda item: row.get("type") == "Detail")
	Example Input: ma_getData(["https://www.lorem.com", "https://www.ipsum.com"])
	Example Input: ma_getData("https://n6.manageamerica.com/api/addsReport/v1/runReport?Company_Id=233&Report_Id=4553", customReport=True)
	Example Input: ma_getData("https://n6.manageamerica.com/api/property/?companyId=233", alias={"Old Name 1": "new_name_1", "Old Name 2": "new_name_2"})
	"""

	token = token or _ma_getToken(**kwargs)

	data = []
	urlList = common.ensure_container(url)
	# urlList = common.ensure_container(url)[:1] #FOR DEBUGGING
	for (index, _url) in enumerate(urlList):
		logging.info(f"Getting data for url {index + 1}: '{_url}'")
		response = requests.request("GET", _url,
			headers = {"Authorization": f"Bearer {token}"},
			data = {},
		)
		try:
			answer = response.json()
			if (isinstance(answer, dict)):
				if (answer.get("message", None)):
					raise ValueError(answer)
				data.append(answer)
			else: 
				data.extend(answer or ())
		except requests.exceptions.JSONDecodeError as error:
			print(f"url: '{_url}'")
			raise error

	# logging.debug(debugging and f"ma_response: '{data}'")
	if (not len(data)):
		return

	if (customReport):
		logging.info("Parsing custom report data...")
		columns = tuple(item["name"] for item in data[0]["columns"])
		data = tuple({key: value for (key, value) in itertools.zip_longest(columns, item["data"])} for item in data[0]["rows"] if (item["type"] != "Total"))
		if (not len(data)):
			return

	if (alias):
		logging.info("Applying alias to data...")
		data = tuple({alias.get(key, key): value for (key, value) in catalogue.items()} for catalogue in data)

	if (filterData):
		logging.info("Filtering data...")
		for myFunction in common.ensure_container(filterData):
			data = data.filter(myFunction)

	if (modifyData):
		logging.info("Modifying data...")
		for myFunction in common.ensure_container(modifyData):
			data = myFunction(data)

	return data

def fs_getData(form, *, view=None, server="fs8", user="e1SwO6", bearer="Bearer 4EBZMpQtMxB2eD5l4se6jH7QHx52V12q", cookie="AWSALB=JhqCn6DQPrZqvO4fk8jOMyB8fucxFlrSohvhblAANzGrN9cR3vUU89YLyXtVwFT/ka+jumGxcejmeI/gGMqX6zo/2fQRKTPUXRTzpr8rhOptlInl0bsVu2Gc3HP8; AWSALBCORS=JhqCn6DQPrZqvO4fk8jOMyB8fucxFlrSohvhblAANzGrN9cR3vUU89YLyXtVwFT/ka+jumGxcejmeI/gGMqX6zo/2fQRKTPUXRTzpr8rhOptlInl0bsVu2Gc3HP8"):
	""" Returns data from Formsite.

	form (str or tuple) - The form code of the form to return data for 
		- If tuple: Will get the data from each form code in the list
	view (str) - Which result view to return data for
	append (bool) - If the results should be appended to what is already in the file or not
	server (str) - Which formsite server to use
	user (str) - Which formsite user account to use
	bearer (str) - The bearer token to use
	cookie (str) - The cookies token to send

	Example Input: fs_getData("wjsrave6n6")
	Example Input: fs_getData("rf1gmwaueh", view=101)
	"""

	data = []

	headers = {
		"Authorization": bearer,
		"Cookie": cookie,
	}

	for _form in common.ensure_container(form):
		url_base = f"https://{server}.formsite.com/api/v2/{user}/forms/{_form}/"

		# Get Pipe Labels
		response = requests.request("GET", f"{url_base}/items", headers=headers)
		pipe_labels = {item["id"]: item["label"] for item in response.json()["items"]}

		# Get Pipe Values
		response = requests.request("GET", f"{url_base}/results", headers=headers)
		last_page = response.headers["Pagination-Page-Last"]
		for page in range(1, int(last_page) + 1):
			_url = f"{url_base}/results?page={page}"
			if (view):
				_url += f"&results_view={view}"

			logging.info(f"Getting data for url {page} of {last_page}: '{_url}'")

			response = requests.request("GET", _url, headers=headers)

			for row in response.json()["results"]:
				if (("date_start" not in row) or ("date_finish" not in row)):
					continue;

				pipe_values = {}
				catalogue = {
					"id": row["id"],
					"date_start": datetime.datetime.strptime(row["date_start"], "%Y-%m-%dT%H:%M:%SZ"),
					"date_finish": datetime.datetime.strptime(row["date_finish"], "%Y-%m-%dT%H:%M:%SZ"),
					"date_update": datetime.datetime.strptime(row["date_update"], "%Y-%m-%dT%H:%M:%SZ"),
					"pipe_labels": pipe_labels,
					"pipe_values": pipe_values,
				}

				for item in row["items"]:
					if "values" not in item:
						pipe_values[item["id"]] = item["value"]
						continue

					match len(item["values"]):
						case 0:
							pipe_values[item["id"]] = None

						case 1:
							pipe_values[item["id"]] = item["values"][0]["value"]

						case _:
							raise NotImplementedError(f"Multiple formsite values; {item['values']}")

				catalogue["pipe_combined"] = json.dumps({pipe_labels[key]: value for (key, value) in pipe_values.items()})
				catalogue["pipe_labels"] = json.dumps(catalogue["pipe_labels"])
				catalogue["pipe_values"] = json.dumps(catalogue["pipe_values"])

				data.append(catalogue)

	return data

def _posgres_renameKeys(iterable):
	""" A recursive function to convert keys to lowercase.
	Otherwise, json keys won't match postgresql column names.

	iterable (list or dict) - What to iterate over

	Example Input: _posgres_renameKeys([{"Lorem": 1, "IPSUM": 2, "dolor": 3}])
	"""

	if isinstance(iterable, dict):
		new_object = {}
		for (key, value) in iterable.items():
			new_object[key.lower()] = value if not isinstance(iterable, (dict, list, tuple)) else _posgres_renameKeys(value)
		return new_object

	if isinstance(iterable, (list, tuple)):
		return [_posgres_renameKeys(item) for item in iterable]

	return iterable

def posgres_insert(data, table, *, method="upsert", insert_method="single", backup=None, lowerNames=True, typeCatalogue=None, **kwargs):
	""" Adds data to a posgres database.
	See: https://www.psycopg.org/docs/usage.html#query-parameters

	data (tuple of dict) - What to send to the database
	table (str) - Which table to send the data to
	method (str) - How to handle sending the data
		- upsert: Update existing data in the db, otherwise insert a new row
		- drop: Drop all rows in the table and insert new rows
		- truncate: Cleanly drop all rows in the table and insert new rows  (See: https://stackoverflow.com/questions/11419536/postgresql-truncation-speed/11423886#11423886)
	insert_method (str) - How to handle inserting things into the database
		- json: Pass in a JSON string with all the data (Does not allow non-serializable inputs such as datetime objects)
		- separate: Do an individual insert for each row (Much slower)
		- single: Do a single insert statement for every 900 rows
	backup (dict or str) - How to backup what is inserted
		- kind (required): Used as the string
			- dropbox: Send to dropbox
			- blob: Send to blob storage
		- other keys: kwargs to send
		- If str: Assumed to be *backup.kind*
	lowerNames (bool) - If object keys should be lower-cased
	typeCatalogue (dict) - What type specific columns need to be; where the key is the column name and the value is one of the following strings:
		- json: The data should be a JSON string (will fail if the column's value contains non-serializable values)

	Example Input: posgres_insert([{"lorem": "ipsum"}], "property")
	Example Input: posgres_insert([{"Lorem": "ipsum"}], "property", lowerNames=True)
	Example Input: posgres_insert([{"lorem": "ipsum"}], "property", backup="dropbpx")
	Example Input: posgres_insert([{"lorem": "ipsum"}], "property", backup={"kind": "dropbpx", "folder": "rps"})
	Example Input: posgres_insert([{"lorem": datetime.datetime.now()}], "property", insert_method="separate")
	Example Input: posgres_insert(frame, "property")
	Example Input: posgres_insert([{"lorem": "ipsum"}], "property", method="drop")
	Example Input: posgres_insert([{"lorem": {"ipsum": 1}}], "property", typeCatalogue={"lorem": "json"})
	"""

	if (isinstance(data, pandas.DataFrame)):
		# See: https://pandas.pydata.org/pandas-docs/version/0.17.0/generated/pandas.DataFrame.to_dict.html#pandas.DataFrame.to_dict
		data = data.replace({numpy.nan: None}).to_dict("records")

	if (not len(data)):
		logging.info(f"No data to insert into '{table}'")
		return False

	if (lowerNames):
		data = _posgres_renameKeys(data)

	if (typeCatalogue):
		for (key, value) in typeCatalogue.items():
			for row in data:
				if (key not in row):
					continue

				match value:
					case "json":
						if (isinstance(row[key], str)):
							continue
							
						row[key] = json.dumps(row[key])

					case _:
						raise KeyError(f"Unknown *typeCatalogue[{key}]* '{value}'")

	queries = []
	no_update = (method != "upsert")
	if (no_update):
		match method:
			case "drop":
				queries.append([f"DELETE FROM {table}", ()])

			case "truncate":
				queries.append([f"TRUNCATE TABLE {table} RESTART IDENTITY", ()])

			case _:
				raise KeyError(f"Unknown *method* '{method}'")

	match insert_method:
		case "json":
			queries.append([
				f"INSERT INTO {table} SELECT p.* FROM jsonb_populate_recordset(NULL::{table}, %s) as p" +
					("" if no_update else f" ON CONFLICT ON CONSTRAINT {table}_pkey DO UPDATE SET {', '.join(f'{key} = EXCLUDED.{key}' for key in data[0].keys())}"),
				(json.dumps(data),)
			])

		case "separate":
			for row in data:
				keyList = tuple(row.keys())
				queries.append([
					f"INSERT INTO {table} ({', '.join(keyList)}) VALUES ({', '.join(f'%({key})s' for key in keyList)})" +
						("" if no_update else f" ON CONFLICT ON CONSTRAINT {table}_pkey DO UPDATE SET {', '.join(f'{key} = EXCLUDED.{key}' for key in keyList)}"),
					row
				])

		case "single":
			keyList = tuple(data[0].keys())

			for chunk in (data[i:i+900] for i in range(0, len(data), 900)):
				valueList = []
				valueCatalogue = {}

				for (i, row) in enumerate(chunk):
					valueList.append(f"({', '.join(f'%({key}_{i})s' for key in keyList)})")
					valueCatalogue.update({f"{key}_{i}": value for (key, value) in row.items()})

				queries.append([
					f"INSERT INTO {table} ({', '.join(keyList)}) VALUES {', '.join(valueList)}" +
						("" if no_update else f" ON CONFLICT ON CONSTRAINT {table}_pkey DO UPDATE SET {', '.join(f'{key} = EXCLUDED.{key}' for key in keyList)}"),
					valueCatalogue
				])

		case _:
			raise KeyError(f"Unknown *insert_method* '{insert_method}'")

	# See: https://www.psycopg.org/docs/connection.html
	logging.info("Making posgres connection...")
	connection = psycopg2.connect(**config(**kwargs))
	with connection:
		connection.autocommit = True
		with connection.cursor() as cursor:
			logging.info(f"Sending {len(queries)} queries...")
			for (query_sql, query_args) in queries:
				logging.debug(debugging and f"query_sql: '{query_sql}'")
				logging.debug(debugging and f"query_args: '{query_args}'")
				cursor.execute(query_sql, query_args or ())
	logging.info("Closing posgres connection...")
	connection.close()

	if (backup):
		backup = common.ensure_dict(backup, "kind")
		kind = backup.get("kind", None)

		if (not backup.get("filename", None)):
			# See: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
			backup["filename"] = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S") + ".csv"

		match kind:
			case "dropbox":
				return dropbox_insert(data, folder=table, input_type="csv", **backup)

			case "blob":
				return blobStorage_insert(data, folder=table, input_type="csv", **backup)

			case None:
				raise ValueError("Required key missing: *backup.kind*")

			case _:
				raise KeyError(f"Unknown *backup.kind* '{kind}'")

	return True

def posgres_raw(query_sql, query_args=None, *, as_dict=True, **kwargs):
	""" Returns the answer to a raw sql statement to ther database.

	table (str) - Which table to get data from
	
	Example Input: posgres_select("SELECT * FROM property")
	Example Input: posgres_select("SELECT * FROM property WHERE id = %s", (1,))
	Example Input: posgres_select("SELECT id FROM property", as_dict=False)
	"""

	# See: https://www.psycopg.org/docs/connection.html
	logging.info("Making posgres connection...")
	connection = psycopg2.connect(**config(**kwargs))
	with connection:
		cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if as_dict else connection.cursor()
		with cursor:
			logging.info("Sending raw query...")
			cursor.execute(query_sql, query_args or ())
			answer = [dict(catalogue) for catalogue in cursor.fetchall()] if as_dict else cursor.fetchall()
			logging.info(f"Recieved '{len(answer or ())}' results")
	logging.info("Closing posgres connection...")
	connection.close()

	return answer

def _blobStorage_getConnection(container, *, account_name="rpbireporting", account_key="bAOuK2OF8AEepv1fjDR+cnBS675dbkikS9jyj0Wz1PV9ST/urHDOp3MrRsZMsR/j2tjXfEQeb7MdwevrYbxKdw==", **kwargs):
	""" Returns a blob storage connection.

	account_name (str) - The name of the account to connect to
	account_key (str)

	Example Input: _blobStorage_getConnection("treehouse")
	Example Input: _blobStorage_getConnection("ma-extract", account_name="birdeye01reporting", account_key="/bVON7ccB6Dt9K46adTgtxppuF2gLosUoNwuWOgKerNyLKOxhyH2Gahogotp4PrP11avJDd4axlHCvdHTb132A==")
	"""

	logging.info(f"Making blob storage connection to '{container}'...")
	connection_string = ";".join([
		"DefaultEndpointsProtocol=https",
		f"AccountName={account_name}",
		f"AccountKey={account_key}",
		f"BlobEndpoint=https://{account_name}.blob.core.windows.net/",
		f"QueueEndpoint=https://{account_name}.queue.core.windows.net/",
		f"TableEndpoint=https://{account_name}.table.core.windows.net/",
		f"FileEndpoint=https://{account_name}.file.core.windows.net/;",
	])

	logging.debug(f"Connection string: '{connection_string}'")
	return storage_util.DirectoryClient(connection_string, container)

def _yield_fileOutput(data, folder=None, filename=None, *, input_type="csv", walk_allow=("csv",), **kwargs):
	""" A generator that yields file handles and their intended destinations based on the input criteria.

	data (any) - What to send to the implemented storage
	folder (str) - What folder path of the container to store the file(s) in
		- If None: Will put the file in the root directory
	filename (str) - What file name to use for the file
		- If None: Will try coming up with a file name
	input_type (str) - How to handle parsing the input data
		- raw: Just send it as recieved
		- json: If it is not a string
		- csv: Make it a csv file
		- file: Will use *data* as a filepath to look it up from disk; if there is no file extension it will walk that directory and send all files contained there
	walk_allow (tuple) - What file extensions to allow from walking the directory for *input_type*

	Example Input: _yield_fileOutput([{Lorem: "ipsum"}])
	Example Input: _yield_fileOutput([{Lorem: "ipsum"}], folder="rps")
	Example Input: _yield_fileOutput({Lorem: "ipsum"}, input_type="json")
	Example Input: _yield_fileOutput("C:/lorem/ipsum", input_type="file")
	Example Input: _yield_fileOutput("C:/lorem/ipsum", input_type="file", walk_allow=("csv", "xlsx"))
	Example Input: _yield_fileOutput(open("lorem.txt", "r"), filename="lorem.txt", input_type="raw")
	"""

	data_isPandas = isinstance(data, pandas.DataFrame)
	if (data_isPandas):
		input_type = "csv"

	match input_type:
		case "file":
			if (os.path.splitext(data)[1]):
				destination = os.path.join(folder, os.path.basename(data))
				logging.info(f"Send '{data}' to '{destination}'")
				with open(file_from, "rb") as handle_file:
					yield (handle_file, destination)
				return True

			for (root, _, files) in os.walk(data):
				for filename_source in files:
					if (os.path.splitext(filename_source)[1][1:] not in walk_allow):
						continue

					source = os.path.join(root, filename_source)
					destination = os.path.join(folder, filename or filename_source)
					logging.info(f"Send '{source}' to '{destination}'")
					with open(source, "rb") as handle_file:
						yield (handle_file, destination)

		case "raw":
			destination = os.path.join(folder, filename)
			logging.info(f"Send raw data to '{destination}'")
			yield (data, destination)

		case "json":
			destination = os.path.join(folder, filename)
			logging.info(f"Send raw json to '{destination}'")
			with io.BytesIO(json.dumps(data).encode("utf8")) as handle_file:
				yield (handle_file, destination)

		case "csv":
			if (not len(data)):
				logging.info(f"No data to write to csv")
				return

			destination = os.path.join(folder, filename).replace("\\", "/")
			logging.info(f"Send raw csv to '{destination}'")

			# See: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
			# See: https://stackoverflow.com/questions/13120127/how-can-i-use-io-stringio-with-the-csv-module/45608450#45608450
			with io.StringIO(newline="") as handle_csv:
				frame = data if data_isPandas else pandas.DataFrame(data)
				frame.to_csv(handle_csv, header=True, index=False, date_format=r"%Y-%m-%dT%H:%M:%S.%fZ")
				yield (io.BytesIO(handle_csv.getvalue().encode('utf8')), destination)

		case _:
			raise KeyError(f"Unknown *input_type* '{input_type}'")

def blobStorage_insert(data, container="postgres", folder=None, filename=None, *, method="upsert", **kwargs):
	""" Sends data to an Azure blob storage.

	data (any) - What to send to the blob storage
	container (str) - Which blob storage container to store the blob(s) in
	folder (str) - What folder path of the container to store the blob(s) in
		- If None: Will put the file in the root directory
	filename (str) - What file name to use for the blob
		- If None: Will try coming up with a file name
	method (str) - How to handle sending the data
		- upsert: Update existing blob in the folder, otherwise create a new blob
		- insert: Try adding it and throw an error if it alrteady exists
		- drop: Drop all blobs in the folder and insert new blobs

	Example Input: blobStorage_insert([{Lorem: "ipsum"}])
	Example Input: blobStorage_insert([{Lorem: "ipsum"}], container="treehouse", folder="rps")
	Example Input: blobStorage_insert({Lorem: "ipsum"}, input_type="json")
	Example Input: blobStorage_insert("C:/lorem/ipsum", input_type="file")
	Example Input: blobStorage_insert("C:/lorem/ipsum", input_type="file", walk_allow=("csv", "xlsx"))
	Example Input: blobStorage_insert(open("lorem.txt", "r"), filename="lorem.txt", input_type="raw")
	"""

	filename = filename or "unknown.txt"
	blobStorage = _blobStorage_getConnection(container, **kwargs)

	contents = blobStorage.ls_files(folder or "")
	if (len(contents)):
		match method:
			case "drop":
				logging.info(f"Dropping the following from '{folder}': {contents}")
				for filename_source in contents:
					blobStorage.rm(f"{folder}/{filename_source}")

			case "upsert":
				for filename_source in contents:
					if (filename_source.endswith(filename)):
						logging.info(f"Dropping '{filename_source}' from '{folder}'")
						blobStorage.rm(f"{folder}/{filename_source}")

			case "insert":
				pass

			case _:
				raise KeyError(f"Unknown *method* '{method}'")

	found = False
	for (handle_binary, destination) in _yield_fileOutput(data=data, folder=folder, filename=filename, **kwargs):
		found = True
		blobStorage.client.upload_blob(name=destination, data=handle_binary.read())

	if (not found):
		raise ValueError("No files were found")

	return True

def blobStorage_select(container="postgres", folder=None, filename=None, *,
	input_type="csv", output_type="python", as_dict=True,
	multifile_method="append", force_list=False, **kwargs):
	""" Returns data from blob storage

	container (str or tuple)) - Which blob storage container to store the blob(s) in
		- If tuple: Will look in all container names given
	folder (str or tuple)) - What folder path of the container to store the blob(s) in
		- If None: Will put the file in the root directory
		- If tuple: Will look in all folder names given
	filename (str or tuple) - What file name to use for the blob
		- If None: Will try coming up with a file name
		- If tuple: Will look for all files given
	output_type (str) - How to return what is in the blob storage
		- client: Pre-download file handle
		- handle_bin: Post-download file handle
		- bin: The raw binary string contents of the blob
		- handle_str: Stringified file handle
		- str: The raw string contents of the blob
		- python: Make it into a python object
	output_type (str) - How to interpret the blob when *output_type* is 'python'
		- csv: A list of dictionaries
	as_dict (bool) - If csv file contrents sholuld be returnded as a list of dictionaries
	multifile_method (str) - How to handle when multiple files are requested (Only applies to the 'python' *output_type*; all other output types will use *multifile_method* as 'separate')
		- append: Add the results from all following files to the ones from the first
		- separate: Each result is a separate item in a list
	force_list (bool) - If empty lists or single item lists should still be rreturned as lists

	Example Input: blobStorage_select(container="ma-extract", folder="treehouse", filename="Resident.csv")
	Example Input: blobStorage_select(container="ma-extract", folder="treehouse", filename="Resident.csv", as_dict=False)
	Example Input: blobStorage_select(container="ma-extract", folder="treehouse", filename="Resident.csv", output_type="handle_str")
	Example Input: blobStorage_select(container="ma-extract", folder="[treehouse", "vineyards"], filename="Resident.csv")
	Example Input: blobStorage_select(container="ma-extract", folder="[treehouse", "vineyards"], filename="Resident.csv", multifile_method="separate")
	Example Input: blobStorage_select(container="ma-extract", folder="treehouse", filename="Resident.csv", force_list=True)
	"""

	def yieldFile():
		for _container in common.ensure_container(container):
			blobStorage = _blobStorage_getConnection(_container, **kwargs)

			for _folder in common.ensure_container(folder):
				for _filename in common.ensure_container(filename or "unknown.txt"):
					client = blobStorage.client.get_blob_client(blob=os.path.join(_folder, _filename))
					if (output_type == "client"):
						yield client
						continue

					handle_blob = client.download_blob()
					if (output_type == "handle_blob"):
						yield handle_blob
						continue

					data_bin = handle_blob.readall()
					if (output_type == "blob"):
						yield data_bin
						continue

					try:
						handle_str = io.TextIOWrapper(io.BytesIO(data_bin), encoding="Windows-1252")
					except UnicodeDecodeError as error:
						import chardet
						handle_str = io.TextIOWrapper(io.BytesIO(data_bin), encoding=chardet.detect(data_bin))

					if (output_type == "str"):
						yield handle_str.read()
						continue

					yield handle_str

	###############################

	output = tuple(yieldFile())
	output_count = len(output)
	if (not output_count):
		return () if force_list else None

	if (output_type != "python"):
		return output if (force_list or (output_count > 1)) else output[0]

	match input_type:
		case "csv":

			answer = []
			for item in output:
				# See: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas-read-csv
				# See: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
				frame = pandas.read_csv(item)
				frame.to_csv(header=True, index=False, date_format=r"%Y-%m-%dT%H:%M:%S.%fZ")
				answer.append(frame)

			if (multifile_method == "separate"):
				return answer

			# See: https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html
			return pandas.concat(answer, join="outer", ignore_index=True, sort=False)

		case _:
			raise KeyError(f"Unknown *input_type* '{input_type}'")

def dropbox_insert(data, container="systems_data/report_data_source", folder=None, filename=None, *, method="upsert", token="jVPlY1ZQdYAAAAAAAAADjMh1aeLjQKPW7Sv57fsyzwxkcnxFcnXUeFIF9Hez35Mz", **kwargs):
	""" Sends data to dropbox.

	data (any) - What to send to the dropbox
	container (str) - Which dropbox root folder to store the file(s) in
	folder (str) - What folder path of the container to store the file(s) in
		- If None: Will put the file in the root directory
	filename (str) - What file name to use for the file
		- If None: Will try coming up with a file name
	method (str) - How to handle sending the data
		- upsert: Update existing file in the folder, otherwise create a new file
		- insert: Try adding it and throw an error if it alrteady exists
		- drop: Drop all blobs in the folder and insert new blobs
	token (str) - The access token for the dropbox account

	Example Input: dropbox_insert([{Lorem: "ipsum"}])
	Example Input: dropbox_insert([{Lorem: "ipsum"}], container="treehouse", folder="rps")
	Example Input: dropbox_insert({Lorem: "ipsum"}, input_type="json")
	Example Input: dropbox_insert("C:/lorem/ipsum", input_type="file")
	Example Input: dropbox_insert("C:/lorem/ipsum", input_type="file", walk_allow=("csv", "xlsx"))
	Example Input: dropbox_insert(open("lorem.txt", "r"), filename="lorem.txt", input_type="raw")
	"""

	filename = filename or "unknown.txt"
	dropboxHandle = dropbox.Dropbox(token)

	# See: https://dropbox-sdk-python.readthedocs.io/en/latest/api/dropbox.html#dropbox.dropbox_client.Dropbox.files_list_folder
	# contents = dropboxHandle.files_list_folder(folder or "")
	# if (len(contents)):
	# 	match method:
	# 		case "drop":
	# 			raise NotImplementedError("Dropbox drop all folder contents")

	# 		case "upsert":
	# 			pass

	# 		case "insert":
	# 			raise NotImplementedError("Dropbox insert file or rename to be unique")

	# 		case _:
	# 			raise KeyError(f"Unknown *method* '{method}'")

	# See: https://dropbox-sdk-python.readthedocs.io/en/latest/api/dropbox.html#dropbox.dropbox_client.Dropbox.files_upload
	for (handle_binary, destination) in _yield_fileOutput(data=data, folder=folder, filename=filename, **kwargs):
		dropboxHandle.files_upload(handle_binary.read(), os.path.join("/", container, destination).replace("\\","/"))

	return True

# Deprecated names
renameKeys = _posgres_renameKeys
get_manage_america_token = _ma_getToken

def get_json_response_ma(token, url, **kwargs):
	""" Function to return json response from MA.

	token (str) - The token to use for this request
	url (str) - The url to send the request to

	Example Input: get_json_response_ma(token, "https://n6.manageamerica.com/api/property/?companyId=233")
	Example Input: get_json_response_ma(token, ["https://www.lorem.com", "https://www.ipsum.com"])
	"""

	logging.info("DEPRECATED: Getting MA Data...")
	response = requests.request("GET", url,
		headers = {"Authorization": f"Bearer {token}"},
		data = {},
	)
	data = response.json()

	logging.debug(debugging and f"ma_response: '{data}'")
	return data

def webflow_select(collection_id, limit=-1, *, offset=0, token="0951d02dbfafb852a8249f1f34db9da5166bb7378872b32b1d4fd8f103002c0f", site_id="623107ba68bd7b6644a033c0" ):
	""" Returns the data from a webflow collection.
	See: https://www.briantsdawson.com/blog/webflow-api-how-to-get-site-collection-and-item-ids-for-zapier-and-parabola-use

	collection_id (str) - Which collection to connect to
	limit (int) - How much data to return. If less than 1 will return everything
	offset (int) - Use in combination with limit being not less than 1

	Example Input: webflow_select(collection_id="623107ba68bd7ba11ca033c7")
	Example Input: webflow_select(collection_id="623107ba68bd7ba11ca033c7", limit=1)
	Example Input: webflow_select(collection_id="623107ba68bd7ba11ca033c7", limit=100, offset=100)
	"""

	webflow_api = Webflow(token=token)
	return webflow_api.collection(collection_id=collection_id, limit=limit, all=limit <= 0)["items"]

def webflow_insert(collection_id, *, token="0951d02dbfafb852a8249f1f34db9da5166bb7378872b32b1d4fd8f103002c0f", site_id="623107ba68bd7b6644a033c0" ):
	""" Sends data to a webflow collection.
	See: https://www.briantsdawson.com/blog/webflow-api-how-to-get-site-collection-and-item-ids-for-zapier-and-parabola-use

	Example Input: webflow_insert(collection_id="623107ba68bd7ba11ca033c7")
	"""

	webflow_api = Webflow(token=token)
	print("@1", webflow_api.items(collection_id=collection_id))
