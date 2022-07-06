""" These functions have to do with databases and file systems. """

import io
import os
import sys
import json
import time
import types
import string
import logging
import datetime
import requests
import itertools
import traceback
import contextlib
import configparser

import bs4
import numpy
import pandas
import pysftp
import dropbox
import psycopg2
import psycopg2.extras
import webflowpy.Webflow

import common
import logger
import testing
import storage_util

# pip install
	# lxml
	# pandas
	# pysftp
	# dropbox
	# psycopg2
	# webflowpy
	# beautifulsoup4
	# azure-storage-blob

parser = None
def config(key=None, section="postgres_prod", *, filename="../settings.ini", useCached=False, defaultValue=None, **kwargs):
	""" Reads value(s) from the config file.

	section (str) - Which ini section to read from
	key (str) - Which key to return from the ini file
		- If None: Returns a dictionary of all items in *section*
	filename (str) - Where the ini file is located
	useCached (bool) - If the config file loaded last time should be reused
	defaultValue (any) - What shoudl be used if *key* is not in *section*

	Example Input: config()
	Example Input: config(section="ipsum")
	Example Input: config(filename="lorem.ini")
	Example Input: config(key="token", section="dropbox")
	"""
	global parser

	if (not useCached or not parser):
		parser = configparser.ConfigParser()
		parser.read(filename)
 
	if not parser.has_section(section):
		raise ValueError(f"Section '{section}' not found in the '{filename}' file")

	if (key):
		return parser[section].get(key, defaultValue)

	return {key: value for (key, value) in parser.items(section)}

class ManageAmerica():
	@classmethod
	def _getToken(cls, login_user=None, login_password=None, *, configKwargs=None, **kwargs):
		""" Function to return MA access token.
		See: https://docs.python-requests.org/en/v1.0.0/api/#main-interface

		login_user (str) - The username to use for logging in
		login_password (str) - The password to use for logging in

		Example Input: _getToken()
		Example Input: _getToken("Lorem", "Ipsum")
		"""

		login_user = login_user or config("user", "ma_vineyards", **(configKwargs or {}))
		login_password = login_password or config("password", "ma_vineyards", **(configKwargs or {}))
		options = {"login": login_user, "password": login_password}

		logging.info("Getting MA token...")
		response = requests.request("POST", "https://api.manageamerica.com/api/account/signin",
			headers = {"Content-Type": "application/json"},
			data = json.dumps(options),
		)
		response_json = response.json()

		if ("exceptionMessage" in response_json): 
			raise ValueError(response_json["exceptionMessage"], options, response_json)

		token = response_json["token"]
		logging.debug(f"ma_token: '{token}'")
		return token

	@classmethod
	def select(cls, url, *, token=None, alias=None, remove=None, modifyData=None, filterData=None, customReport=False, **kwargs):
		""" Returns data from Manage America.

		url (str or tuple) - The URL to pull the data from
			- If tuple: Will get the data from each URL in the list
		alias (dict of str) - Key names to replace in the data source if they exist
		remove (tuple of str) - Key names to remove from the data source if they exist
		modifyData (function) - A function which modifies the data before it is returned
		filterData (function) - A function which filters the data before it is modified or returned
		customReport (bool) - If the result should be parsed as a manage america 'adds report'

		Example Input: select("https://n6.manageamerica.com/api/property/?companyId=233")
		Example Input: select("https://n6.manageamerica.com/api/property/?companyId=233", modifyData=lambda data: [*data, {"lorem": "ipsum"}])
		Example Input: select("https://n6.manageamerica.com/api/property/?companyId=233", filterData=lambda item: row.get("type") == "Detail")
		Example Input: select(["https://www.lorem.com", "https://www.ipsum.com"])
		Example Input: select("https://n6.manageamerica.com/api/addsReport/v1/runReport?Company_Id=233&Report_Id=4553", customReport=True)
		Example Input: select("https://n6.manageamerica.com/api/property/?companyId=233", alias={"Old Name 1": "new_name_1", "Old Name 2": "new_name_2"})
		"""

		token = token or cls._getToken(**kwargs)

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

		# logging.debug(logger.debugging and f"ma_response: '{data}'")
		if (not len(data)):
			return

		if (customReport):
			logging.info("Parsing custom report data...")
			columns = tuple(item["name"] for item in data[0]["columns"])
			data = tuple({key: value for (key, value) in itertools.zip_longest(columns, item["data"])} for item in data[0]["rows"] if (item["type"] != "Total"))
			if (not len(data)):
				return

		if (alias):
			logging.info("Renaming columns...")
			data = tuple({alias.get(key, key): value for (key, value) in catalogue.items()} for catalogue in data)

		if (remove):
			logging.info("Removing columns...")
			data = tuple({key: value for (key, value) in catalogue.items() if (key not in remove)} for catalogue in data)

		if (filterData):
			logging.info("Filtering rows...")
			for myFunction in common.ensure_container(filterData):
				data = data.filter(myFunction)

		if (modifyData):
			logging.info("Modifying data...")
			for myFunction in common.ensure_container(modifyData):
				data = myFunction(data)

		return data

class Ftp:
	@classmethod
	def getConnection(cls, login_user=None, login_password=None, login_host=None, *, connection=None,configKwargs=None, **kwargs):
		""" Retuns an object to use for connecting to an FTP server.

		Example Input: getConnection()
		"""

		if (connection is not None):
			return connection

		login_host = login_host or config("host", "ftp_ma_vineyards", **(configKwargs or {}))
		login_user = login_user or config("user", "ftp_ma_vineyards", **(configKwargs or {}))
		login_password = login_password or config("password", "ftp_ma_vineyards", **(configKwargs or {}))

		options = pysftp.CnOpts()
		options.hostkeys = None

		with pysftp.Connection(host=login_host, username=login_user, password=login_password, cnopts=options) as _connection:
			yield _connection

	@classmethod
	def select(cls, container, *, a , **kwargs):
		""" Gets data from an ftp server.

		Example Input: select("Feeds")
		"""
		
		connection = cls.getConnection(**kwargs)

		return connection.get(filename, filepath)

class Formsite():
	@classmethod
	def getConnection(cls, *args, connection=None, **kwargs):
		""" Retuns an object to use for connecting to Formsite.

		Example Input: getConnection()
		"""

		if (connection is not None):
			return connection

		return cls.FormsiteConnection(*args, **kwargs)

	@classmethod
	def select(cls, form_code, **kwargs):
		""" Yields the unformatted data for a form from Formsite

		form_code (str or tuple) - The form code of the form to return data for 
			- If tuple: Will get the data from each form code in the list

		Example Input: select_unformatted("wjsrave6n6")
		Example Input: select_unformatted("rf1gmwaueh", view=101)
		Example Input: select_unformatted("wjsrave6n6", output_type="raw")
		"""

		connection = cls.getConnection(**kwargs)

		data = []
		for _form_code in common.ensure_container(form_code):
			data.append(connection.getForm(_form_code, **kwargs))

		return data

	class FormsiteConnection():
		def __init__(self, server=None, user=None, bearer=None, cookie=None, configKwargs=None, **kwargs):
			""" A helper object for working with formsite.

			server (str) - Which formsite server to use
			user (str) - Which formsite user account to use
			bearer (str) - The bearer token to use
			cookie (str) - The cookies token to send

			Example Input: FormsiteConnection()
			"""

			configKwargs = configKwargs or {}
			self.user = user or config("user", "formsite", **configKwargs)
			self.server = server or config("server", "formsite", **configKwargs)
			self.bearer = bearer or config("bearer", "formsite", **configKwargs)
			self.cookie = cookie or config("cookie", "formsite", **configKwargs)

			self.url_base = f"https://{self.server}.formsite.com/api/v2/{self.user}"

			self.headers = {
				"Authorization": self.bearer,
				"Cookie": self.cookie,
			}

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc_val, exc_tb):
			pass

		def getForm(self, form_code, *, output_type="pipe_values_formatted", **kwargs):
			""" Returns a form object.

			form_code (str) - The form code of the form to return data for or it's base form url

			Example Input: getForm("wjsrave6n6")
			Example Input: getForm(url_base_form)
			Example Input: getForm("wjsrave6n6", output_type="raw")
			"""

			form = self.FormsiteForm(self, form_code, **kwargs)

			match output_type:
				case "raw":
					return form

				case "pipe_labels":
					return form.pipe_labels

				case "pipe_values":
					return form.pipe_values

				case "pipe_values_formatted":
					return form.pipe_values_formatted

				case _:
					raise NotImplementedError(f"Unknown *output_type* '{output_type}'")

		class FormsiteForm():
			def __init__(self, parent, form_code, *, view=None, limit=None, page=None, sort_direction=None,
				after_date=None, after_date__format=None, after_date__table=None, after_date__column=None, 
				before_date=None, before_date__format=None, before_date__table=None, before_date__column=None, 
				after_id=None, before_id=None, sort_id=None, search=None,
			):
				""" A lazy form result from Formsite.
				Things are calculated as they are requested.
				See: https://support.formsite.com/hc/en-us/articles/360000288594-API

				form_code (str) - The form code of the form to return data for or it's base form url
				view (str) - Which result view to return data for
				page (int) - If None, will return all pages
				sort_direction (str or bool) - If True: "asc"; If False: "desc"

				after_date (str or datetime or bool) - If True: Will get the latest date from *after_date__table*
				after_date__format (str) - What format to use for parsing *after_date*
				after_date__table (str) - Which table to get the date from
				after_date__column (str) - Which column from *after_date__table* to get the max date from
				before_date (str or datetime or bool) - If True: Will get the earliest date from *after_date__table*

				Example Input: FormsiteForm(self, "wjsrave6n6")
				Example Input: FormsiteForm(self, "rf1gmwaueh", view=101)
				Example Input: FormsiteForm(self, "wjsrave6n6", after_date="2022-05-11")
				Example Input: FormsiteForm(self, "wjsrave6n6", sort_direction=True)
				"""

				self.parent = parent
				self.form_code = form_code

				self.view = view
				self.limit = limit
				self.page = page
				self.sort_direction = sort_direction
				self.after_date = after_date
				self.after_date__format = after_date__format
				self.after_date__table = after_date__table
				self.after_date__column = after_date__column
				self.before_date = before_date
				self.before_date__format = before_date__format
				self.before_date__table = before_date__table
				self.before_date__column = before_date__column
				self.after_id = after_id
				self.before_id = before_id
				self.sort_id = sort_id
				self.search = search

				self.url_base = f"{self.parent.url_base}/forms/{self.form_code}"

				self._pipe_ids = None
				self._pipe_labels = None
				self._pipe_values = None
				self._pipe_values_formatted = None

			@property
			def pipe_labels(self):
				if (self._pipe_labels is not None):
					return self._pipe_labels

				url = f"{self.url_base}/items"
				logging.info(f"Getting pipe labels from url: '{url}'")
				response = requests.request("GET", url, headers=self.parent.headers)
				
				self._pipe_labels = {item["id"]: item["label"] for item in response.json()["items"]}
				return self._pipe_labels

			@property
			def pipe_values(self):
				if (self._pipe_values is not None):
					return self._pipe_values

				def getResultPage(page, last_page, *, nested=0, nested_max=3):
					nonlocal url_noPage

					url = f"{url_noPage}&page={page}"
					logging.info(f"Getting pipe values for url {page} of {last_page}: '{url}'")
					response = requests.request("GET", url, headers=self.parent.headers)
					catalogue = response.json()

					error = catalogue.get("error")
					if (error):
						match (error["status"]):
							case 429: # Too many requests. Wait at least one minute and try again.
								if (nested >= nested_max):
									raise ValueError(error["message"], catalogue)

								logging.info(f"Waiting 90 seconds then trying again ({nested + 1} of {nested_max} tries)")
								time.sleep(90)
								for item in getResultPage(page, last_page, nested=nested + 1):
									yield item
								return

							case _:
								raise ValueError(error["message"], catalogue)

					if ("results" not in catalogue):
						raise ValueError("Unknown JSON response", catalogue)

					for item in catalogue["results"]:
						# Account for duplicate entries (that happen when people submit during the query?)
						index = item["id"]
						if (index in self._pipe_ids):
							logging.info(f"Encountered a duplicate entry; {item}")
							continue
						self._pipe_ids[index] = True

						yield item

				def yieldResult():
					if (self.page != None):
						for item in getResultPage(self.page, "?"):
							yield item
						return

					url = f"{self.url_base}/results"
					logging.info(f"Getting number of pages: '{url}'")
					response = requests.request("GET", url, headers=self.parent.headers)
					last_page = response.headers["Pagination-Page-Last"]
					for page in range(1, int(last_page) + 1):
						for item in getResultPage(page, last_page):
							yield item

				######################

				url_noPage = f"{self.url_base}/results?"
				if (self.after_date is not None):
					pass

				if (self.before_date is not None):
					pass

				if (self.view is not None):
					url_noPage += f"&results_view={self.view}"

				if (self.limit is not None):
					url_noPage += f"&limit={self.limit}"

				if (self.after_id is not None):
					url_noPage += f"&after_id={self.after_id}"

				if (self.before_id is not None):
					url_noPage += f"&before_id={self.before_id}"

				if (self.sort_id is not None):
					url_noPage += f"&sort_id={self.sort_id}"

				if (self.sort_direction is not None):
					url_noPage += f"&sort_direction={self.sort_direction}"

				if (self.search is not None):
					pass

				self._pipe_ids = {}
				self._pipe_values = tuple(yieldResult())
				return self._pipe_values

			@property
			def pipe_values_formatted(self):
				if (self._pipe_values_formatted is not None):
					return self._pipe_values_formatted

				def formatValue(row):
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

					return catalogue

				##########################

				# So the logging makes sense- ensure these are calculated first
				pipe_labels = self.pipe_labels
				_pipe_values = self.pipe_values

				logging.info("Combineing pipe labels and values")
				self._pipe_values_formatted = tuple(formatValue(row) for row in _pipe_values if (("date_start" in row) and ("date_finish" in row)))
				return self._pipe_values_formatted

class ShowMojo:
	@classmethod
	def getConnection(cls, *args, connection=None, **kwargs):
		""" Retuns an object to use for connecting to ShowMojo.

		Example Input: getConnection()
		"""

		if (connection is not None):
			return connection

		return cls.ShowMojoConnection(*args, **kwargs)

	@classmethod
	def select(cls, report_id=None, include_standard=False, **kwargs):
		""" Yields the unformatted data for a report from ShowMojo

		report_id (str or tuple) - The report id of the report to return data for 
			- If tuple: Will get the data from each report id in the list

		Example Input: select_unformatted("1569")
		Example Input: select_unformatted(("1569", "1570"))
		Example Input: select_unformatted("1569", include_standard=True)
		Example Input: select_unformatted("1569", date_start=datetime.datetime.now())
		"""

		connection = cls.getConnection(**kwargs)

		data = []

		if (report_id is not None):
			data.append(*connection.yieldReport(report_id, **kwargs))

		if (include_standard):
			data.append(*connection.yieldStandard(**kwargs))

		return tuple(data)

	class ShowMojoConnection():
		def __init__(self, login_user=None, login_password=None, *, configKwargs=None, **kwargs):
			""" A helper object for working with ShowMojo.

			Example Input: ShowMojoConnection()
			"""

			configKwargs = configKwargs or {}
			login_user = login_user or config("user", "showmojo", **(configKwargs or {}))
			login_password = login_password or config("password", "showmojo", **(configKwargs or {}))

			self._soup_root = None
			self._soup_list = None
			self.authorization = (login_user, login_password)



			test = self.soup_list
			print(test[0])
			# print(test[0].prettify())
			dssdfsdf

		def __enter__(self):
			return self

		def __exit__(self, exc_type, exc_val, exc_tb):
			pass

		@property
		def soup_root(self):
			if (self._soup_root):
				return self._soup_root

			response = requests.request("GET", "https://showmojo.com/syndication/trulia/46415e1005.xml")
			response.raise_for_status()

			self._soup_root = bs4.BeautifulSoup(response.content, "xml")
			return self._soup_root

		@property
		def soup_list(self):
			if (self._soup_list):
				return self._soup_list

			self._soup_list = tuple({
				"listing_id": element.details.find("provider-listingid").string,
				"listing_type": element.find("listing-type").string,
				"listing_title": element.details.find("listing-title").string,
				"status": element.status.string,
				"date_availasble": element.status.string,
				"address_street": element.location.find("street-address").string,
				"address_city": element.location.find("city-name").string,
				"address_zipcode": element.location.zipcode.string,
				"url_home": element.find("landing-page").find("lp-url").string,
				"url_schedule": element.site.find("site-url").string,
				"url_video": [item.find("video-url").string for item in element.videos.children],
				"url_photo": [item.find("picture-url").string for item in element.pictures.children],

				"agent_id": element.agent.find("agent-id"),
				"agent_name": element.agent.find("agent-name"),
				"agent_phone": element.agent.find("agent-phone"),
				"agent_email": element.agent.find("agent-email"),

			} for element in self.soup_root.find_all("property"))
			return self._soup_list

		def yieldStandard(self, **kwargs):
			""" Returns all the standard reports.

			Example Input: yieldStandard()
			Example Input: yieldStandard(date_start=datetime.datetime.now())
			"""

			for report_id in ("detailed_prospect_data", "high_level_metrics", "detailed_listing_data", "listing_and_showing_metrics", "prospect_showing_data", "listing_performance"):
				yield yieldReport(report_id,
					**kwargs)

		def yieldReport(self, report_id, *, date_start=None, date_finish=None):
			""" Returns a report object.
			See: https://showmojo.com/help#/29069-api/223545-report-export-api
			See: https://docs.python-requests.org/en/v1.0.0/api/#main-interface

			report_id (str) - The form code of the form to return data for or it's base form url

			Example Input: getForm("1569")
			Example Input: getForm("1569", date_start=datetime.datetime.now())
			Example Input: getForm("detailed_prospect_data")
			"""

			date_start = date_start or "2019-12-19"
			date_finish = date_finish or datetime.datetime.now()

			if (isinstance(date_start, datetime.date)):
				date_start = date_start.strftime("%Y-%m-%d")

			if (isinstance(date_finish, datetime.date)):
				date_finish = date_finish.strftime("%Y-%m-%d")

			for _report_id in common.ensure_container(report_id): 
				if (f"{_report_id}".isnumeric()):
					url = f"https://showmojo.com/api/v3/reports/custom?report_id={_report_id}&start_date={date_start}&end_date={date_finish}"
				elif (_report_id in ("detailed_prospect_data", "high_level_metrics", "detailed_listing_data", "listing_and_showing_metrics", "prospect_showing_data", "listing_performance")):
					url = f"https://showmojo.com/api/v3/reports/{_report_id}?start_date={date_start}&end_date={date_finish}"
				else:
					raise KeyError(f"Unknown *report_id*: '{_report_id}'")

				logging.info(f"Getting report '{_report_id}': {url}")
				response = requests.request("POST", url, auth=self.authorization)
				response.raise_for_status()

				catalogue = response.json()["response"]
				match (catalogue["status"]):
					case "success":
						print("@1", catalogue["status"])
						yield pandas.DataFrame(catalogue["data"])

					case _:
						raise KeyError(f"Unknown status: '{catalogue['status']}'", catalogue)

		def getStandard(self, *args, **kwargs):
			return tuple(self.yieldAllStandard(*args, **kwargs))

		def getReport(self, *args, **kwargs):
			return tuple(self.yieldReport(*args, **kwargs))

class Postgres():
	@classmethod
	def _renameKeys(cls, iterable):
		""" A recursive function to convert keys to lowercase.
		Otherwise, json keys won't match postgresql column names.

		iterable (list or dict) - What to iterate over

		Example Input: _renameKeys([{"Lorem": 1, "IPSUM": 2, "dolor": 3}])
		"""

		if isinstance(iterable, dict):
			new_object = {}
			for (key, value) in iterable.items():
				new_object[key.lower()] = value if not isinstance(iterable, (dict, list, tuple)) else cls._renameKeys(value)
			return new_object

		if isinstance(iterable, (list, tuple)):
			return [cls._renameKeys(item) for item in iterable]

		return iterable

	@classmethod
	def insert(cls, data, table, *, method="upsert", insert_method="single", drop_where=None, upsert_constraint=None,
		chunk_size=900, backup=None, lowerNames=True, typeCatalogue=None, configKwargs=None, preInsert=None, postInsert=None, **kwargs):
		""" Adds data to a postgres database.
		See: https://www.psycopg.org/docs/usage.html#query-parameters
		Use: http://www.postgresqltutorial.com/postgresql-python/connect/

		data (tuple of dict) - What to send to the database
		table (str) - Which table to send the data to
		method (str) - How to handle sending the data
			- insert: Inserts a row if it does not yet exist
			- insert_ignore: Inserts a row if it does not yet exist; skips conflicts
			- update: Update a row if it exists
			- upsert: Update existing data in the db, otherwise insert a new row
			- drop: Drop all rows in the table and insert new rows
			- truncate: Cleanly drop all rows in the table and insert new rows  (See: https://stackoverflow.com/questions/11419536/postgresql-truncation-speed/11423886#11423886)
			- If Function: Will use a string returned by it as what to do for an individual row
		insert_method (str) - How to handle inserting things into the database
			- json: Pass in a JSON string with all the data (Does not allow non-serializable inputs such as datetime objects)
			- separate: Do an individual insert for each row (Much slower)
			- single: Do a single insert statement for every *chunk_size* rows
		backup (dict or str) - How to backup what is inserted
			- kind (required): Used as the string
				- dropbox: Send to dropbox
				- blob: Send to blob storage
			- other keys: kwargs to send
			- If str: Assumed to be *backup.kind*
		lowerNames (bool) - If object keys should be lower-cased
		typeCatalogue (dict) - What type specific columns need to be; where the key is the column name and the value is one of the following strings:
			- json: The data should be a JSON string (will fail if the column's value contains non-serializable values)
		drop_where (str) - What to use for selecting what is dropped

		Example Input: insert([{"lorem": "ipsum"}], "property")
		Example Input: insert([{"Lorem": "ipsum"}], "property", lowerNames=True)
		Example Input: insert([{"lorem": "ipsum"}], "property", backup="dropbox")
		Example Input: insert([{"lorem": "ipsum"}], "property", backup={"kind": "dropbox", "folder": "rps"})
		Example Input: insert([{"lorem": "ipsum"}], "property", backup={"kind": "dropbox", "filename_subname": "treehouse"})
		Example Input: insert([{"lorem": datetime.datetime.now()}], "property", insert_method="separate")
		Example Input: insert(frame, "property")
		Example Input: insert([{"lorem": "ipsum"}], "property", method="drop")
		Example Input: insert([{"lorem": {"ipsum": 1}}], "property", typeCatalogue={"lorem": "json"})
		Example Input: insert([[{"lorem": "ipsum"}], [{"dolor": "sit"}]], "property")
		Example Input: insert([frame_1, frame_2], "property")
		Example Input: insert(frame, "property", method=lambda row, context: "insert" if row["should_insert"] else "skip", insert_method="single")
		"""

		def yield_sqlInsert(_data, _method):
			if (_method == "update"):
				raise NotImplementedError("Update or Ignore SQL generation")

			no_update = (_method != "upsert")
			no_ignore = (_method != "insert_ignore")
			match insert_method:
				case "json":
					yield [
						f"INSERT INTO {table} SELECT p.* FROM jsonb_populate_recordset(NULL::{table}, %s) as p" +
							("" if no_update else f" ON CONFLICT ON CONSTRAINT {upsert_constraint} DO UPDATE SET {', '.join(f'{key} = EXCLUDED.{key}' for key in _data[0].keys())}") +
							("" if no_ignore else f" ON CONFLICT DO NOTHING"),
						(json.dumps(_data),)
					]

				case "separate":
					for row in _data:
						keyList = tuple(row.keys())
						yield [
							f"INSERT INTO {table} ({', '.join(keyList)}) VALUES ({', '.join(f'%({key})s' for key in keyList)})" +
								("" if no_update else f" ON CONFLICT ON CONSTRAINT {upsert_constraint} DO UPDATE SET {', '.join(f'{key} = EXCLUDED.{key}' for key in keyList)}") +
								("" if no_ignore else f" ON CONFLICT DO NOTHING"),
							row
						]

				case "single":
					keyList = tuple(_data[0].keys())

					for chunk in (_data[j:j+chunk_size] for j in range(0, len(_data), chunk_size)):
						valueList = []
						valueCatalogue = {}
						for (j, row) in enumerate(chunk):
							valueList.append(f"({', '.join(f'%({key}_{j})s' for key in keyList)})")
							valueCatalogue.update({f"{key}_{j}": value for (key, value) in row.items()})

						yield [
							f"INSERT INTO {table} ({', '.join(keyList)}) VALUES {', '.join(valueList)}" +
								("" if no_update else f" ON CONFLICT ON CONSTRAINT {upsert_constraint} DO UPDATE SET {', '.join(f'{key} = EXCLUDED.{key}' for key in keyList)}") +
								("" if no_ignore else f" ON CONFLICT DO NOTHING"),
							valueCatalogue
						]

				case _:
					raise KeyError(f"Unknown *insert_method* '{insert_method}'")

		def doInsert(_data, i, connection):
			if (not len(_data)):
				logging.info(f"No data to insert into '{table}' for item number '{i}'")
				return ()

			if (isinstance(_data, pandas.DataFrame)):
				# See: https://pandas.pydata.org/pandas-docs/version/0.17.0/generated/pandas.DataFrame.to_dict.html#pandas.DataFrame.to_dict
				_data = _data.replace({numpy.nan: None}).to_dict("records")

			elif (isinstance(_data, dict)):
				raise ValueError("Data was not given in the correct format for this function; Make sure it is a container")
				# _data = [_data]

			if (lowerNames):
				_data = cls._renameKeys(_data)

			if (typeCatalogue):
				for (key, value) in typeCatalogue.items():
					for row in _data:
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
			if ((not i) and (method in ("drop", "truncate"))):
				# Only do this for the first item
				match method:
					case "drop":
						queries.append([f"DELETE FROM {table}{'' if not drop_where else f' WHERE ({drop_where})'}", ()])

					case "truncate":
						queries.append([f"TRUNCATE TABLE {table} RESTART IDENTITY", ()])

					case _:
						raise KeyError(f"Unknown *method* '{method}'")

			if (isinstance(method, str)):
				queries.extend(yield_sqlInsert(_data, method))

				# print("DEBUGGING: NO QUERY SENT\n")
				cls.runSQL(queries, **{"connection":connection, **kwargs})
				return _data

			data_drop = []
			data_insert = []
			data_insert_ignore = []
			data_update = []
			data_upsert = []
			skip_count = 0

			total = len(_data)
			for (j, row) in enumerate(_data):
				key = method(row, {
					"batch_count": i,
					"row_count": j,
				})
				match key:
					case "drop":
						data_drop.append(row)

					case "insert":
						data_insert.append(row)

					case "insert_ignore":
						data_insert_ignore.append(row)

					case "update":
						data_update.append(row)

					case "upsert":
						data_upsert.append(row)

					case "skip":
						skip_count += 1
						continue

					case None:
						skip_count += 1
						continue

					case _:
						raise KeyError(f"Unknown *method() answer* '{key}'")

			if (data_drop):
				logging.info(f"Will drop {len(data_drop)} of {total} rows")
				raise NotImplementedError("Drop row without knowing the primary key")

			if (data_insert):
				logging.info(f"Will insert {len(data_insert)} of {total} rows")
				queries.extend(yield_sqlInsert(data_insert, "insert"))

			if (data_insert_ignore):
				logging.info(f"Will insert or ignore {len(data_insert_ignore)} of {total} rows")
				queries.extend(yield_sqlInsert(data_insert_ignore, "data_insert_ignore"))

			if (data_update):
				logging.info(f"Will update {len(data_update)} of {total} rows")
				queries.extend(yield_sqlInsert(data_update, "update"))

			if (data_upsert):
				logging.info(f"Will upsert {len(data_upsert)} of {total} rows")
				queries.extend(yield_sqlInsert(data_upsert, "upsert"))

			if (skip_count):
				logging.info(f"Will skip {skip_count} of {total} rows")

			# print("DEBUGGING: NO QUERY SENT\n")
			cls.runSQL(queries, **{"connection": connection, **kwargs})

			return (*data_drop, *data_insert, *data_update, *data_upsert)

		def formatData(_data):
			container = common.ensure_container(_data, checkIteratorFunction=lambda item: not isinstance(item, pandas.DataFrame))
			if (not container):
				return ()

			if (isinstance(container[0], dict)):
				return (container,)

			return container

		#################################

		upsert_constraint = upsert_constraint or f"{table}_pkey"

		last_i = -1
		data_used = []
		with cls.getConnection(**kwargs) as connection:
			if (preInsert):
				for (i, _data) in enumerate(formatData(preInsert()), start=last_i + 1):
					data_used.extend(doInsert(_data, i, connection))
					last_i = i

			for (i, _data) in enumerate(formatData(data), start=last_i + 1):
				data_used.extend(doInsert(_data, i, connection))
				last_i = i

			if (postInsert):
				for (i, _data) in enumerate(formatData(postInsert()), start=last_i + 1):
					data_used.extend(doInsert(_data, i, connection))
					last_i = i

		if (not len(data_used)):
			logging.info(f"No data was inserted into '{table}' after {last_i} runs")
			return False

		if (backup):
			backup = common.ensure_dict(backup, "kind")
			kind = backup.get("kind", None)

			if (not backup.get("filename", None)):
				# See: https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
				subname = backup.get("filename_subname", None)

				backup["filename"] = f"{f'{subname}_' if subname else ''}{datetime.datetime.now():%Y-%m-%d_%H-%M-%S}.csv"

			match kind:
				case "dropbox":
					return Dropbox.insert(data_used, folder=table, input_type="csv", **backup)

				case "blob":
					return BlobStorage.insert(data_used, folder=table, input_type="csv", **backup)

				case None:
					raise ValueError("Required key missing: *backup.kind*")

				case _:
					raise KeyError(f"Unknown *backup.kind* '{kind}'")

		return True

	# @classmethod
	# def update(cls, frame, table, column, *, pk=None, method="upsert", upsert_constraint=None, connection=None):
	# 	""" Does a batch update statement.
	# 	See: https://stackoverflow.com/questions/7019831/bulk-batch-update-upsert-in-postgresql/20224370#20224370

	# 	Example Input: update(frame, table="sos", column="code")
	# 	Example Input: update(frame, table="sos", column={"sos_code": "code"})
	# 	"""

	# 	pk = pk or f"{table}_id"

	# 	catalogue_column = common.ensure_dict(column, useAsKey=None)
	# 	columnList = tuple(catalogue_column.keys())

	# 	sql_set = ", ".join(f"{key} = b.{key}" for key in columnList)
	# 	sql_from = ", ".join(f"unnest(array[{', '.join(v)}]) as {key}")

	# 	sql_full = f"UPDATE {table} as a SET {sql_set} FROM (SELECT {sql_from}) as b WHERE (a.{pk} = b.{pk})",
	# 	print("@1", [sql_full])

	# 	Postgres.raw(query_sql=sql_full, as_dict=False, connection=connection)

	@classmethod
	def yield_raw(cls, query_sql, query_args=None, **kwargs):
		""" Yields the answer to a raw sql statement to ther database.
		See: https://www.psycopg.org/docs/connection.html

		table (str) - Which table to get data from
		
		Example Input: yield_raw("SELECT * FROM property")
		Example Input: yield_raw("SELECT * FROM property WHERE id = %s", (1,))
		Example Input: yield_raw("SELECT id FROM property", as_dict=False)
		Example Input: yield_raw((("SELECT * FROM property", ()), ("SELECT * FROM property WHERE id = %s", (1,))))
		"""

		for item in (cls.yield_runSQL(((query_sql, query_args),), **kwargs) if isinstance(query_sql, str) else cls.yield_runSQL(query_sql, **kwargs)):
			yield item

	@classmethod
	def raw(cls, *args, **kwargs):
		return tuple(cls.yield_raw(*args, **kwargs))

	@classmethod
	@contextlib.contextmanager
	def getConnection(cls, *, _self=None, connection=None, configKwargs=None, **kwargs):
		if (connection is not None):
			yield connection
			return

		logging.info("Opening postgres connection...")
		_connection = psycopg2.connect(**config(**(configKwargs or {})))
		if (_self is None):
			with _connection:
				yield _connection
		else:
			with _connection:
				_self._connection = _connection
				yield _connection
				_self._connection = None
		logging.info("Closing postgres connection...")
		_connection.close()

	@classmethod
	def yield_runSQL(cls, queries, *, as_dict=common.NULL_private, **kwargs):
		# See: https://www.psycopg.org/docs/connection.html
		
		with cls.getConnection(**kwargs) as connection:
			with (connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) if (as_dict or (as_dict is common.NULL_private)) else connection.cursor()) as cursor:
				logging.info(f"Sending {len(queries)} queries...")
				for (query_sql, query_args) in queries:
					logging.debug(logger.debugging and f"query_sql: '{query_sql}'")
					logging.debug(logger.debugging and f"query_args: '{query_args}'")
					try:
						# TODO logging.info how many rows were added, modified, deleted, etc
						cursor.execute(query_sql, query_args or ())
						
						if (as_dict is common.NULL_private):
							_as_dict = True if (query_sql[:6].lower().startswith("select")) else None
						else:
							_as_dict = as_dict

						if (_as_dict is not None):
							count = 0

							if (_as_dict):
								for row in cursor:
									count += 1
									yield dict(row)
							else:
								for row in cursor:
									count += 1
									yield row

							logging.info(f"Recieved '{count}' results")
					except Exception as error:
						if (not logger.debugging):
							logging.info(f"query_sql: '{query_sql}'")
							logging.info(f"query_args: '{query_args}'")
						raise error
						# traceback.print_exception(type(error), error, error.__traceback__)

	@classmethod
	def runSQL(cls, *args, **kwargs):
		return tuple(cls.yield_runSQL(*args, **kwargs))

	@classmethod
	def apply_stringIndex(cls, frame, columnName, *, connection=None):
		""" Replaces a string value with an id in the table string_index.
		Use this to make string columns a primary key in a table.
		
		Example Input: apply_stringIndex(frame, "lorem")
		Example Input: apply_stringIndex(frame, ("lorem", "ipsum"))
		"""

		# Replace pk string columns with foreign key IDs
		columnList = common.ensure_container(columnName)

		Postgres.insert(
			data=GeneralOutput.getUnique(frame, columnList, as_list=False),
			table="string_index",
			method="insert_ignore",
			connection=connection,
		)

		catalogue_index = {value: key for (key, value) in Postgres.raw(query_sql="SELECT id, value FROM string_index", as_dict=False, connection=connection)}

		for key in columnList:
			frame[key] = frame[key].replace(catalogue_index).astype("Int64")

		return columnList

	@classmethod
	def apply_foreign(cls, frame, table, column, *, fk=None, move=False, method="upsert", upsert_constraint=None, connection=None):
		""" Takes *column* from *frame* and puts it into *table*, replacing it with a foreign key as *fk*.

		move (bool) - Determines if the inserted foreign key gets insrted back into the data or not

		Example Input: apply_foreign(frame, table="sos", column="code")
		Example Input: apply_foreign(frame, table="sos", column={"sos_code": "code"})
		Example Input: apply_foreign(frame, table="sos", column="code", method="insert")
		Example Input: apply_foreign(frame, table="sos", column={"sos_code": "code"}, move=True)
		"""

		fkList = [f"{table}_id"] if (not fk) else list(common.ensure_container(fk))
		fk_missing = all(key not in frame.columns for key in fkList)

		if (move and fk_missing):
			raise ValueError(f"*frame* is missing at least 1 of these columns: {fkList}", frame.columns)

		if ((not upsert_constraint) and fk_missing):
			upsert_constraint = f"{table}_un"

		catalogue_column = common.ensure_dict(column, useAsKey=None)
		columnList = list(catalogue_column.keys())

		frame_foreign = frame[columnList if fk_missing else [*fkList, *columnList]].drop_duplicates()
		frame_foreign.rename(catalogue_column, axis=1, inplace=True)

		Postgres.insert(
			data=frame_foreign,
			table=table,
			method=method,
			upsert_constraint=upsert_constraint,
			connection=connection,
		)

		if (fk_missing and (not move)):
			catalogue_index = {tuple(args): key for (key, *args) in Postgres.raw(
				query_sql=f"SELECT {', '.join(fkList)}, {', '.join(catalogue_column.values())} FROM {table}",
				as_dict=False, connection=connection
			)}

			for key in fkList:
				frame[key] = None
				frame[key] = frame[key].astype("Int64")
				frame[key] = [catalogue_index[index] for index in zip(*(frame[_key] for _key in columnList))]

		frame.drop(columnList, axis=1, inplace=True)

		return fkList

class GeneralOutput():
	@classmethod
	def yield_fileOutput(cls, data, folder=None, filename=None, *, input_type="csv", can_yield_pandas=False, walk_allow=("csv", "xlsx", "xls"), **kwargs):
		""" A generator that yields file handles and their intended destinations based on the input criteria.
		See: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
		See: https://stackoverflow.com/questions/13120127/how-can-i-use-io-stringio-with-the-csv-module/45608450#45608450

		data (any) - What to send to the implemented storage
		folder (str) - What folder path of the container to store the file(s) in
			- If None: Will put the file in the root directory
		filename (str) - What file name to use for the file
			- If None: Will try coming up with a file name
		input_type (str) - How to handle parsing the input data
			- raw: Just send it as recieved
			- json: If it is not a string
			- csv: Make it a csv file
				- Can be a container
			- file: Will use *data* as a filepath to look it up from disk; if there is no file extension it will walk that directory and send all files contained there
				- Can be a container
		walk_allow (tuple) - What file extensions to allow from walking the directory for *input_type*
		can_yield_pandas (bool) - If a pandas frame can be yielded instead of a file buffer

		Example Input: yield_fileOutput([{Lorem: "ipsum"}])
		Example Input: yield_fileOutput([{Lorem: "ipsum"}], folder="rps")
		Example Input: yield_fileOutput({Lorem: "ipsum"}, input_type="json")
		Example Input: yield_fileOutput("C:/lorem/ipsum", input_type="file")
		Example Input: yield_fileOutput("C:/lorem/ipsum", input_type="file", walk_allow=("csv"))
		Example Input: yield_fileOutput(open("lorem.txt", "r"), filename="lorem.txt", input_type="raw")
		"""

		match input_type:
			case "mixed":
				container = common.ensure_container(data, checkIteratorFunction=lambda item: not isinstance(item, pandas.DataFrame));
				if (not len(container)):
					logging.info(f"No data given")
					return

				if (isinstance(container[0], dict)):
					for item in cls.yield_fileOutput((data,), folder, filename, input_type="csv", can_yield_pandas=can_yield_pandas, walk_allow=walk_allow, **kwargs):
						yield item
					return

				for item in container:
					if (isinstance(item, pandas.DataFrame)):
						_input_type = "csv"
					elif (isinstance(item, str)):
						_input_type = "file"
					else:
						raise KeyError(f"Unknown mixed type '{item}'")

					for _item in cls.yield_fileOutput(item, folder, filename, input_type=_input_type, can_yield_pandas=can_yield_pandas, walk_allow=walk_allow, **kwargs):
						yield _item

				return

			case "raw":
				destination = os.path.join(folder or "", filename or "")
				logging.info(f"Send raw data to '{destination}'")
				yield (data, destination)

			case "json":
				destination = os.path.join(folder or "", filename or "")
				logging.info(f"Send raw json to '{destination}'")
				if (not isinstance(data, str)):
					data = json.dumps(data)

				if (not isinstance(data, bytes)):
					data = data.encode("utf8")

				with io.BytesIO(data) as handle_file:
					yield (handle_file, destination)

			case "file":
				for _data in common.ensure_container(data):
					if (os.path.splitext(_data)[1]):
						destination = os.path.join(folder or "", os.path.basename(_data))
						logging.info(f"Send '{_data}' to '{destination}'")
						with open(_data, "rb") as handle_file:
							yield (handle_file, destination)
						continue

					for (root, _, files) in os.walk(_data):
						for filename_source in files:
							if (os.path.splitext(filename_source)[1][1:] not in walk_allow):
								continue

							source = os.path.join(root, filename_source)
							destination = os.path.join(folder or "", filename or filename_source)
							logging.info(f"Send '{source}' to '{destination}'")
							with open(source, "rb") as handle_file:
								yield (handle_file, destination)

			case "csv":
				container = common.ensure_container(data, checkIteratorFunction=lambda item: not isinstance(item, pandas.DataFrame));
				if (not len(container)):
					logging.info(f"No data to write to csv")
					return

				data_isPandas = isinstance(container[0], pandas.DataFrame)
				destination = os.path.join(folder or "", filename or "").replace("\\", "/")

				if (data_isPandas and can_yield_pandas):
					for frame in container:
						logging.info(f"Send pandas frame to '{destination}'")
						yield (frame, destination)
					return

				elif (isinstance(container[0], dict)):
					container = (container,) # Account for lists of lists

				for _data in container:
					with io.StringIO(newline="") as handle_csv:
						frame = _data if data_isPandas else pandas.DataFrame(_data)
						frame.to_csv(handle_csv, header=True, index=False, date_format=r"%Y-%m-%dT%H:%M:%S.%fZ")
						
						if (can_yield_pandas):
							logging.info(f"Send pandas frame to '{destination}'")
							yield (frame, destination)
							continue

						logging.info(f"Send raw csv to '{destination}'")
						yield (io.BytesIO(handle_csv.getvalue().encode('utf8')), destination)

			case _:
				raise KeyError(f"Unknown *input_type* '{_input_type}'")

	@classmethod
	def get_frame(cls, *args, **kwargs):
		return pandas.concat(cls.yield_frame(*args, **kwargs), ignore_index=True)

	@classmethod
	def yield_frame(cls, data, *, is_excel=False, typeCatalogue=None, alias=None, remove=None, modifyData=None, replace_nan=True, no_duplicates=None,
		sort_by=None, sortByKwargs=None, sort_by_post=None, sortByPostKwargs=None, string_index=None, foreign=None, move=None, connection=None, **kwargs):
		""" A generator that yields pandas data frames.
		See: https://stackoverflow.com/questions/46283312/how-to-proceed-with-none-value-in-pandas-fillna/62691803#62691803
		See: https://github.com/pandas-dev/pandas/issues/25288#issuecomment-463054425

		data (str or DataFrame) - A filepath or pandas frame

		Example Input: yield_frame(data="./SyncSource/vineyards/acapdetail.csv")
		Example Input: yield_frame(data="./SyncSource/vineyards/acapdetail.csv", typeCatalogue={"lorem": "datetime"})
		Example Input: yield_frame(data="./SyncSource/vineyards/acapdetail.csv", alias={"Old Name 1": "new_name_1", "Old Name 2": "new_name_2"})
		Example Input: yield_frame(data="./SyncSource/vineyards/acapdetail.csv", remove=("Unecissary Column 1", "Unecissary Column 2"))
		Example Input: yield_frame(data=frame, modifyData=lambda data: [*data, {"lorem": "ipsum"}])
		Example Input: yield_frame(data=[{"lorem": 1, "ipsum": 2}, {"lorem": 3, "ipsum": 4}])
		Example Input: yield_frame(data=frame, foreign={"table": "lorem", "column": "ipsum"})
		Example Input: yield_frame(data=frame, foreign={"table": "lorem", "column": {"ipsum": "dolor"}})
		Example Input: yield_frame(data=frame, foreign={"table": "lorem", "column": {"ipsum": "dolor", "sit": "sit"}})
		Example Input: yield_frame(data=frame, move={"table": "lorem", "column": {"ipsum": "dolor"}})
		"""

		dtype = {}
		int_columns = {}
		datetime_columns = []
		if (typeCatalogue):
			for (key, value) in typeCatalogue.items():
				match value:
					case "datetime" | "date":
						datetime_columns.append(key)

					case "int":
						dtype[key] = "Int64"
						int_columns[key] = True

					case "str":
						dtype[key] = str

					case _:
						raise KeyError(f"Unknown *typeCatalogue['{key}']* '{value}'")

		found = False
		for (handle_binary, destination) in cls.yield_fileOutput(data=data, **{"input_type": "file", "can_yield_pandas": True, "connection":connection, **kwargs}):
			found = True
			data_isPandas = isinstance(handle_binary, pandas.DataFrame)
			
			if (isinstance(handle_binary, pandas.DataFrame)):
				frame = handle_binary

			elif (is_excel):
				frame = pandas.read_excel(handle_binary)
			else:
				# frame = pandas.read_csv(handle_binary, encoding="Windows-1252")
				frame = pandas.read_csv(handle_binary)

			if (alias):
				logging.info("Applying alias to data...")
				frame.rename(alias, axis=1, inplace=True)

			if (no_duplicates):
				# TODO: https://stackoverflow.com/questions/20625582/how-to-deal-with-settingwithcopywarning-in-pandas/53954986#53954986
				logging.info("Removing duplicate rows...")
				frame.drop_duplicates(subset=list(common.ensure_container(no_duplicates)), inplace=True)

			if ((typeCatalogue is not None) or len(datetime_columns)):
				logging.info("Converting data types...")

				for key in datetime_columns:
					frame[key] = pandas.to_datetime(frame[key], errors="coerce")

				for (key, value) in dtype.items():
					frame[key] = frame[key].astype(value)

			if (string_index):
				logging.info("Referencing String Index Columns...")
				for key in Postgres.apply_stringIndex(frame, string_index, connection=connection):
					int_columns[key] = True

			if (foreign):
				logging.info("Migrating Foreign Columns...")
				for foreignKwargs in common.ensure_container(foreign):
					for key in Postgres.apply_foreign(frame, **foreignKwargs, connection=connection):
						int_columns[key] = True

			if (move):
				logging.info("Moving Columns...")
				for moveKwargs in common.ensure_container(move):
					Postgres.apply_foreign(frame, move=True, **moveKwargs, connection=connection)

			if (replace_nan):
				frame.fillna(numpy.nan, inplace=True)
				frame.replace({numpy.nan: None}, inplace=True)

				for key in int_columns.keys():
					frame[key].fillna(0, inplace=True)

			if (remove):
				logging.info("Removing Columns...")
				frame.drop(list(remove), axis=1, inplace=True)

			if (True or logger.debugging):
				with pandas.option_context("display.max_rows", 4, "display.max_columns", None):
					logging.debug(f"\n{frame}")

			if (sort_by):
				logging.info("Sorting Pre Modified data...")
				frame.sort_values(by=sort_by, axis=0, inplace=True, ascending=True, na_position="last", **(sortByKwargs or {}))

			if (modifyData):
				logging.info("Modifying data...")
				for myFunction in common.ensure_container(modifyData):
					myFunction(frame)

			if (sort_by_post):
				logging.info("Sorting Post Modified data...")
				frame.sort_values(by=sort_by_post, axis=1, inplace=True, ascending=True, na_position="last", **(sortByPostKwargs or {}))

			if (logger.debugging):
				with pandas.option_context("display.max_rows", 4, "display.max_columns", None):
					logging.debug(f"\n{frame}")

			yield frame

		if (not found):
			raise ValueError("No files were found")

	@classmethod
	def getUnique(cls, data, column, *, as_list=True):
		""" Returns a list of the unique values in *data* for *columns*

		data (any) - What to search through
			- Can be a container
		column (str) - What column to search through
			- If list of strings, will return a combined list of all unique values for each item seprately
		as_list (bool) - If the answer should be returned as a list

		Example Input: getUnique(frame, "lorem")
		Example Input: getUnique(frame, ("lorem", "ipsum", "dolor"))
		Example Input: getUnique(frame, "lorem", as_list=True)
		"""

		columnList = list(common.ensure_container(column))

		data_str = pandas.concat(tuple(item[columnList].copy() for item in common.ensure_container(data, checkIteratorFunction=lambda item: not isinstance(item, pandas.DataFrame))), ignore_index=True)
		data_unique = pandas.concat(data_str[key].drop_duplicates() for key in columnList).drop_duplicates().dropna()
		
		if (as_list):
			return data_unique.tolist()

		frame = pandas.DataFrame()
		frame["value"] = data_unique

		return frame

	@classmethod
	def makeEtc(cls, frame, columnList, *, columnName="etc", overwrite=False):
		""" Moves values from *columnList* in *frame* into a JSON object column

		Example Input: makeEtc(frame, ("lorem", "ipsum"))
		Example Input: makeEtc(frame, ("lorem", "ipsum"), overwrite=True)
		Example Input: makeEtc(frame, ("lorem", "ipsum"), columnName="house_info")
		"""

		def myFunction(series):
			catalogue = series[columnName]
			for key in columnList:
				catalogue[key] = series[key]

		#########################

		if (overwrite or (columnName not in frame.columns)):
			frame[columnName] = [{} for i in range(len(frame))]

		frame.apply(myFunction, axis=1)
		frame.drop(list(columnList), axis=1, inplace=True)

		return frame

class BlobStorage(GeneralOutput):
	@classmethod
	def getConnection(cls, container, *, account_name="rpbireporting", account_key=None, configKwargs=None, connection=None, **kwargs):
		""" Returns a blob storage connection.

		account_name (str) - The name of the account to connect to
		account_key (str) - THey key of the account to connect to

		Example Input: getConnection("treehouse")
		Example Input: getConnection("ma-extract", account_name="birdeye01reporting")
		"""

		if (connection is not None):
			return connection

		account_key = account_key or config("account_key", "blob", **(configKwargs or {}))

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

	@classmethod
	def insert(cls, data, container="postgres", folder=None, filename=None, *, method="upsert", **kwargs):
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

		Example Input: insert([{Lorem: "ipsum"}])
		Example Input: insert([{Lorem: "ipsum"}], container="treehouse", folder="rps")
		Example Input: insert({Lorem: "ipsum"}, input_type="json")
		Example Input: insert("C:/lorem/ipsum", input_type="file")
		Example Input: insert("C:/lorem/ipsum", input_type="file", walk_allow=("csv", "xlsx"))
		Example Input: insert(open("lorem.txt", "r"), filename="lorem.txt", input_type="raw")
		"""

		connection = cls.getConnection(container, **kwargs)

		is_upsert = False
		existing = connection.ls_files(folder or "")
		if (len(existing)):
			match method:
				case "drop":
					logging.info(f"Dropping the following from '{folder}': {existing}...")
					for filename_source in existing:
						connection.rm(f"{folder}/{filename_source}")

				case "upsert":
					is_upsert = True

				case "insert":
					pass

				case _:
					raise KeyError(f"Unknown *method* '{method}'")

		found = False
		for (handle_binary, destination) in cls.yield_fileOutput(data=data, folder=folder, filename=filename, **kwargs):
			if (is_upsert):
				for filename_source in existing:
					if (destination.endswith(filename_source)):
						logging.info(f"Dropping '{filename_source}' from '{folder}'...")
						connection.rm(f"{folder}/{filename_source}")
						break

			found = True
			logging.info(f"Uploading blob to '{destination}'...")
			connection.client.upload_blob(name=destination, data=handle_binary.read())

		if (not found):
			raise ValueError("No files were found")

		return True

	@classmethod
	def select(cls, container="postgres", folder=None, filename=None, *,
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

		Example Input: select(container="ma-extract", folder="treehouse", filename="Resident.csv")
		Example Input: select(container="ma-extract", folder="treehouse", filename="Resident.csv", as_dict=False)
		Example Input: select(container="ma-extract", folder="treehouse", filename="Resident.csv", output_type="handle_str")
		Example Input: select(container="ma-extract", folder="[treehouse", "vineyards"], filename="Resident.csv")
		Example Input: select(container="ma-extract", folder="[treehouse", "vineyards"], filename="Resident.csv", multifile_method="separate")
		Example Input: select(container="ma-extract", folder="treehouse", filename="Resident.csv", force_list=True)
		"""

		def yieldFile():
			for _container in common.ensure_container(container, checkIteratorFunction=lambda item: not isinstance(item, pandas.DataFrame)):
				connection = cls.getConnection(_container, **kwargs)

				for _folder in common.ensure_container(folder or ("")):
					for _filename in common.ensure_container(filename or ("")):
						destination = os.path.join(_folder, _filename)
						logging.info(f"Getting blob from '{destination}'...")
						handle_client = connection.client.get_blob_client(blob=destination)
						if (output_type == "client"):
							yield handle_client
							continue

						handle_blob = handle_client.download_blob()
						if (output_type == "handle_blob"):
							yield handle_blob
							continue

						data_bin = handle_blob.readall()
						if ((output_type == "blob") or ((input_type == "excel") and (output_type == "python"))):
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
			case "csv" | "excel":

				answer = []
				for item in output:
					# See: https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html#pandas-read-csv
					# See: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
					if (input_type == "excel"):
						frame = pandas.read_excel(item)
					else:
						frame = pandas.read_csv(item)
					frame.to_csv(header=True, index=False, date_format=r"%Y-%m-%dT%H:%M:%S.%fZ")
					answer.append(frame)

				if (multifile_method == "separate"):
					return answer

				# See: https://pandas.pydata.org/pandas-docs/stable/user_guide/merging.html
				return pandas.concat(answer, join="outer", ignore_index=True, sort=False)

			case _:
				raise KeyError(f"Unknown *input_type* '{input_type}'")

	@classmethod
	def getMeta(cls, *args, force_list=False, **kwargs):
		output = tuple(handle_client.get_blob_properties() for handle_client in cls.select(*args, force_list=True, **{**kwargs, "output_type": "client"}))
		
		output_count = len(output)
		if (not output_count):
			return () if force_list else None

		if (force_list or (output_count > 1)):
			return output

		return output[0]

class Dropbox(GeneralOutput):
	@classmethod
	def insert(cls, data, container="systems_data/report_data_source", folder=None, filename=None, *, method="upsert", token=None, chunk_size=145, configKwargs=None, **kwargs):
		""" Sends data to dropbox.
		See: https://riptutorial.com/dropbox-api/example/1927/uploading-a-file-using-the-dropbox-python-sdk

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
		chunk_size (int) - How many MB large a chunk should be

		Example Input: insert([{Lorem: "ipsum"}])
		Example Input: insert([{Lorem: "ipsum"}], container="treehouse", folder="rps")
		Example Input: insert({Lorem: "ipsum"}, input_type="json")
		Example Input: insert("C:/lorem/ipsum", input_type="file")
		Example Input: insert("C:/lorem/ipsum", input_type="file", walk_allow=("csv", "xlsx"))
		Example Input: insert(open("lorem.txt", "r"), filename="lorem.txt", input_type="raw")
		"""

		token = token or config("token", "dropbox", **(configKwargs or {}))
		with dropbox.Dropbox(token, timeout=900) as dropboxHandle:
			# See: https://dropbox-sdk-python.readthedocs.io/en/latest/api/dropbox.html#dropbox.dropbox_client.Dropbox.files_list_folder
			# contents = dropboxHandle.files_list_folder(folder or "")
			# if (len(contents)):
			
			mode = None
			autorename = False
			match method:
				case "drop":
					raise NotImplementedError("Dropbox drop all folder contents")

				case "upsert":
					mode = dropbox.files.WriteMode.overwrite
					pass

				case "insert":
					autorename = True
					mode = dropbox.files.WriteMode.add

				case _:
					raise KeyError(f"Unknown *method* '{method}'")

			chunk_size *= 1024 * 1024

			# See: https://dropbox-sdk-python.readthedocs.io/en/latest/api/dropbox.html#dropbox.dropbox_client.Dropbox.files_upload
			for (handle_binary, destination) in cls.yield_fileOutput(data=data, folder=folder, filename=filename, **kwargs):
				# See: https://stackoverflow.com/questions/4677433/in-python-how-do-i-check-the-size-of-a-stringio-object/4677542#4677542
				handle_raw = getattr(handle_binary, "raw", handle_binary)
				current = handle_raw.tell()
				handle_raw.seek(0, os.SEEK_END)
				file_size = handle_raw.tell()
				handle_raw.seek(current, os.SEEK_SET)

				_destination = os.path.join("/", container, destination).replace("\\","/")
				if (file_size <= chunk_size):
					logging.info(f"Sending file to dropbox at '{_destination}'...")
					dropboxHandle.files_upload(handle_binary.read(), _destination, mode=mode, autorename=autorename)
					continue

				# See: https://dropbox-sdk-python.readthedocs.io/en/latest/api/files.html#dropbox.files.CommitInfo
				logging.info(f"Sending file in chunks to dropbox at '{_destination}'...")
				session = dropboxHandle.files_upload_session_start(handle_binary.read(chunk_size))
				cursor = dropbox.files.UploadSessionCursor(session_id=session.session_id, offset=handle_binary.tell())
				commit = dropbox.files.CommitInfo(path=_destination, mode=mode, autorename=autorename)

				while (handle_binary.tell() < file_size):
					logging.info(f"Sending chunk at '{cursor.offset}' of '{file_size}'...")
					if ((file_size - handle_binary.tell()) <= chunk_size):
						dropboxHandle.files_upload_session_finish(handle_binary.read(chunk_size), cursor, commit)
					else:
						dropboxHandle.files_upload_session_append(handle_binary.read(chunk_size), cursor.session_id, cursor.offset)
						cursor.offset = handle_binary.tell()

			return True

	@classmethod
	def select(cls):
		raise NotImplementedError("Get Dropbox File")

	@classmethod
	def getMeta(cls):
		# See: https://www.dropboxforum.com/t5/Dropbox-API-Support-Feedback/python-SDK/td-p/206272
		# See: https://dropbox-sdk-python.readthedocs.io/en/latest/api/dropbox.html#dropbox.dropbox_client.Dropbox.files_get_metadata

		raise NotImplementedError("Get Dropbox Metadata")
		# connection.files_get_metadata(filename)

class Webflow():
	@classmethod
	def select(cls, collection_id, limit=-1, *, offset=0, token=None, configKwargs=None, **kwargs):
		""" Returns the data from a webflow collection.
		See: https://www.briantsdawson.com/blog/webflow-api-how-to-get-site-collection-and-item-ids-for-zapier-and-parabola-use

		collection_id (str) - Which collection to connect to
		limit (int) - How much data to return. If less than 1 will return everything
		offset (int) - Use in combination with limit being not less than 1

		Example Input: select(collection_id="623107ba68bd7ba11ca033c7")
		Example Input: select(collection_id="623107ba68bd7ba11ca033c7", limit=1)
		Example Input: select(collection_id="623107ba68bd7ba11ca033c7", limit=100, offset=100)
		"""

		token = token or config("token", "webflow_treehouse", **(configKwargs or {}))

		webflow_api = webflowpy.Webflow.Webflow(token=token)
		return webflow_api.collection(collection_id=collection_id, limit=limit, all=limit <= 0)["items"]

	@classmethod
	def insert(cls, data, collection_id="623107ba68bd7ba11ca033c7", *, method="upsert", upsert_on="_id", live=False, token=None, configKwargs=None, **kwargs):
		""" Sends data to a webflow collection.
		See: https://www.briantsdawson.com/blog/webflow-api-how-to-get-site-collection-and-item-ids-for-zapier-and-parabola-use

		method (str) - How to handle sending the data
			- insert: Try adding it and throw an error if it already exists
			- drop: Drop all collection items in the folder and insert new collection items
		upsert_on (str) - What key to compare updates against
		live (bool) - If the change should be applied to the production server instead of the development server

		Example Input: insert([{Lorem: "ipsum"}])
		Example Input: insert([{Lorem: "ipsum"}], collection_id="623107ba68bd7ba11ca033c7")
		"""

		if (isinstance(data, pandas.DataFrame)):
			# See: https://pandas.pydata.org/pandas-docs/version/0.17.0/generated/pandas.DataFrame.to_dict.html#pandas.DataFrame.to_dict
			data = data.replace({numpy.nan: None}).to_dict("records")

		if (not len(data)):
			logging.info(f"No data to insert into '{table}'")
			return False

		token = token or config("token", "webflow_treehouse", **(configKwargs or {}))
		webflow_api = webflowpy.Webflow.Webflow(token=token)

		match method:
			case "drop":
				for item in webflow_api.items(collection_id=collection_id)["items"]:
					webflow_api.removeItem(collection_id=collection_id, item_id=item["_id"])

				for item in data:
					webflow_api.createItem(collection_id=collection_id, item_data=item, live=live)

			case "insert":
				for item in data:
					webflow_api.createItem(collection_id=collection_id, item_data=item, live=live)

			case "upsert":
				catalogue = {}
				catalogue = {item.get(upsert_on, None): item for item in webflow_api.items(collection_id=collection_id)["items"]}

				for item in data:
					item_existing = catalogue.get(item.get(upsert_on, None), None)
					if (not item_existing):
						if ("_draft" not in item):
							item["_draft"] = False

						if ("_archived" not in item):
							item["_archived"] = False

						webflow_api.createItem(collection_id=collection_id, item_data=item, live=live)
						continue

					# Check if any changes need to be made
					for (key, value) in item.items():
						if (item_existing.get(key, None) != value):
							webflow_api.patchItem(collection_id=collection_id, item_id=item_existing["_id"], item_data=item, live=live)
							break
		
			case _:
				raise KeyError(f"Unknown *method* '{method}'")

class GoogleSheet():
	@classmethod
	def select(cls, book_id, sheet_name=None, cell_range=None, **kwargs):
		""" Returns data from the google sheet.
		Use: https://towardsdatascience.com/read-data-from-google-sheets-into-pandas-without-the-google-sheets-api-5c468536550
		See: https://stackoverflow.com/questions/33713084/download-link-for-google-spreadsheets-csv-export-with-multiple-sheets/33727897#33727897

		Example Input: select(book_id)
		Example Input: select(book_id, "Sheet1")
		Example Input: select(book_id, "Sheet1", "A1:S27")
		"""

		if (not sheet_name):
			return pandas.read_csv(f"https://docs.google.com/spreadsheets/d/{book_id}/export?format=csv", **kwargs)
		
		url = f"https://docs.google.com/spreadsheets/d/{book_id}/gviz/tq?tqx=out:csv&sheet={sheet_name.replace(' ', '%20')}"
		if (cell_range):
			url += f"&range={cell_range}"
		logging.info(f"Loading Google Sheet from: {url}")
		return pandas.read_csv(url, **kwargs)

	@classmethod
	def number2Column(cls, number):
		# Use: https://stackoverflow.com/questions/23861680/convert-spreadsheet-number-to-column-letter/28782635#28782635
		
		text = ""
		alist = string.ascii_uppercase
		while number:
			mod = (number-1) % 26
			number = int((number - mod) / 26)  
			text += alist[mod]
		return text[::-1]

	@classmethod
	def column2Number(cls, text):
		# Use: https://stackoverflow.com/questions/7261936/convert-an-excel-or-spreadsheet-column-letter-to-its-number-in-pythonic-fashion/12640614#12640614
		
		number = 0
		for c in text:
			if c in string.ascii_letters:
				number = number * 26 + (ord(c.upper()) - ord('A')) + 1
		return number

testFile = os.path.join(os.path.dirname(__file__), "test.txt")
class TestCase(testing.BaseCase):
	def test_ManageAmerica_password(self):
		with self.assertLogs(level="INFO"):
			self.assertIsNotNone(ManageAmerica._getToken(
				login_user=config("user", "ma_vineyards"),
				login_password=config("password", "ma_vineyards"),
			))

		with self.assertLogs(level="INFO"):
			self.assertIsNotNone(ManageAmerica._getToken(
				login_user=config("user", "ma_treehouse"),
				login_password=config("password", "ma_treehouse"),
			))

	def test_Postgres_canInsert(self):
		with self.assertLogs(level="INFO"):
			frame = GeneralOutput.get_frame(
				data=[{"a": 1, "b": 2}, {"a": 2, "b": 3}],
				input_type="csv",
			)

			with self.assertRaises(psycopg2.errors.UndefinedColumn):
				Postgres.insert(
					data=frame,
					table="property",
					method="upsert",
				)

	def test_BlobStorage_canInsert(self):
		with self.assertLogs(level="INFO"):
			with self.assertRaises(FileNotFoundError):
				BlobStorage.insert(
					data="unknown.txt",
					container="ma-extract",
					folder="vineyards",
					input_type="file",
				)

			BlobStorage.insert(
				data=testFile,
				container="ma-extract",
				folder="vineyards",
				input_type="file",
			)

	def test_DropBox_canInsert(self):
		with self.assertLogs(level="INFO"):
			with self.assertRaises(FileNotFoundError):
				Dropbox.insert(
					data="unknown.txt",
					container="systems_data/report_data_source",
					folder="vineyards_feed",
					input_type="file",
				)

			Dropbox.insert(
				data=testFile,
				container="systems_data/report_data_source",
				folder="vineyards_feed",
				input_type="file",
			)

if (__name__ == "__main__"):
	testing.test(TestCase)