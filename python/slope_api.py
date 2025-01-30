import requests
import datetime
import logging
import time
import threading
from dateutil.parser import parse
import pandas as pd

class SlopeApi:
    api_url = "https://api.slopesoftware.com/api/v1"
    __expires: datetime.datetime
    __refresh_token = ""
    __lock = threading.Lock()

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Content-type": "application/json"})

    @staticmethod
    def check_response(response):

        if not response.ok:
            logging.error(f"API call did not succeed. Response {response}")
            logging.error(f"Headers: {response.headers}")
            logging.error(f"Response Content:")
            content_type = response.headers.get('Content-Type')
            if content_type is not None and 'json' in content_type:
                logging.error(response.json())
            else:
                logging.error(response.content)
            response.raise_for_status()

        logging.debug(f"API Response: {response}")

    # Authenticate the API using API Key and API Secret
    def authorize(self, key: str, secret: str):
        with self.__lock:
            auth_params = {
                "apiKey": key,
                "apiSecretKey": secret
            }
            logging.debug("Authorizing SLOPE API")
            response = self.session.post(f"{self.api_url}/Authorize", json=auth_params)
            self.check_response(response)
            access_token = response.json()["accessToken"]
            self.session.headers.update({"Authorization": f"Bearer {access_token}"})
            self.__refresh_token = response.json()["refreshToken"]
            self.__expires = parse(response.json()["expires"])

    # Refresh the API authentication session
    def refresh(self):
        logging.debug("Refreshing API auth token")
        refresh_params = {
            "refreshToken": self.__refresh_token
        }
        response = requests.post(f"{self.api_url}/Authorize/Refresh", json=refresh_params, headers={"Content-Type": "application/json"})
        self.check_response(response)
        access_token = response.json()["accessToken"]
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})
        self.__refresh_token = response.json()["refreshToken"]
        self.__expires = parse(response.json()["expires"])

    # Returns an integer representing the number of seconds until the current API session key expires
    def expires_in_seconds(self) -> float:
        # How many seconds until the current token expires
        return (self.__expires - datetime.datetime.now(datetime.timezone.utc)).total_seconds()

    def __keep_alive(self):
        # Check this one thread at a time so multiple refreshes don't get triggered. Race conditions on authorization calls = Bad.
        with self.__lock:
            # The token expires after 10 minutes. Refresh it if we have less than 5 minutes left
            if self.expires_in_seconds() < 300:
                self.refresh()

    # Upload a file from local machine to the SLOPE file manager
    def upload_file(self, filename: str, slope_path: str) -> int:
        self.__keep_alive()
        slope_file_params = {"filePath": slope_path}
        response = self.session.post(f"{self.api_url}/Files/GetUploadUrl", json=slope_file_params)
        upload_url = response.json()["uploadUrl"]

        logging.debug(f"Uploading file '{filename}' to '{slope_path}'.")
        # Note - Do not use session here - this is a direct call to s3 and does not use the Slope session auth
        response = requests.put(upload_url, data=open(filename, "rb"))
        self.check_response(response)

        response = self.session.post(f"{self.api_url}/Files/SaveUpload", json=slope_file_params)
        self.check_response(response)
        return response.json()["fileId"]

    # Take a file from the local machine, upload it to SLOPE and create a data table from it
    def create_data_table(self, filename: str, slope_table_params) -> int:
        self.__keep_alive()
        self.upload_file(filename, slope_table_params["filePath"])
        logging.debug(f"Creating Data Table with parameters: {slope_table_params}")
        response = self.session.post(f"{self.api_url}/DataTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    # Take a file from the local machine, upload it to SLOPE and update an existing data table to create a new version of it
    def update_data_table(self, filename: str, slope_table_params) -> int:
        self.__keep_alive()
        self.upload_file(filename, slope_table_params["filePath"])
        logging.debug(f"Updating Data Table with parameters: {slope_table_params}")
        response = self.session.patch(f"{self.api_url}/DataTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    # Take a file from the local machine, upload it to SLOPE
    # If the requested data table does not already exist, create it from this file
    # If it does already exist, update it from this file
    def create_or_update_data_table(self, filename: str, slope_table_params) -> int:
        self.__keep_alive()
        self.upload_file(filename, slope_table_params["filePath"])
        response = self.session.post(f"{self.api_url}/DataTables", json=slope_table_params)
        if response.ok:
            logging.debug(f"Created new Data Table with parameters: {slope_table_params}")
            return response.json()["id"]
        if response.status_code != 409:
            self.check_response(response)
            return None

        logging.debug(f"Updating Data Table with parameters: {slope_table_params}")
        response = self.session.patch(f"{self.api_url}/DataTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    # Download the contents of a data table with given Data Table ID
    # Returns an pandas DataFrame object with the contents of the table
    def get_data_table_by_id(self, data_table_id: int) -> pd.DataFrame:
        self.__keep_alive()
        logging.debug(f"Retrieving contents of data table with ID '{data_table_id}'")
        endpoint_url = f"{self.api_url}/DataTables/Data?DataTableId={data_table_id}"
        return self.__get_data_table(endpoint_url)

    # Download the contents of a data table with given Data Table Name, Version, and Table Structure ID
    # Returns an pandas DataFrame object with the contents of the table
    def get_data_table_by_name(self, table_name: str, table_structure_id: int, version: int = None) -> pd.DataFrame:
        self.__keep_alive()
        version_name = version or "latest"
        logging.debug(f"Retrieving contents of data table with Name '{table_name}' Version '{version_name}' of Table Structure ID '{table_structure_id}'")
        endpoint_url = f"{self.api_url}/DataTables/Data?Name={table_name}&TableStructureId={table_structure_id}"
        if version is not None:
            endpoint_url += f"&Version={version}"

        return self.__get_data_table(endpoint_url)

    # Internal function for getting Data Table contents - Handles pagination of the data contents
    def __get_data_table(self, url: str) -> pd.DataFrame:
        response = self.session.get(url)
        self.check_response(response)
        json = response.json()
        if 'rows' not in json:
            logging.error("Data Table Files not implemented yet. Empty Data Returns")
            return pd.DataFrame()

        table = SlopeApi.__parse_data_table_json(json)

        # Check if we got the whole table or if we hit the row limit
        # If row limit was hit, then offset will be not' 'None' (and contain an integer value)
        # Keep looping until we get the whole table
        while json['offset']:
            logging.debug(f"Retrieving more data from table '{json['name']}' ID '{json['id']}' starting at row {json['offset']}")
            response = self.session.get(url + f"&Offset={json['offset']}")
            self.check_response(response)
            json = response.json()
            table = pd.concat([table, SlopeApi.__parse_data_table_json(json)])

        return table

    # Internal Method for converting data table contents into pandas DataFrame and setting data properties correctly
    @staticmethod
    def __parse_data_table_json(json) -> pd.DataFrame:
        columns = []
        index = []
        # Get the column names
        for col in json['columns']:
            columns.append(col['name'])
            if col['isIndex']:
                index.append(col['name'])
        df = pd.DataFrame.from_records(data=json['rows'], columns=columns)
        # Convert each column to the correct data type
        for col in json['columns']:
            if col['dataType'] == 'Integer' or col['dataType'] == 'Decimal':
                df[col['name']] = pd.to_numeric(df[col['name']])
            elif col['dataType'] == 'Boolean':
                df[col['name']] = df[col['name']].astype(bool)
            else:
                df[col['name']] = df[col['name']].astype(str)

        df.set_index(index)
        return df

    # Returns a list of all data tables that exist on a given Model ID
    def list_data_tables(self, model_id: int) -> []:
        self.__keep_alive()
        logging.debug(f"Retrieving Data Table listing from model {model_id}")
        response = self.session.get(f"{self.api_url}/DataTables/List?ModelId={model_id}")
        self.check_response(response)
        return response.json()

    # Returns a list of all data tables that exist on a given Model ID with specified Table Structure Name
    def list_data_tables_by_structure_name(self, model_id: int, table_structure_name: str) -> []:
        self.__keep_alive()
        logging.debug(f"Retrieving Data Table listing from model {model_id} with Table Structure name '{table_structure_name}'")
        response = self.session.get(f"{self.api_url}/DataTables/List?ModelId={model_id}&TableStructureName={table_structure_name}")
        self.check_response(response)
        return response.json()

    # Returns a list of all data tables that exist on a given Table Structure ID
    def list_data_tables_by_structure_id(self, table_structure_id: int) -> []:
        self.__keep_alive()
        logging.debug(f"Retrieving Data Table listing from Table Structure {table_structure_id}")
        response = self.session.get(
            f"{self.api_url}/DataTables/List?TableStructureId={table_structure_id}")
        self.check_response(response)
        return response.json()

    # Returns a list of all table structures that exist on a given Model ID
    def list_table_structures(self, model_id: int) -> []:
        self.__keep_alive()
        logging.debug(f"Retrieving all Table Structures from mode {model_id}")
        response = self.session.get(f"{self.api_url}/TableStructures/List/{model_id}")
        self.check_response(response)
        return response.json()

    # Take a file from the local machine, upload it to SLOPE and create a decrement table from it
    def create_decrement_table(self, filename: str, slope_table_params) -> int:
        self.__keep_alive()
        self.upload_file(filename, slope_table_params["filePath"])
        logging.debug(f"Creating Decrement Table with parameters: {slope_table_params}")
        # Check if table already exists and rename to new name
        response = self.session.post(f"{self.api_url}/DecrementTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    # Create a decrement table from a file that already exists in the SLOPE File Manager
    def create_only_decrement_table(self, slope_table_params) -> int:
        self.__keep_alive()
        logging.debug(f"Creating Decrement Table with parameters: {slope_table_params}")
        # Check if table already exists and rename to new name
        response = self.session.post(f"{self.api_url}/DecrementTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    # Returns a list of all decrement tables that exist on a given Model ID
    def list_decrement_tables(self, model_id: int) -> []:
        self.__keep_alive()
        logging.debug(f"Retrieving Data Table listing from model {model_id}")
        response = self.session.get(f"{self.api_url}/DecrementTables/List?ModelId={model_id}")
        self.check_response(response)
        return response.json()

    # Take a file from the local machine, upload it to SLOPE and create a scenario table from it
    def create_scenario_table(self, filename: str, slope_scenario_table_params) -> int:
        self.__keep_alive()
        self.upload_file(filename, slope_scenario_table_params["filePath"])
        logging.debug(f"Creating scenario table with parameters: {slope_scenario_table_params}")
        response = self.session.post(f"{self.api_url}/ScenarioTables", json=slope_scenario_table_params)
        self.check_response(response)
        return response.json()["id"]

    # Create a new projection from an existing projection template
    def create_projection_from_template(self, template_id: int, name: str) -> int:
        self.__keep_alive()
        params = {"templateId": template_id, "name": name}
        logging.debug(f"Creating projection from Template {template_id} with name: {name}")
        response = self.session.post(f"{self.api_url}/Projections", json=params)
        self.check_response(response)
        return response.json()["id"]

    # Make a Copy of an existing projection
    def copy_projection(self, projection_id: int, name: str, update_tables: bool = True) -> int:
        self.__keep_alive()
        params = {"projectionName": name, "setTablesToLatestVersion": update_tables}
        logging.debug(f"Copy projection with ID of {projection_id} to new projection named '{name}'.")
        response = self.session.post(f"{self.api_url}/Projections/{projection_id}/Copy", json=params)
        self.check_response(response)
        return response.json()["id"]

    # Update values and properties on a projection
    def update_projection(self, projection_id, properties):
        self.__keep_alive()
        logging.debug(f"Updating Projection ID {projection_id} with parameters: {properties}")
        response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=properties)
        self.check_response(response)

    # Update the Model Point file on a projection
    def update_projection_mpf(self, projection_id, portfolio_name, product_name, model_point_file_id):
        self.__keep_alive()
        projection_update_parameters = {
            "portfolios": [{
                "portfolioName": portfolio_name,
                "products": [{
                    "productName": product_name,
                    "modelPointFile": {
                        "fileId": model_point_file_id
                    }
                }]
            }]
        }
        logging.debug(f"Updating Projection ID {projection_id} with parameters: {projection_update_parameters}")
        response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=projection_update_parameters)
        self.check_response(response)

    # Update the data table being used on a projection
    def update_projection_table(self, projection_id, table_name, data_table_id):
        self.__keep_alive()
        projection_update_parameters = {
            "dataTables": [{
                "tableStructureName": table_name,
                "dataTableId": data_table_id
            }]
        }
        logging.debug(f"Updating Projection ID {projection_id} with parameters: {projection_update_parameters}")
        response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=projection_update_parameters)
        self.check_response(response)

    # Run a projection
    def run_projection(self, projection_id):
        self.__keep_alive()
        response = self.session.post(f"{self.api_url}/Projections/{projection_id}/run")
        if not response.ok:
            raise Exception(f"Failed to start projection with id: {projection_id}", response.text)

    # Check if a projection is still running
    # True - The projection is still running
    # False - The projection has completed (possibly unsuccessfully)
    def is_projection_running(self, projection_id) -> bool:
        self.__keep_alive()
        response = self.session.get(f"{self.api_url}/Projections/{projection_id}")
        if response.ok:
            return response.json()["isRunning"]
        else:
            return False

    # Returns all of the properties set on a given Projection
    def get_projection_details(self, projection_id: int, fields: [str] = None):
        self.__keep_alive()
        response = ""
        if fields is None:
            response = self.session.get(f"{self.api_url}/Projections/{projection_id}")
        else :
            csv_fields = ','.join(fields)
            response = self.session.get(f"{self.api_url}/Projections/{projection_id}?Fields={csv_fields}")
        self.check_response(response)
        return response.json()

    # Gets the run status of a projection
    def get_projection_status(self, projection_id) -> str:
        return self.get_projection_details(projection_id, ["status"])["status"]

    # Wait until a projection has completed running. Periodically check for updates until is is finished.
    def wait_for_completion(self, projection_id):
        while self.is_projection_running(projection_id):
            status = self.get_projection_status(projection_id)
            logging.info(f"Waiting for Projection ID {projection_id} to finish. Current status: {status}")
            time.sleep(15)  # Check once every 15 seconds if it is done

    # Download results from a single element in a single workbook
    def download_report(self, workbook_id, element_id, filename, format_type, parameters):
        self.__keep_alive()
        report_params = {
            "elementId": element_id,
            "reportFormat": format_type,
            "parameters": parameters
        }
        logging.debug(f"Downloading report from workbook {workbook_id} with parameters: {report_params}")
        response = self.session.post(f"{self.api_url}/Reports/Workbooks/{workbook_id}", json=report_params)
        self.check_response(response)
        logging.debug(f"Saving as '{filename}'.")
        file = open(filename, "wb")
        file.write(response.content)
        file.close()
