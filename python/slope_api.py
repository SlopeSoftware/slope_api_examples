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
        """Check if the API response is successful and log errors if not."""
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

    def authorize(self, key: str, secret: str):
        """Authenticate the API using API Key and API Secret."""
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

    def refresh(self):
        """Refresh the API authentication session."""
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

    def expires_in_seconds(self) -> float:
        """Return the number of seconds until the current API session key expires."""
        return (self.__expires - datetime.datetime.now(datetime.timezone.utc)).total_seconds()

    def __keep_alive(self):
        """Check this one thread at a time so multiple refreshes don't get triggered. Race conditions on authorization calls = Bad."""
        with self.__lock:
            # The token expires after 10 minutes. Refresh it if we have less than 5 minutes left
            if self.expires_in_seconds() < 300:
                self.refresh()

    def __paginate_get_request(self, url: str, limit: int = 200) -> list:
        """Handle pagination for GET requests that return paginated results."""
        self.__keep_alive()
        all_items = []
        offset = 0
        
        while True:
            # Add pagination parameters to URL
            separator = "&" if "?" in url else "?"
            paginated_url = f"{url}{separator}Limit={limit}&Offset={offset}"
            
            response = self.session.get(paginated_url)
            self.check_response(response)
            result = response.json()

            if isinstance(result, dict) and "items" in result:
                items = result["items"]
                all_items.extend(items)
                
                # Check if there are more pages
                if result.get("offset") is None:
                    break
                offset = result["offset"]
            else:
                # Assume it's a direct list
                all_items.extend(result)
                break
                
        return all_items

    @staticmethod
    def __parse_data_table_json(json) -> pd.DataFrame:
        """Internal Method for converting data table contents into pandas DataFrame and setting data properties correctly."""
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

    def upload_file(self, filename: str, slope_path: str) -> int:
        """Upload a file from local machine to the SLOPE file manager."""
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

    def create_data_table(self, filename: str, slope_table_params) -> int:
        """Take a file from the local machine, upload it to SLOPE and create a data table from it."""
        self.__keep_alive()
        self.upload_file(filename, slope_table_params["filePath"])
        logging.debug(f"Creating Data Table with parameters: {slope_table_params}")
        response = self.session.post(f"{self.api_url}/DataTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    def update_data_table(self, filename: str, slope_table_params) -> int:
        """Take a file from the local machine, upload it to SLOPE and update an existing data table to create a new version of it."""
        self.__keep_alive()
        self.upload_file(filename, slope_table_params["filePath"])
        logging.debug(f"Updating Data Table with parameters: {slope_table_params}")
        response = self.session.patch(f"{self.api_url}/DataTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    def create_or_update_data_table(self, filename: str, slope_table_params) -> int:
        """Take a file from the local machine, upload it to SLOPE.
        If the requested data table does not already exist, create it from this file.
        If it does already exist, update it from this file."""
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

    def get_data_table_by_id(self, data_table_id: int) -> pd.DataFrame:
        """Download the contents of a data table with given Data Table ID.
        Returns a pandas DataFrame object with the contents of the table."""
        self.__keep_alive()
        logging.debug(f"Retrieving contents of data table with ID '{data_table_id}'")
        endpoint_url = f"{self.api_url}/DataTables/Data?DataTableId={data_table_id}"
        return self.__get_data_table(endpoint_url)

    def get_data_table_by_name(self, table_name: str, table_structure_id: int, version: int = None) -> pd.DataFrame:
        """Download the contents of a data table with given Data Table Name, Version, and Table Structure ID.
        Returns a pandas DataFrame object with the contents of the table."""
        self.__keep_alive()
        version_name = version or "latest"
        logging.debug(f"Retrieving contents of data table with Name '{table_name}' Version '{version_name}' of Table Structure ID '{table_structure_id}'")
        endpoint_url = f"{self.api_url}/DataTables/Data?Name={table_name}&TableStructureId={table_structure_id}"
        if version is not None:
            endpoint_url += f"&Version={version}"

        return self.__get_data_table(endpoint_url)

    def __get_data_table(self, url: str) -> pd.DataFrame:
        """Internal function for getting Data Table contents - Handles pagination of the data contents."""
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

    def get_scenario_tables(self, model_id: int) -> list:
        """Get model's scenario tables with full pagination support."""
        url = f"{self.api_url}/Models/{model_id}/ScenarioTables"
        return self.__paginate_get_request(url)

    def get_improvement_scales(self, model_id: int) -> list:
        """Get model's improvement scales with full pagination support."""
        url = f"{self.api_url}/Models/{model_id}/ImprovementScales"
        return self.__paginate_get_request(url)

    def get_projection_templates(self, model_id: int) -> list:
        """Get model's projection templates with full pagination support."""
        url = f"{self.api_url}/Models/{model_id}/ProjectionTemplates"
        return self.__paginate_get_request(url)

    def get_files(self, folders: list = None) -> list:
        """Get all currently available latest-version files with full pagination support."""
        url = f"{self.api_url}/Files/GetFiles"
        if folders:
            folder_param = ','.join(f'"{folder}"' if ',' in folder else folder for folder in folders)
            url += f"?Folders={folder_param}"
        
        return self.__paginate_get_request(url)

    def get_table_structure_columns(self, table_structure_id: int) -> list:
        """Get columns for a table structure with full pagination support."""
        url = f"{self.api_url}/TableStructures/{table_structure_id}/Columns"
        return self.__paginate_get_request(url)
    
    def list_data_tables(self, model_id: int, table_structure_name: str = None) -> list:
        """Get model's data tables with full pagination support."""
        url = f"{self.api_url}/Models/{model_id}/DataTables"
        if table_structure_name:
            url += f"?TableStructureName={table_structure_name}"
        return self.__paginate_get_request(url)

    def list_data_tables_by_structure_id(self, table_structure_id: int) -> list:
        """Get data tables for a specific table structure with full pagination support."""
        url = f"{self.api_url}/TableStructures/{table_structure_id}/DataTables"
        return self.__paginate_get_request(url)

    def list_table_structures(self, model_id: int) -> list:
        """Get model's table structures with full pagination support."""
        url = f"{self.api_url}/Models/{model_id}/TableStructures"
        return self.__paginate_get_request(url)

    def list_decrement_tables(self, model_id: int) -> list:
        """Get model's decrement tables with full pagination support."""
        url = f"{self.api_url}/Models/{model_id}/DecrementTables"
        return self.__paginate_get_request(url)

    def create_decrement_table(self, filename: str, slope_table_params) -> int:
        """Take a file from the local machine, upload it to SLOPE and create a decrement table from it."""
        self.__keep_alive()
        self.upload_file(filename, slope_table_params["filePath"])
        logging.debug(f"Creating Decrement Table with parameters: {slope_table_params}")
        # Check if table already exists and rename to new name
        response = self.session.post(f"{self.api_url}/DecrementTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    def create_only_decrement_table(self, slope_table_params) -> int:
        """Create a decrement table from a file that already exists in the SLOPE File Manager."""
        self.__keep_alive()
        logging.debug(f"Creating Decrement Table with parameters: {slope_table_params}")
        # Check if table already exists and rename to new name
        response = self.session.post(f"{self.api_url}/DecrementTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    def create_scenario_table(self, filename: str, slope_scenario_table_params) -> int:
        """Take a file from the local machine, upload it to SLOPE and create a scenario table from it."""
        self.__keep_alive()
        self.upload_file(filename, slope_scenario_table_params["filePath"])
        logging.debug(f"Creating scenario table with parameters: {slope_scenario_table_params}")
        response = self.session.post(f"{self.api_url}/ScenarioTables", json=slope_scenario_table_params)
        self.check_response(response)
        return response.json()["id"]

    def create_projection_from_template(self, template_id: int, name: str) -> int:
        """Create a new projection from an existing projection template."""
        self.__keep_alive()
        params = {"templateId": template_id, "name": name}
        logging.debug(f"Creating projection from Template {template_id} with name: {name}")
        response = self.session.post(f"{self.api_url}/Projections", json=params)
        self.check_response(response)
        return response.json()["id"]

    def copy_projection(self, projection_id: int, name: str, update_tables: bool = True) -> int:
        """Make a Copy of an existing projection."""
        self.__keep_alive()
        params = {"projectionName": name, "setTablesToLatestVersion": update_tables}
        logging.debug(f"Copy projection with ID of {projection_id} to new projection named '{name}'.")
        response = self.session.post(f"{self.api_url}/Projections/{projection_id}/Copy", json=params)
        self.check_response(response)
        return response.json()["id"]

    def update_projection(self, projection_id, properties):
        """Update values and properties on a projection."""
        self.__keep_alive()
        logging.debug(f"Updating Projection ID {projection_id} with parameters: {properties}")
        response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=properties)
        self.check_response(response)

    def update_projection_mpf(self, projection_id, portfolio_name, product_name, model_point_file_id):
        """Update the Model Point file on a projection."""
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

    def update_projection_table(self, projection_id, table_name, data_table_id):
        """Update the data table being used on a projection."""
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

    def run_projection(self, projection_id):
        """Run a projection."""
        self.__keep_alive()
        response = self.session.post(f"{self.api_url}/Projections/{projection_id}/run")
        if not response.ok:
            raise Exception(f"Failed to start projection with id: {projection_id}", response.text)

    def is_projection_running(self, projection_id) -> bool:
        """Check if a projection is still running.
        
        Returns:
            True - The projection is still running
            False - The projection has completed (possibly unsuccessfully)
        """
        return self.get_projection_details(projection_id, ["isRunning"])["isRunning"]

    def get_projection_details(self, projection_id: int, fields: list = None):
        """Return all of the properties set on a given Projection."""
        self.__keep_alive()
        response = ""
        if fields is None:
            response = self.session.get(f"{self.api_url}/Projections/{projection_id}")
        else :
            csv_fields = ','.join(fields)
            response = self.session.get(f"{self.api_url}/Projections/{projection_id}?Fields={csv_fields}")
        self.check_response(response)
        return response.json()

    def get_projection_status(self, projection_id) -> str:
        """Get the run status of a projection."""
        return self.get_projection_details(projection_id, ["status"])["status"]

    def wait_for_completion(self, projection_id):
        """Wait until a projection has completed running. Periodically check for updates until it is finished."""
        while self.is_projection_running(projection_id):
            status = self.get_projection_status(projection_id)
            logging.info(f"Waiting for Projection ID {projection_id} to finish. Current status: {status}")
            time.sleep(15)  # Check once every 15 seconds if it is done

    def generate_workbook_report(self, workbook_id: str, element_id: str, format_type: str, parameters: dict, row_limit: int = None, offset: int = None) -> dict:
        """Start a workbook report generation."""
        self.__keep_alive()
        report_params = {
            "reportFormat": format_type,
            "elementId": element_id,
        }
        if parameters:
            report_params["parameters"] = parameters
        if row_limit:
            report_params["rowLimit"] = row_limit
        if offset:
            report_params["offset"] = offset
            
        logging.debug(f"Generating workbook report {workbook_id} with parameters: {report_params}")
        response = self.session.post(f"{self.api_url}/Reports/Workbooks/{workbook_id}/Generate", json=report_params)
        self.check_response(response)
        return response.json()

    def get_workbook_report_status(self, generation_id: str) -> dict:
        """Get workbook report generation status."""
        self.__keep_alive()
        response = self.session.get(f"{self.api_url}/Reports/Workbooks/Status/{generation_id}")
        self.check_response(response)
        return response.json()
    
    def download_report(self, workbook_id: str, element_id: str, filename: str, format_type: str, parameters: dict, row_limit=None, offset=None, timeout=900):
        """Start a workbook report generation and poll for completion. Once complete, download the file."""
        self.__keep_alive()
        report_response = self.generate_workbook_report(
            workbook_id=workbook_id,
            element_id=element_id,
            report_format=format_type,
            parameters=parameters,
            row_limit=row_limit,
            offset=offset
        )
        generation_id = report_response["generationId"]
        start_time = time.time()
        while True:
            status_response = self.get_workbook_report_status(generation_id)
            if status_response["status"] == "Completed":
                download_url = status_response["downloadUrl"]
                break
            elif status_response["status"] == "Failed":
                raise Exception(f"Report generation failed: {status_response.get('message', 'Unknown error')}")
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Report generation did not complete within {timeout} seconds.")
            time.sleep(5)
        logging.debug(f"Downloading report from {download_url}")
        file_response = requests.get(download_url)
        self.check_response(file_response)
        
        logging.debug(f"Saving as '{filename}'.")
        file = open(filename, "wb")
        file.write(file_response.content)
        file.close()