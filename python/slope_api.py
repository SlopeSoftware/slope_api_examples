import requests


class SlopeApi:
    api_url = "https://api.slopesoftware.com/api/v1"

    def __init__(self):
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({"Content-type": "application/json"})

    @staticmethod
    def check_response(response):
        print(response)
        if not response.ok:
            print(response.json())
            response.raise_for_status()

    def authorize(self, key: str, secret: str):
        auth_params = {
            "apiKey": key,
            "apiSecretKey": secret
        }
        response = self.session.post(f"{self.api_url}/Authorize", json=auth_params)
        self.check_response(response)
        access_token = response.json()["accessToken"]
        self.session.headers.update({"Authorization": f"Bearer {access_token}"})
            
    def upload_file(self, filename: str, slope_path: str) -> int:
        slope_file_params = {"filePath": slope_path}
        response = self.session.post(f"{self.api_url}/Files/GetUploadUrl", json=slope_file_params)
        uploadUrl = response.json()["uploadUrl"]
        
        # Note - Do not use session here - this is a direct call to s3 and does not use the session auth
        response = requests.put(uploadUrl, data=open(filename, "rb"))
        self.check_response(response)

        response = self.session.post(f"{self.api_url}/Files/SaveUpload", json=slope_file_params)
        self.check_response(response)
        return response.json()["fileId"]

    def list_table_structures(self, model_id: int) -> any:
        response = self.session.get(f"{self.api_url}/TableStructures/List/{model_id}")
        self.check_response(response)
        return response.json()

    def create_data_table(self, filename: str, slope_table_params) -> int:
        self.upload_file(filename, slope_table_params["filePath"])
        response = self.session.post(f"{self.api_url}/DataTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]

    def update_data_table(self, filename: str, slope_table_params) -> int:
        self.upload_file(filename, slope_table_params["filePath"])
        response = self.session.patch(f"{self.api_url}/DataTables", json=slope_table_params)
        self.check_response(response)
        return response.json()["id"]
    
    def list_data_tables(self, model_id: int) -> any:
        response = self.session.get(f"{self.api_url}/DataTables/List/{model_id}")
        self.check_response(response)
        return response.json()
    
    def create_scenario_table(self, filename: str, slope_scenario_table_params) -> int:
        self.upload_file(filename, slope_scenario_table_params["filePath"])
        response = self.session.post(f"{self.api_url}/ScenarioTables", json=slope_scenario_table_params)
        self.check_response(response)
        return response.json()["id"]
    
    def create_projection_from_template(self, template_id: int, name: str) -> int:
        params = {"templateId": template_id, "name": name}
        response = self.session.post(f"{self.api_url}/Projections", json=params)
        self.check_response(response)
        return response.json()["id"]
    
    def update_projection(self, projection_id, properties):
        response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=properties)
        self.check_response(response)
        
    def update_projection_mpf(self, projection_id, portfolio_name, product_name, model_point_file_id):
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
        response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=projection_update_parameters)
        self.check_response(response)
        
    def update_projection_table(self, projection_id, table_name, data_table_id):
        projection_update_parameters = {
            "dataTables": [{
                "tableStructureName": table_name,
                "dataTableId": data_table_id
            }]      
        }
        response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=projection_update_parameters)
        self.check_response(response)
    
    def run_projection(self, projection_id):
        response = self.session.post(f"{self.api_url}/Projections/{projection_id}/run")
        if not response.ok:
            raise Exception(f"Failed to start projection with id: {projection_id}", response.text)

    def is_projection_running(self, projection_id) -> bool:
        response = self.session.get(f"{self.api_url}/Projections/{projection_id}")
        if response.ok:
            return response.json()["isRunning"]
        else:
            return False
    
    def get_projection_status(self, projection_id) -> str:
        response = self.session.get(f"{self.api_url}/Projections/{projection_id}")
        self.check_response(response)
        return response.json()["status"]
    
    def download_report(self, workbook_id, element_id, filename, format_type, parameters):
        report_params = {
            "elementId": element_id,
            "reportFormat": format_type,
            "parameters": parameters
        }
        response = self.session.post(f"{self.api_url}/Reports/Workbooks/{workbook_id}", json=report_params)
        self.check_response(response)
        file = open(filename, "wb")
        file.write(response.content)
        file.close()
