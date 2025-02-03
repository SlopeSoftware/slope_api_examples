import threading
import logging
import keys, setup, slope_api

# the following list contains sets of data to load to SLOPE with the following format
# {
#   "name": The name of the decrement table to create in SLOPE
#   "path": the location of the file on this computer (full path and filename)
#   "sheet": if 'path' refers to an Excel file, then this should contain the Worksheet name that has the decrement table
#   "type": "Standard", "Improving", or "SelectAndUltimate"
#   "year": The year from which any improvement should start (Typically the decrement table year)
#   "frequency": "Annual" or "Monthly" - specifies the time of the select period
# }
modelId = 11377      # The model ID of the model where tables should be loaded - https://support.slopesoftware.com/hc/en-us/articles/22875658777108-API-IDs-and-Keys
tables = [
    {"name": "API - 1980 CSO.csv", "path": r'c:\api\1980 CSO Basic ANB.csv', "sheet": "", "type": "Standard", "year": 1980, "frequency": "Annual"},
    {"name": "API - 2012-IAM.xlsx", "path": r'c:\api\2012 IAM.xlsx', "sheet": "Basic Plus Improvement Scale", "type": "Improving", "year": 2012, "frequency": "Annual"},
    {"name": "Premium Rates.xlsx", "path": r'c:\api\PremiumRates.xlsx', "sheet": "Sheet2", "type": "SelectAndUltimate", "year": 2017, "frequency": "Annual"},
]


# Single Threaded load of tables. For small batches or single table loads, this will be sufficient
def load_decrement_tables():
    api_client = slope_api.SlopeApi()
    api_client.authorize(keys.api_key, keys.api_secret)

    for table in tables:
        decrement_table_parameters = {
            "modelId": modelId,
            "name": table["name"],
            "filePath": f'api/{table["name"]}',
            "delimiter": ",",
            "excelSheetName": table["sheet"],
            "decrementTableType": table["type"],
            "improvementBaseYear": table["year"],
            'selectPeriodFrequency': table["frequency"]
        }
        table_id = api_client.create_decrement_table(table["path"], decrement_table_parameters)


# Multi-Threaded table load. This will load multiple tables in parallel into SLOPE.
# For large sets of tables, this is faster. Be sure to consider table size and network bandwidth.
def load_decrement_tables_parallel():
    api_client = slope_api.SlopeApi()
    api_client.authorize(keys.api_key, keys.api_secret)

    loader_threads = []

    for table in tables:
        decrement_table_parameters = {
            "modelId": modelId,
            "name": table["name"],
            "filePath": f'api/{table["name"]}',
            "delimiter": ",",
            "excelSheetName": table["sheet"],
            "decrementTableType": table["type"],
            "improvementBaseYear": table["year"],
            'selectPeriodFrequency': table["frequency"]
        }
        thread = threading.Thread(target=api_client.create_decrement_table, args=(table["path"], decrement_table_parameters))
        thread.start()
        loader_threads.append(thread)
        # SLOPE Api has a rate limiter in place. If your code goes too fast, add some delay to space out the requests
        # time.sleep(10 / 1000)     # wait ~10 ms between calls

    # Wait for all table loads to be done
    for thread in loader_threads:
        thread.join()


if __name__ == '__main__':
    # Change this to appropriate level for your run
    setup.setup_logging(logging.INFO)

    load_decrement_tables()
