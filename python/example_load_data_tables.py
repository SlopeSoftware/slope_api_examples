import threading
import logging
import keys, setup, slope_api

# the following list contains sets of data to load to SLOPE with the following format
# {
#   "name": The name of the table to create in SLOPE (folder path and name in the File Manager)
#   "path": tho location of the file on this computer (full path and filename)
#   "structure": The Table Structure ID in SLOPE - https://support.slopesoftware.com/hc/en-us/articles/22875658777108-API-IDs-and-Keys
# }
table_structure_id = 215852
tables = [
    {"name": "Table A", "path": r'c:\api\TableA.csv', "structure": table_structure_id},
    {"name": "Table B", "path": r'c:\api\TableB.csv', "structure": table_structure_id},
]


# Single Threaded load of tables. For small batches or single table loads, this will be sufficient
def load_data_tables():
    api_client = slope_api.SlopeApi()
    api_client.authorize(keys.api_key, keys.api_secret)

    for table in tables:
        data_table_parameters = {
            "tableStructureId": table["structure"],
            "name": table["name"],
            "filePath": f'api/{table["name"]}.csv',
            "delimiter": ","
        }
        data_table_id = api_client.create_data_table(table["path"], data_table_parameters)


# Multi-Threaded table load. This will load multiple tables in parallel into SLOPE.
# For large sets of tables, this is faster. Be sure to consider table size and network bandwidth.
def load_data_tables_parallel():
    # Connect SLOPE API
    api_client = slope_api.SlopeApi()
    api_client.authorize(keys.api_key, keys.api_secret)

    loader_threads = []

    for table in tables:
        data_table_parameters = {
            "tableStructureId": table["structure"],
            "name": table["name"],
            "filePath": f'api/{table["name"]}.csv',
            "delimiter": ","
        }
        thread = threading.Thread(target=api_client.create_data_table, args=(table["path"], data_table_parameters))
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

    load_data_tables()
