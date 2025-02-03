import logging
import keys, setup, slope_api

data_table_id = 622058
table_structure_id = 293310

# Read a data table from SLOPE and print it out
if __name__ == '__main__':
    # Change this to appropriate level for your run
    setup.setup_logging(logging.INFO)

    api_client = slope_api.SlopeApi()
    api_client.authorize(keys.api_key, keys.api_secret)

    # Read by Data Table ID
    table = api_client.get_data_table_by_id(data_table_id)
    print(table)

    # Read by Table Name and Table Structure ID (optional version number too)
    table = api_client.get_data_table_by_name('Flat_To65_EP90', table_structure_id)

    # This example shows with the Version number specified as well
    # If version is left out, the latest table version will be retrieved
    # table = api_client.get_data_table_by_name('Flat_To65_EP90', table_structure_id, version=1)

    print(table)

