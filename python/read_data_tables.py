import keys, slope_api

# Read a data table from SLOPE and print it out
def read_tables():
    api_client = slope_api.SlopeApi()
    api_client.authorize(keys.api_key, keys.api_secret)

    # Read by Data Table ID
    table = api_client.get_data_table_by_id(622058)
    print(table)

    # Read by Table Name and Table Structure ID (optional version number too)
    table = api_client.get_data_table_by_name('Flat_To65_EP90', 293310)

    # This example shows with the Version number specified as well
    # If version is left out, the latest table version will be retrieved
    # table = api_client.get_data_table_by_name('Flat_To65_EP90', 293310, 1)

    print(table)

