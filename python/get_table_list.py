import keys, slope_api

def get_table_list():
    api = slope_api.SlopeApi()
    api.authorize(keys.api_key, keys.api_secret)

    # Get a List of all Data Tables on a Model
    tables = api.list_data_tables(11759)

    # Got a List of all Data Tables with Table Structure ID =
    tables = api.list_data_tables_by_structure_id(293310)

    # Get a List of all Data Tables where Table Structure Name =  on a Model
    tables = api.list_data_tables_by_structure_name(11759,"Plan Code Table - Whole Life")

    # Get a List of all Decrement Tables on a Model
    tables = api.list_decrement_tables(11759)

    # Get a List of all Table Structures on a Model
    tables = api.list_table_structures(11759)

