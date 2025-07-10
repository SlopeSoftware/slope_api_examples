import logging
import keys, setup, slope_api

model_id = 11759
table_structure_id = 293310
table_structure_name = "Plan Code Table - Whole Life"

if __name__ == '__main__':
    # Change this to appropriate level for your run
    setup.setup_logging(logging.INFO)

    api = slope_api.SlopeApi()
    api.authorize(keys.api_key, keys.api_secret)

    # Get a List of all Data Tables on a Model
    tables = api.list_data_tables(model_id)

    # Got a List of all Data Tables with Table Structure ID =
    tables = api.list_data_tables_by_structure_id(table_structure_id)

    # Get a List of all Data Tables where Table Structure Name =  on a Model
    tables = api.list_data_tables(model_id, table_structure_name)

    # Get a List of all Decrement Tables on a Model
    tables = api.list_decrement_tables(model_id)

    # Get a List of all Table Structures on a Model
    tables = api.list_table_structures(model_id)

