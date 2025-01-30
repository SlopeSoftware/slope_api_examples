import get_table_list
import load_data_tables
import load_decrement_tables as decrement
import pricing_solver
import read_data_tables
import logging
import setup

if __name__ == '__main__':
    # Change this to appropriate level for your run
    setup.setup_logging(logging.DEBUG)

    # Get List of Data Tables
    get_table_list.get_table_list()

    # Read an existing Data Table
    read_data_tables.read_tables()

    # Load Decrement Tables into a Model
    decrement.load_decrement_tables_parallel()

    # Load Data Tables into a Model
    load_data_tables.load_tables()

    solver = pricing_solver.Solver({
        "model_id": 16001,                          # Model ID of the model being run
        "pricing_table_name": "Pricing Input",
        "projection_id": 95944,                     # Projection ID with initial run of the pricing model
        "target": 0.05,                             # Target Pricing Value
    })
    solver.solve()

