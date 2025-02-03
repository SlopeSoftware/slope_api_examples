# Python Examples

This example has been tested with Python 3.10 and requires some packages (see _dependencies_ in the file `pyproject.toml`).

## Setup

Update the config file (`config.ini`) with the API key and secret.

This project is compatible with the command line tool `uv` (https://docs.astral.sh/uv/getting-started/installation/).

## Examples
- `example_get_table_list.py` - Shows how to get a list of table structures, data tables and decrement tables.
- `example_read_data_tables.py` - Shows different ways to get data table information.
- `example_load_data_tables.py` - Shows how to create data tables (also includes a parallel method).
- `example_load_decrement_tables.py` - Shows how to create decrement tables (also includes a parallel method).
- `example_run_projection.py` - Setups up the tables for projection, creates the projection from a template, updates the parameters on the projection, runs it and then downloads the results once it's finished.
- `example_pricing_solver.py` - Does a goal seek to solve for a value by continuously running a projection, feeding its results into another projection, and repeating the process until the desired value is achieved.