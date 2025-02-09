import datetime
import time
import logging
import keys, slope_api, setup

model_id = 9999  # The ID of the model to be run
workbook_id = "5rMaW9R0yVoehrIjyAUtew"  # The ID of the workbook with the element to download
element_id = "SQ_fXFwL0L"  # The ID of the element in the workbook to download
template_id = 9999  # The ID of the Projection Template to be run

table_structure_id = 9999  # The ID of the table structure of the data table to create
data_table_file_path = r"C:\Api\Assumption Update.xlsx"
data_table_file_excel_sheet_name = "Assumptions"
data_table_name = "Data Table Name"

model_point_file_path = r"C:\Api\Inforce.csv"
model_point_portfolio_name = "Portfolio 1"
model_point_product_name = "Product A"

scenario_table_file_path = r"C:\Api\Scenario.csv"
valuation_date = datetime.datetime(2021, 1, 31)  # The start date of the scenario table
valuation_date_string = valuation_date.strftime("%Y-%m")

report_download_file_path_excel = r"C:\Api\Results.xlsx"
report_download_file_path_csv = r"C:\Api\Results.csv"


if __name__ == '__main__':
    # Change this to appropriate level for your run
    setup.setup_logging(logging.INFO)

    api_client = slope_api.SlopeApi()
    api_client.authorize(keys.api_key, keys.api_secret)

    # Create Scenario File
    scenario_table_parameters = {
        "modelId": model_id,
        "name": f"Scenarios {valuation_date_string}",
        "startDate": valuation_date.isoformat(),
        "yieldCurveRateType": "BondEquivalent",
        "filePath": f"Scenario Files/Scenarios {valuation_date_string}.csv",
        "delimiter": ","
    }
    scenario_table_id = api_client.create_scenario_table(scenario_table_file_path, scenario_table_parameters)

    # Update Assumptions
    table_structures = api_client.list_table_structures(model_id)
    logging.info("Listing Table Structures")
    for d in table_structures:
        logging.info(f"Id: {d['id']}, Name: {d['name']}, Description: {d['description']}")

    data_table_parameters = {
        "tableStructureId": table_structure_id,
        "name": f"Assumptions {valuation_date_string}",
        "filePath": f"Assumptions/Assumption Update {valuation_date_string}.xlsx",
        "excelSheetName": data_table_file_excel_sheet_name
    }
    data_table_id = api_client.create_data_table(data_table_file_path, data_table_parameters)

    data_tables = api_client.list_data_tables(model_id)
    logging.info("Listing Data Tables")
    for d in data_tables:
        logging.info(f"Id: {d['id']}, Name: {d['name']}")

    data_table_id = api_client.update_data_table(data_table_file_path, data_table_parameters)

    # Upload New Inforce Files
    model_point_file_id = api_client.upload_file(model_point_file_path, f"Inforce/Inforce File - {valuation_date_string}.csv")

    # Create Projection from Template
    projection_id = api_client.create_projection_from_template(template_id, f"Valuation {valuation_date_string}")

    # Update Projection Properties
    projection_update_parameters = {
        "startDate": valuation_date.isoformat(),
        "scenarioTableId": scenario_table_id
    }

    api_client.update_projection(projection_id, projection_update_parameters)
    api_client.update_projection_table(projection_id, data_table_name, data_table_id)
    api_client.update_projection_mpf(projection_id, model_point_portfolio_name, model_point_product_name, model_point_file_id)

    # The projection can also be updated in a single call:
    #
    # projection_update_parameters = {
    #    "startDate": valuation_date.isoformat(),
    #    "scenario_table_id": scenario_table_id,
    #    "dataTables": [{
    #        "tableStructureName": "EPA Inputs",
    #        "data_table_id": data_table_id
    #    }],
    #    "portfolios": [{
    #        "portfolioName": "Life Insurance Portfolio",
    #        "products": [{
    #            "productName": "Term Life",
    #            "modelPointFile": {
    #                "fileId": model_point_file_id
    #            }
    #        }]
    #    }]
    # }
    # api_client.update_projection(projection_id, projection_update_parameters)

    # Start Projection and wait for it to finish
    api_client.run_projection(projection_id)
    logging.info("Starting Projection")

    while api_client.is_projection_running(projection_id):
        time.sleep(15)  # Check once every 15 seconds if it is done

    status = api_client.get_projection_status(projection_id)
    logging.info(f"Status: {status}")

    # Download Results
    if status in ["Completed", "CompletedWithErrors"]:
        api_client.download_report(workbook_id, element_id, report_download_file_path_excel, "Excel", {"Projection-ID": f"{projection_id}"})
        api_client.download_report(workbook_id, element_id, report_download_file_path_csv, "Csv", {"Projection-ID": f"{projection_id}"})
