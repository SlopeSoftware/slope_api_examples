import os
import pandas as pd
import logging
import keys, slope_api

solver_tolerance = 0.001
solver_max_iterations = 5

solver_folder = "c:\\Slope Api\\Pricing Solver Solver\\"

reports = {
    "Cash Flows": {"workbook": r'6W5gKlGv2pLW4MwT1UntD8', "element": r'UYyRCGeP3x'},
    "Pricing": {"workbook": r'5RZWZjMiXjdBj7oPDFdBca', "element": r'd-42wNCNrT'},
}

class Solver:
    initial_guess_offset = 0.01                          # Try 1% higher than original projection as first guess
    target_value_column = 'Pricing Metric Target Value'  # Name of the column in the Entity Parameters table tha has the target IRR
    commission_value_column = 'Trail Commission Rate'    # Name of the column in the Trail Commission to change in order to solve for target

    def __init__(self, params: {}):
        self.api = slope_api.SlopeApi()
        self.api.authorize(keys.api_key, keys.api_secret)

        self.projection_id = params["projection_id"]
        self.model_id = params["model_id"]
        self.pricing_target = params["target"]

        self.solver_folder = f"{solver_folder}\\{self.projection_id}\\"
        if not os.path.exists(self.solver_folder):
            os.makedirs(self.solver_folder)
        self.slope_file_path = f"Pricing Solver/{self.projection_id}"

        table_structures = self.api.list_table_structures(self.model_id)
        self.pricing_table_name = params["pricing_table_name"]
        pricing_table_structure = next(table for table in table_structures if table["name"] == params["pricing_table_name"])
        self.pricing_table_structure_id = pricing_table_structure["id"]

        # Set Display preferences for log messages
        pd.set_option('display.max_columns', None)

    def solve(self):
        logging.info("Starting Pricing Solver")
        logging.info(f"  Model ID: {self.model_id}")
        logging.info(f"  Projection ID: {self.projection_id}")
        logging.info(f"  Pricing Target: {self.pricing_target}")

        # Get the completed projection details and check status
        projection_details = self.api.get_projection_details(self.projection_id)
        if projection_details['status'] == 'NotStarted':
            logging.error("Initial projection must be run before the solver is started.")
            logging.error("Current State:")
            logging.error(projection_details)
            return None

        # Get the Pricing Target from the first completed run
        initial_guess_table_id = projection_details["dataTables"][self.pricing_table_name]
        initial_guess_table = self.api.get_data_table_by_id(initial_guess_table_id)
        prior_guess = initial_guess_table.iloc[0]['Pricing Input']
        prior_result = self.__get_result(self.projection_id)
        diff = abs(prior_result - self.pricing_target)
        if diff <= solver_tolerance:
            return prior_guess

        # Run the first guess
        curr_guess = prior_guess + self.initial_guess_offset
        curr_id = self.__start_run(curr_guess)
        curr_result = self.__get_result(curr_id)
        diff = abs(curr_result - self.pricing_table_structure_id)

        # Loop until solved or iteration maximum is hit
        iterations = 0
        while iterations < solver_max_iterations and abs(diff) > solver_tolerance:
            logging.info(f"Tolerance Not met - Guess({curr_guess}) Result({curr_result})")
            prior_diff = prior_result - self.pricing_target
            curr_diff = curr_result - self.pricing_target
            new_guess = curr_guess - curr_diff / ((curr_diff - prior_diff) / (curr_guess - prior_guess))

            prior_guess = curr_guess
            curr_guess = new_guess
            prior_result = curr_result

            # Start new run with new guess
            curr_id = self.__start_run(curr_guess)
            curr_result = self.__get_result(curr_id)
            diff = abs(curr_result - self.pricing_target)

            iterations += 1

        if diff > solver_tolerance:
            logging.warning(f'Hit maximum iterations without solving. Final maximum difference of {diff}.')

        logging.info("Final Result:")
        logging.info(f"  Pricing Guess: {curr_guess}")
        logging.info(f"  Pricing Result: {curr_result}")
        logging.info(f"  Diff from target: {diff}")
        logging.info(f"  Projection: https://app.slopesoftware.com/ModelResults/FinancialProjection/DetailsTabView/{curr_id}")

        return curr_guess

    def __get_result(self, projection_id: int) -> float:
        # Wait for projection to finish
        self.api.wait_for_completion(projection_id)
        status = self.api.get_projection_status(projection_id)
        logging.debug(f"Projection completed with status of '{status}'")
        if status not in ['Completed', 'CompletedWithErrors']:
            logging.error(f'Projection Completed with Status of {status}. Cannot continue solver.')
            raise Exception("Projection did not complete successfully")

        # Get Pricing results
        result_file_name = solver_folder + f"{self.projection_id}/{projection_id}_Pricing_Result.csv"
        report_params = {'Projection-ID': f"{projection_id}"}
        self.api.download_report(reports["Pricing"]["workbook"], reports["Pricing"]["element"], result_file_name, "Csv", report_params)
        pricing_data = pd.read_csv(result_file_name, header=0)
        return pricing_data.iloc[0]['Profit Margin']

    def __start_run(self, pricing_input_guess) -> int:
        # Write new Pricing Input to the table
        pricing_input_file = self.solver_folder + "pricing_guess.csv"
        with open(pricing_input_file, 'w') as file:
            file.write("ID,Pricing Input\n")
            file.write(f",{pricing_input_guess}")

        logging.info(f"Starting run for pricing solve with value: {pricing_input_guess}")

        # Upload new pricing guess to SLOPE
        table_params = {"tableStructureId": self.pricing_table_structure_id,
                        "name": f"Solver for Projection {self.projection_id}",
                        "filePath": f"{self.slope_file_path}/trail_commissions.csv",
                        "isFileOnly": False,
                        "delimiter": ","}
        pricing_table_id = self.api.create_or_update_data_table(pricing_input_file, table_params)

        # Create a new projection from the template
        logging.info(f"Creating new projection")
        projection_id = self.api.copy_projection(self.projection_id, f"Pricing Solve - {self.projection_id}", False)

        # Set starting pricing guess
        self.api.update_projection_table(projection_id, self.pricing_table_name, pricing_table_id)

        # Start the projection
        logging.info(f"Starting projection ID {projection_id}")
        self.api.run_projection(projection_id)
        return projection_id
