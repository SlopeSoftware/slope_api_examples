import pricing_solver
import logging
import setup

if __name__ == '__main__':
    # Change this to appropriate level for your run
    setup.setup_logging(logging.INFO)

    solver = pricing_solver.Solver({
        "model_id": 16001,                          # Model ID of the model being run
        "pricing_table_name": "Pricing Input",
        "projection_id": 95944,                     # Projection ID with initial run of the pricing model
        "target": 0.05,                             # Target Pricing Value
    })
    solver.solve()

