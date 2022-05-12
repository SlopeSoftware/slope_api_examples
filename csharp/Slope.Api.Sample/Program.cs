// See https://aka.ms/new-console-template for more information on the format of this console application.

using Slope.Api.Sample;

// Substitute Real API credentials here
const string apiKey = "990f3347-e5f6-4a55-ab8e-486131e92f77";
const string apiSecret = "b10823cc-2f0b-4788-a82e-3598c6cd6ef5";

const int modelId = 9999;  // The ID of the model to be run
const int reportId = 9999;  // The ID of the Look report to download
const int templateId = 88594;  // The ID of the Projection Template to be run

const int tableStructureId = 9999;  // The ID of the table structure of the data table to create
const string dataTableFilePath = "C:\\Api\\Assumption Update.xlsx";
const string dataTableFileExcelSheetName = "Assumptions";
const string dataTableName = "Data Table Name";

const string modelPointFilePath = "C:\\Api\\Inforce.csv";
const string modelPointPortfolioName = "Portfolio 1";
const string modelPointProductName = "Product A";

const string scenarioTableFilePath = "C:\\Api\\Scenario.csv";
var valuationDate = new DateTime(2021, 1, 31); // The start date of the scenario table

const string reportDownloadFilePathExcel = "C:\\Api\\Results.xlsx";
const string report_download_file_path_csv = "C:\\Api\\Results.csv";

var apiClient = new SlopeApi();

await apiClient.AuthorizeAsync(apiKey, apiSecret);

    /*
// Create Scenario File
var scenarioTableParameters =
{
    "modelId": model_id,
    "description": f"Scenarios {valuation_date_string}",
    "startDate": valuation_date.isoformat(),
    "yieldCurveRateType": "BondEquivalent",
    "filePath": f"Scenario Files/Scenarios {valuation_date_string}.csv",
    "delimiter": ","
};
var scenarioTableId = apiClient.CreateScenarioTable(scenarioTableFilePath, scenarioTableParameters);

// Update Assumptions
var data_table_parameters =
{
    "tableStructureId": table_structure_id,
    "description": f"Assumptions {valuation_date_string}",
    "filePath": f"Assumptions/Assumption Update {valuation_date_string}.xlsx",
    "excelSheetName": data_table_file_excel_sheet_name
};
var dataTableId = apiClient.CreateDataTable(dataTableFilePath, data_table_parameters);
*/
// Upload New Inforce Files
Console.WriteLine("Uploading Inforce File");
var modelPointFileId = await apiClient.UploadFileAsync(modelPointFilePath, $"Inforce/Inforce File - {valuationDate:yyyy-MM}.csv");

// Create Projection from Template
Console.WriteLine("Creating Projection");
var projectionId = await apiClient.CreateProjectionFromTemplateAsync(templateId, $"Valuation {valuationDate:yyyy-MM}");

    /*
// Update Projection Properties
var projection_update_parameters =
{
    "startDate": valuation_date.isoformat(),
    "scenarioTableId": scenario_table_id
};

apiClient.UpdateProjection(projectionId, projection_update_parameters);
apiClient.UpdateProjectionTable(projectionId, dataTableName, data_table_id);
apiClient.UpdateProjectionMpf(projectionId, modelPointPortfolioName, modelPointProductName, model_point_file_id);

// Note - Can Also Update Projection in single call:
//
var projection_update_parameters = {
   "startDate": valuation_date.isoformat(),
   "scenario_table_id": scenario_table_id,
   "dataTables": [{
       "tableStructureName": "EPA Inputs",
       "data_table_id": data_table_id
   }],
   "portfolios": [{
       "portfolioName": "Life Insurance Portfolio",
       "products": [{
           "productName": "Term Life",
           "modelPointFile": {
               "fileId": model_point_file_id
           }
       }]
   }]
}
await apiClient.UpdateProjectionAsync(projectionId, projection_update_parameters);
*/

// Start Projection and wait for it to finish
Console.WriteLine("Starting Projection");
await apiClient.RunProjectionAsync(projectionId);

while (await apiClient.IsProjectionRunningAsync(projectionId))
{
    Thread.Sleep(15000); // Check once every 15 seconds if it is done
}

var status = await apiClient.GetProjectionStatusAsync(projectionId);
Console.WriteLine(status);

//     // Download Results
// if (status == "Completed" || status == "CompletedWithErrors")
// {
//     apiClient.DownloadReport(reportId, report_download_file_path_excel, "Excel",  {
//         "Projection ID": f"{projection_id}"
//     });
//     apiClient.DownloadReport(reportId, report_download_file_path_csv, "Csv",  {
//         "Projection ID": f"{projection_id}"
//     });
// }