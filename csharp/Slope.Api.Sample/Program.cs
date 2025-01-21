using Slope.Api.Sample;

const string rootPath = @"C:\Files\";

// Substitute Real API credentials here
const string apiKey = "<API KEY HERE>";
const string apiSecret = "<API SECRET HERE>";

const int modelId = 99999;  // The ID of the model to be run
const string workbookId = "5rMaW9R0yVoehrIjyAUtew";  // The ID of the workbook with the element to download
const string elementId = "SQ_fXFwL0L";  // The ID of the element in the workbook to download
const int templateId = 99999;  // The ID of the Projection Template to be run

const int tableStructureId = 999999;  // The ID of the table structure of the data table to create
const string dataTableFilePath = $"{rootPath}Assumption Update.xlsx";
const string dataTableFileExcelSheetName = "Assumptions";
const string dataTableName = "Data Table Name";

const string modelPointFilePath = $"{rootPath}Inforce.csv";
const string modelPointPortfolioName = "Portfolio 1";
const string modelPointProductName = "Product A";

const string scenarioTableFilePath = $"{rootPath}Scenario.xlsx";
const string scenarioTableFileExcelSheetName = "Scenarios";
var valuationDate = new DateTime(2021, 1, 31); // The start date of the scenario table

const string reportDownloadFilePathExcel = $"{rootPath}Results.xlsx";
const string reportDownloadFilePathCsv = $"{rootPath}Results.csv";


var apiClient = await SlopeApi.CreateClientAsync(apiKey, apiSecret);


Console.WriteLine("Uploading Data Table File");
var dataTableFileId = await apiClient.UploadFileAsync(dataTableFilePath, $"Assumptions/Assumption Update {valuationDate:yyyy-MM}.xlsx");

Console.WriteLine("Uploading Scenario File");
var scenarioFileId = await apiClient.UploadFileAsync(scenarioTableFilePath, $"Scenario Files/Scenarios {valuationDate:yyyy-MM}.xlsx");

Console.WriteLine("Uploading Inforce File");
var modelPointFileId = await apiClient.UploadFileAsync(modelPointFilePath, $"Inforce/Inforce File - {valuationDate:yyyy-MM}.csv");

/// Example - Using the GetDownloadUrl endpoint to download a file
/// await apiClient.DownloadFileAsync($"Scenario Files/Scenarios {valuationDate:yyyy-MM}.xlsx", "scenario_file.xlsx");

Console.WriteLine("Listing Table Structures");
var tableStructures = await apiClient.ListTableStructuresAsync(modelId);
foreach(var tableStructure in tableStructures)
{
    Console.WriteLine($"Id: {tableStructure.id}, Name: {tableStructure.name}, Description: {tableStructure.description}");
}

Console.WriteLine("Creating Data Table");
var dataTableId = await apiClient.CreateDataTableAsync(tableStructureId,
    dataTableName,
    fileId: dataTableFileId,
    excelSheetName: dataTableFileExcelSheetName);

Console.WriteLine("Listing Data Tables");
var dataTables = await apiClient.ListDataTablesAsync(modelId);
foreach(var dataTable in dataTables)
{
    Console.WriteLine($"Id: {dataTable.id}, Name: {dataTable.name}");
}

Console.WriteLine("Updating Data Table");
dataTableId = await apiClient.UpdateDataTableAsync(
    name: dataTableName,
    tableStructureId: tableStructureId,
    fileId: dataTableFileId,
    excelSheetName: dataTableFileExcelSheetName);


Console.WriteLine("Creating Scenario Table");
var scenarioTableId = await apiClient.CreateScenarioTableAsync(modelId, 
    $"Scenarios - {valuationDate:yyyy-MM}",
    valuationDate,
    "BondEquivalent",
    fileId: scenarioFileId,
    excelSheetName: scenarioTableFileExcelSheetName);


Console.WriteLine("Creating Projection from Template");
var projectionId = await apiClient.CreateProjectionFromTemplateAsync(templateId, "API Test Projection");

Console.WriteLine("Updating Projection Name and Scenario Table");
await apiClient.UpdateProjectionAsync(projectionId, new SlopeApi.UpdateProjectionRequest("API Test Projection Rename", scenarioTableId));

Console.WriteLine("Updating Projection Data Table");
await apiClient.UpdateProjectionDataTableAsync(projectionId, new SlopeApi.UpdateProjectionDataTableAsyncRequest(new []
{
    new SlopeApi.UpdateProjectionTableDataTable(tableStructureId: tableStructureId, dataTableId: dataTableId)
}));

Console.WriteLine("Updating Projection Inforce File");
await apiClient.UpdateProjectionModelPointFileAsync(projectionId, new SlopeApi.UpdateProjectionModelPointFileRequest(new []
{
    new SlopeApi.UpdateProjectionModelPointFilePortfolio(modelPointPortfolioName, new []
    {
        new SlopeApi.UpdateProjectionModelPointFileProduct(modelPointProductName, new SlopeApi.UpdateProjectionModelPointFileModelPointFile(modelPointFileId))
    })
}));


Console.WriteLine("Starting Projection");
await apiClient.RunProjectionAsync(projectionId);

// Wait until the Projection is finished
while (await apiClient.IsProjectionRunningAsync(projectionId))
{
    Console.WriteLine("Waiting for projection to finish...");
    Thread.Sleep(15000); // Check once every 15 seconds if it is done
}

var status = await apiClient.GetProjectionStatusAsync(projectionId);
Console.WriteLine($"Projection finished with status: {status}");


// Download results only if the projection finished running
if (status is "Completed" or "CompletedWithErrors")
{
    Console.WriteLine("Saving Workbook element in Excel format");
    await apiClient.DownloadReportAsync(projectionId, workbookId, elementId, reportDownloadFilePathExcel, "Excel", new Dictionary<string, string>
    {
        {"Time", "0"}
    });

    Console.WriteLine("Saving Workbook element in CSV format");
    await apiClient.DownloadReportAsync(projectionId, workbookId, elementId, reportDownloadFilePathCsv, "Csv", new Dictionary<string, string>
    {
        {"Time", "0"}
    });
}
