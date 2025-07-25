using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Slope.Api.Sample;

public class SlopeApi
{
    private const string ApiUrl = "https://api.slopesoftware.com";
    private const string ApiVersion = "v1";

    private readonly HttpClient _httpClient;

    private SlopeApi()
    {
        _httpClient = new HttpClient
        {
            BaseAddress = new Uri(ApiUrl)
        };
    }

    public static async Task<SlopeApi> CreateClientAsync(string apiKey, string apiSecret)
    {
        var apiClient = new SlopeApi();
        await apiClient.AuthorizeAsync(apiKey, apiSecret);

        return apiClient;
    }

    private record AuthorizeRequest(string apiKey, string apiSecretKey);
    private record AuthorizeResponse(string accessToken);
    public async Task AuthorizeAsync(string apiKey, string apiSecretKey)
    {
        var loginRequest = new AuthorizeRequest(apiKey, apiSecretKey);
        var response = await PostAsync<AuthorizeRequest, AuthorizeResponse>(loginRequest, $"/api/{ApiVersion}/Authorize");
        
        _httpClient.DefaultRequestHeaders.Accept.Clear();
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", response.accessToken);
    }
    
    private record GetUploadUrlRequest(string filePath);
    private record GetUploadUrlResponse(string uploadUrl);
    private record SaveUploadRequest(string filePath);
    private record SaveUploadResponse(int fileId);
    public async Task<int> UploadFileAsync(string filePath, string slopePath)
    {
        // Get a temporary direct upload url
        var urlRequest = new GetUploadUrlRequest(slopePath);
        var urlResponse = await PostAsync<GetUploadUrlRequest, GetUploadUrlResponse>(urlRequest, $"/api/{ApiVersion}/Files/GetUploadUrl");

        // Upload file directly to S3
        // Note: Do not use session here - this is a direct call to S3 and does not use the session auth
        using var fileHttpClient = new HttpClient();
        
        var fileResponse = await fileHttpClient.PutAsync(urlResponse.uploadUrl, new StreamContent(File.OpenRead(filePath)));
        await CheckResponseAsync(fileResponse);

        // Tell SLOPE we are done uploading
        var saveRequest = new SaveUploadRequest(slopePath);
        var saveResponse = await PostAsync<SaveUploadRequest, SaveUploadResponse>(saveRequest, $"/api/{ApiVersion}/Files/SaveUpload");
        return saveResponse.fileId;
    }
    
    public record TableStructureItem(int id, string? name, string? description);
    public record TableStructuresPaginatedResponse(List<TableStructureItem>? items, int? offset);
    public async Task<ICollection<TableStructureItem>> ListTableStructuresAsync(int modelId)
    {
        var allItems = new List<TableStructureItem>();
        int? offset = 0;
        do
        {
            var url = $"/api/{ApiVersion}/Models/{modelId}/TableStructures?Limit=200" + (offset > 0 ? $"&Offset={offset}" : "");
            var response = await GetAsync<TableStructuresPaginatedResponse>(url);
            if (response.items != null)
                allItems.AddRange(response.items);
            offset = response.offset;
        } while (offset != null);
        return allItems;
    }

    private record CreateDataTableRequest(int tableStructureId,
        string name,
        int? fileId,
        string? filePath,
        string? excelSheetName,
        string? delimiter);
    private record CreateDataTableResponse(int id);
    public async Task<int> CreateDataTableAsync(int tableStructureId,
        string name,
        int? fileId = null,
        string? slopePath = null,
        string? excelSheetName = null,
        string delimiter = ",")
    {
        var request = new CreateDataTableRequest(tableStructureId, name, fileId, slopePath, excelSheetName, delimiter);
        var response = await PostAsync<CreateDataTableRequest, CreateDataTableResponse>(request, $"/api/{ApiVersion}/DataTables");
        return response.id;
    }
    
    private record UpdateDataTableRequest(
        int? dataTableId,
        int? tableStructureId,
        string? name,
        int? fileId,
        string? filePath,
        string? excelSheetName,
        string? delimiter);
    private record UpdateDataTableResponse(int id);
    public async Task<int> UpdateDataTableAsync(
        int? dataTableId = null,
        int? tableStructureId = null,
        string? name = null,
        int? fileId = null,
        string? slopePath = null,
        string? excelSheetName = null,
        string delimiter = ",")
    {
        var request = new UpdateDataTableRequest(dataTableId, tableStructureId, name, fileId, slopePath, excelSheetName, delimiter);
        var response = await PatchAsync<UpdateDataTableRequest, UpdateDataTableResponse>(request, $"/api/{ApiVersion}/DataTables");
        return response.id;
    }
    
    public record DataTableItem(int id, string? name, int? tableStructureId, string? tableStructureName, int version, bool isLatestVersion);
    public record DataTablesPaginatedResponse(List<DataTableItem>? items, int? offset);
    public async Task<ICollection<DataTableItem>> ListDataTablesAsync(int modelId)
    {
        var allItems = new List<DataTableItem>();
        int? offset = 0;
        do
        {
            var url = $"/api/{ApiVersion}/Models/{modelId}/DataTables?Limit=200" + (offset > 0 ? $"&Offset={offset}" : "");
            var response = await GetAsync<DataTablesPaginatedResponse>(url);
            if (response.items != null)
                allItems.AddRange(response.items);
            offset = response.offset;
        } while (offset != null);
        return allItems;
    }

    private record CreateScenarioTableRequest(int modelId,
        string name,
        string startDate,
        string yieldCurveRateType,
        int? fileId,
        string? filePath,
        string? excelSheetName,
        string? delimiter);
    private record CreateScenarioTableResponse(int id);
    public async Task<int> CreateScenarioTableAsync(int modelId,
        string name,
        DateTime startDate,
        string yieldCurveRateType,
        int? fileId = null,
        string? slopePath = null,
        string? excelSheetName = null,
        string delimiter = ",")
    {
        var request = new CreateScenarioTableRequest(modelId,
            name,
            startDate.Date.ToString("o"),
            yieldCurveRateType,
            fileId,
            slopePath,
            excelSheetName,
            delimiter);
        
        var response = await PostAsync<CreateScenarioTableRequest, CreateScenarioTableResponse>(request, $"/api/{ApiVersion}/ScenarioTables");
        return response.id;
    }

    private record CreateProjectionRequest(int templateId, string name);
    private record CreateProjectionResponse(int id);
    public async Task<int> CreateProjectionFromTemplateAsync(int templateId, string name)
    {
        var request = new CreateProjectionRequest(templateId, name);
        var response = await PostAsync<CreateProjectionRequest, CreateProjectionResponse>(request, $"/api/{ApiVersion}/Projections");
        return response.id;
    }

    public record UpdateProjectionRequest(string? name = null, int? scenarioTableId = null);
    public async Task UpdateProjectionAsync(int projectionId, UpdateProjectionRequest request)
    {
        await PatchAsync(request, $"/api/{ApiVersion}/Projections/{projectionId}");
    }
    
    public record UpdateProjectionDataTableAsyncRequest(UpdateProjectionTableDataTable[] dataTables);
    public record UpdateProjectionTableDataTable(int? tableStructureId = null,
        string? tableStructureName= null,
        int? dataTableId= null,
        string? dataTableName= null,
        int? dataTableVersion= null);
    public async Task UpdateProjectionDataTableAsync(int projectionId, UpdateProjectionDataTableAsyncRequest request)
    {
        await PatchAsync(request, $"/api/{ApiVersion}/Projections/{projectionId}");
    }
    
    public record UpdateProjectionModelPointFileRequest(UpdateProjectionModelPointFilePortfolio[] portfolios);
    public record UpdateProjectionModelPointFilePortfolio(string portfolioName, UpdateProjectionModelPointFileProduct[] products);
    public record UpdateProjectionModelPointFileProduct(string productName, UpdateProjectionModelPointFileModelPointFile modelPointFile);
    public record UpdateProjectionModelPointFileModelPointFile(long fileId);
    public async Task UpdateProjectionModelPointFileAsync(int projectionId, UpdateProjectionModelPointFileRequest request)
    {
        await PatchAsync(request, $"/api/{ApiVersion}/Projections/{projectionId}");
    }

    public async Task RunProjectionAsync(int projectionId)
    {
        var content = new StringContent(string.Empty, Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync($"/api/{ApiVersion}/Projections/{projectionId}/run", content);
        if (!response.IsSuccessStatusCode)
        {
            var errorMessage = $"Failed to start projection with id: {projectionId}\r\n{await response.Content.ReadAsStringAsync()}";
            throw new Exception(errorMessage);
        }
    }

    private record IsProjectionRunningResponse(bool isRunning);
    public async Task<bool> IsProjectionRunningAsync(int projectionId)
    {
        var response = await TryGetAsync<IsProjectionRunningResponse>($"/api/{ApiVersion}/Projections/{projectionId}");
        if (response == null)
        {
            return false;
        }
        
        return response.isRunning;
    }

    private record GetProjectionStatusResponse(string status);
    public async Task<string> GetProjectionStatusAsync(int projectionId)
    { 
        var response = await GetAsync<GetProjectionStatusResponse>($"/api/{ApiVersion}/Projections/{projectionId}?Fields=status");
        return response.status;
    }

    public record DownloadFileRequest(string filePath, int? version = null);
    public record DownloadFileResponse(string downloadUrl);
    public async Task DownloadFileAsync(string slopeFilePath, string outputPath, int? version = null)
    {
        var request = new DownloadFileRequest(slopeFilePath, version);
        var response = await PostAsync<DownloadFileRequest, DownloadFileResponse>(request, $"/api/{ApiVersion}/Files/GetDownloadUrl");

        // Note: Do not use session here - this is a direct call to S3 and does not use the session auth
        using var fileHttpClient = new HttpClient();
        var data = await fileHttpClient.GetByteArrayAsync(response.downloadUrl);
        await System.IO.File.WriteAllBytesAsync(outputPath, data);
    }

    public async Task DownloadReportAsync(int projectionId, string workbookId, string elementId, string fileName, string reportFormat, Dictionary<string, string> parameters, TimeSpan? timeout = null)
    {
        var finalParameters = new Dictionary<string, string>(parameters ?? new Dictionary<string, string>());
        finalParameters["Projection-ID"] = projectionId.ToString();

        var request = new
        {
            elementId = elementId,
            reportFormat = reportFormat,
            parameters = finalParameters
        };
        var content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync($"/api/{ApiVersion}/Reports/Workbooks/{workbookId}/Generate", content);
        await CheckResponseAsync(response);

        var responseJson = await response.Content.ReadAsStringAsync();
        var generation = JsonSerializer.Deserialize<GenerateWorkbookReportResponse>(responseJson);
        if (generation == null || string.IsNullOrEmpty(generation.generationId))
        {
            throw new Exception("Failed to start report generation");
        }

        string? downloadUrl = null;
        string statusUrl = $"/api/{ApiVersion}/Reports/Workbooks/Status/{generation.generationId}";
        var statusTimeout = timeout ?? TimeSpan.FromMinutes(15);
        var startTime = DateTime.UtcNow;
        while (true)
        {
            var statusResponse = await _httpClient.GetAsync(statusUrl);
            await CheckResponseAsync(statusResponse);

            var statusJson = await statusResponse.Content.ReadAsStringAsync();
            var status = JsonSerializer.Deserialize<GetWorkbookReportGenerationStatusResponse>(statusJson) ?? throw new Exception("Failed to get report status");
            if (status.status == "Completed")
            {
                downloadUrl = status.downloadUrl;
                break;
            }
            
            if (status.status == "Failed")
            {
                throw new Exception($"Report generation failed: {status.message}");
            }

            if (DateTime.UtcNow - startTime > statusTimeout)
            {
                throw new TimeoutException("Timed out waiting for report generation to complete");
            }

            await Task.Delay(5000);
        }

        if (string.IsNullOrEmpty(downloadUrl))
        {
            throw new Exception("No download URL returned");
        }

        using var fileHttpClient = new HttpClient();
        var data = await fileHttpClient.GetByteArrayAsync(downloadUrl);
        await File.WriteAllBytesAsync(fileName, data);
    }

    private record GenerateWorkbookReportResponse(string? generationId, string? reportStatusUrl);
    private record GetWorkbookReportGenerationStatusResponse(string status, string? downloadUrl, string? message);

    private async Task CheckResponseAsync(HttpResponseMessage response)
    {
        if (!response.IsSuccessStatusCode)
        {
            Console.WriteLine(await response.Content.ReadAsStringAsync());
        }
        response.EnsureSuccessStatusCode();
    }

    private async Task<TResponse> GetAsync<TResponse>(string url)
    {
        var response = await _httpClient.GetAsync(url);
        await CheckResponseAsync(response);

        var responseJson = await response.Content.ReadAsStringAsync();
        return DeserializeJsonOrThrow<TResponse>(responseJson, url);
    }
    
    private async Task<TResponse?> TryGetAsync<TResponse>(string url) where TResponse : class
    {
        var response = await _httpClient.GetAsync(url);
        if (!response.IsSuccessStatusCode)
        {
            return null;
        }

        var responseJson = await response.Content.ReadAsStringAsync();
        return DeserializeJsonOrThrow<TResponse>(responseJson, url);
    }

    private async Task<TResponse> PostAsync<TRequest, TResponse>(TRequest request, string url)
    {
        var content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync(url, content);
        await CheckResponseAsync(response);

        var responseJson = await response.Content.ReadAsStringAsync();
        return DeserializeJsonOrThrow<TResponse>(responseJson, url);
    }

    private async Task PatchAsync<TRequest>(TRequest request, string url)
    {
        var content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");
        var response = await _httpClient.PatchAsync(url, content);
        await CheckResponseAsync(response);
    }
    
    private async Task<TResponse> PatchAsync<TRequest, TResponse>(TRequest request, string url)
    {
        var content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");
        var response = await _httpClient.PatchAsync(url, content);
        await CheckResponseAsync(response);

        var responseJson = await response.Content.ReadAsStringAsync();
        return DeserializeJsonOrThrow<TResponse>(responseJson, url);
    }

    private TResponse DeserializeJsonOrThrow<TResponse>(string json, string url) =>
        JsonSerializer.Deserialize<TResponse>(json) ?? throw new Exception("Invalid JSON from endpoint: " + url);
}