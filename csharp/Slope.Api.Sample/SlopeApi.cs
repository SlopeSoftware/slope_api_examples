using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Slope.Api.Sample;

public class SlopeApi
{
    private const string ApiUrl = "https://api.slopesoftware.com";
    private const string ApiVersion = "v1";

    private readonly HttpClient _httpClient;

    public SlopeApi()
    {
        _httpClient = new HttpClient()
        {
            BaseAddress = new Uri(ApiUrl)
        };
    }

    private async Task CheckResponseAsync(HttpResponseMessage response)
    {
        if (!response.IsSuccessStatusCode)
        {
            Console.WriteLine(await response.Content.ReadAsStringAsync());
        }
        response.EnsureSuccessStatusCode();
    }

    private async Task<TResponse> PostAsync<TRequest, TResponse>(TRequest request, string url)
    {
        var content = new StringContent(JsonSerializer.Serialize(request), Encoding.UTF8, "application/json");
        var response = await _httpClient.PostAsync(url, content);
        await CheckResponseAsync(response);

        var responseJson = await response.Content.ReadAsStringAsync();
        return JsonSerializer.Deserialize<TResponse>(responseJson);
    }

    private record AuthorizeRequest(string apiKey, string apiSecretKey);
    private record AuthorizeResponse(string accessToken);
    public async Task AuthorizeAsync(string apiKey, string apiSecretKey)
    {
        var loginParams = new AuthorizeRequest(apiKey, apiSecretKey);
        var response = await PostAsync<AuthorizeRequest, AuthorizeResponse>(loginParams, $"/api/{ApiVersion}/Authorize");
        
        _httpClient.DefaultRequestHeaders.Accept.Clear();
        _httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
        _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", response.accessToken);
    }

    /*
    public async Task<int> UploadFileAsync(string fileName, string slopePath)
    {
        var slope_file_params = {"filePath": slope_path}
        var response = self.session.post(f"{self.api_url}/Files/GetUploadUrl", json=slope_file_params)
        uploadUrl = response.json()["uploadUrl"]
        
        // Note - Do not use session here - this is a direct call to s3 and does not use the session auth
        var response = requests.put(uploadUrl, data=open(filename, "rb"))
        await CheckResponseAsync(response);

        var response = self.session.post(f"{self.api_url}/Files/SaveUpload", json=slope_file_params)
        await CheckResponseAsync(response);
        return response.json()["fileId"]
    }

    public async Task CreateDataTable(string fileName, slopeTableParams)
    {
        self.upload_file(filename, slope_table_params["filePath"])
        var response = self.session.post(f"{self.api_url}/DataTables", json=slope_table_params)
        await CheckResponseAsync(response);
        return response.json()["id"]
    }

    public async Task CreateScenarioTable(string fileName, slopeScenarioTableParams)
    {
        self.upload_file(filename, slope_scenario_table_params["filePath"])
        var response = self.session.post(f"{self.api_url}/ScenarioTables", json=slope_scenario_table_params)
        await CheckResponseAsync(response);
        return response.json()["id"]
    }
    */

    private record CreateProjectionRequest(int templateId, string name);
    private record CreateProjectionResponse(int id);
    public async Task<int> CreateProjectionFromTemplateAsync(int templateId, string name)
    {
        var request = new CreateProjectionRequest(templateId, name);
        var response = await PostAsync<CreateProjectionRequest, CreateProjectionResponse>(request, $"/api/{ApiVersion}/Projections");
        return response.id;
    }
        
    /*
    public async Task UpdateProjection(int projection_id, properties)
    {
        var response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json = properties)
        await CheckResponseAsync(response);
    }

    public async Task UpdateProjectionMpf(projection_id, portfolio_name, product_name, model_point_file_id)
    {
        projection_update_parameters = {
            "portfolios": [{
                "portfolioName": portfolio_name,
                "products": [{
                    "productName": product_name,
                    "modelPointFile": {
                        "fileId": model_point_file_id
                    }
                }]
            }]        
        }
        var response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=projection_update_parameters)
        await CheckResponseAsync(response);
    }

    public async Task UpdateProjectionTable(projection_id, table_name, data_table_id)
    {
        projection_update_parameters = {
            "dataTables": [{
                "tableStructureName": table_name,
                "dataTableId": data_table_id
            }]      
        }
        var response = self.session.patch(f"{self.api_url}/Projections/{projection_id}", json=projection_update_parameters)
        await CheckResponseAsync(response);
    }*/

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
        var response = await _httpClient.GetAsync($"/api/{ApiVersion}/Projections/{projectionId}");
        if (!response.IsSuccessStatusCode)
        {
            return false;
        }

        var responseJson = await response.Content.ReadAsStringAsync();
        var deserializedResponse = JsonSerializer.Deserialize<IsProjectionRunningResponse>(responseJson);
        return deserializedResponse.isRunning;
    }

    private record GetProjectionStatusResponse(string status);
    public async Task<string> GetProjectionStatusAsync(int projectionId)
    { 
        var response = await _httpClient.GetAsync($"/api/{ApiVersion}/Projections/{projectionId}");
        await CheckResponseAsync(response);
        var responseJson = await response.Content.ReadAsStringAsync();
        var deserializedResponse = JsonSerializer.Deserialize<GetProjectionStatusResponse>(responseJson);
        return deserializedResponse.status;
    }

    /*
    public async Task DownloadReport(int lookId, string fileName, string formatType, object filters)
    {
        report_params = {
            "reportFormat": formatType,
            "filters": filters
        }
        var response = self.session.post(f"{self.api_url}/Reports/Looks/{lookId}", json=report_params)
        await CheckResponseAsync(response);
        file = open(filename, "wb")
        file.write(response.content)
        file.close()
    }*/
}