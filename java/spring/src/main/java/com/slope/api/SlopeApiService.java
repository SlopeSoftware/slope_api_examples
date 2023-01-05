package com.slope.api;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.netty.ByteBufFlux;
import reactor.netty.http.client.HttpClient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.Map;

public class SlopeApiService {
    private WebClient webClient;

    public SlopeApiService() {
        this.webClient = WebClient.builder()
                .baseUrl("https://api.slopesoftware.com/api/v1")
                .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .build();
    }

    record AuthorizeRequest(String apiKey, String apiSecretKey){}
    record AuthorizeResponse(String accessToken){}
    public ProcessedRequestResult<AuthorizeResponse> authorize(String apiKey, String apiSecretKey) {
         var result = webClient.post()
                .uri("/Authorize")
                .body(Mono.just(new AuthorizeRequest(apiKey, apiSecretKey)), AuthorizeRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                 .block();

        if (result == null || result.hasError() || !result.parsedJsonResponse().containsKey("accessToken"))
        {
            return new ProcessedRequestResult<>(result, null);
        }

        var accessToken = String.valueOf(result.parsedJsonResponse().get("accessToken"));
        this.webClient = this.webClient.mutate().defaultHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken).build();
        return new ProcessedRequestResult<>(result, new AuthorizeResponse(accessToken));
    }

    record GetProjectionStatusResponse(String status){}
    public ProcessedRequestResult<GetProjectionStatusResponse> getProjectionStatus(int projectionId) {
         var result = webClient.get()
                .uri("/Projections/" + projectionId)
                .exchangeToMono(RequestResult::FromClientResponse)
                 .block();

        if (result == null || result.hasError() || !result.parsedJsonResponse().containsKey("status"))
        {
            return new ProcessedRequestResult<>(result, null);
        }

        return new ProcessedRequestResult<>(result, new GetProjectionStatusResponse(String.valueOf(result.parsedJsonResponse().get("status"))));
    }

    record IsProjectionRunningResponse(boolean isRunning){}
    public ProcessedRequestResult<IsProjectionRunningResponse> isProjectionRunning(int projectionId) {
         var result = webClient.get()
                .uri("/Projections/" + projectionId)
                .exchangeToMono(RequestResult::FromClientResponse)
                 .block();

        if (result == null || result.hasError() || !result.parsedJsonResponse().containsKey("isRunning"))
        {
            return new ProcessedRequestResult<>(result, null);
        }

        return new ProcessedRequestResult<>(result, new IsProjectionRunningResponse(Boolean.parseBoolean(String.valueOf(result.parsedJsonResponse().get("isRunning")))));
    }

    record UploadFileRequest(String filePath){}
    record UploadFileResponse(int fileId){}
    public ProcessedRequestResult<UploadFileResponse> uploadFile(String filename, String slopeFilePath) {
        var uploadFileRequest = new UploadFileRequest(slopeFilePath);

        var uploadUrlResult = webClient.post()
                .uri("/Files/GetUploadUrl")
                .body(Mono.just(uploadFileRequest), UploadFileRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (uploadUrlResult == null || uploadUrlResult.hasError() || !uploadUrlResult.parsedJsonResponse().containsKey("uploadUrl"))
        {
            return new ProcessedRequestResult<>(uploadUrlResult, null);
        }

        // Notes - Since this is a direct call to S3 a separate HTTP client is used.
        // Due to the strictness of S3's upload request validation, HttpClient is used to ensure no extra metadata is added.
        var uploadUrl = String.valueOf(uploadUrlResult.parsedJsonResponse().get("uploadUrl"));
        try {
            var fileSize = Files.size(Path.of(filename));
            var response = HttpClient.create()
                    .headers(headers -> headers.add(HttpHeaders.CONTENT_LENGTH, fileSize))
                    .put()
                    .uri(uploadUrl)
                    .send(ByteBufFlux.fromPath(Path.of(filename)))
                    .response()
                    .block();

            if (response == null || response.status().code() != 200) {
                return new ProcessedRequestResult<>(new RequestResult(true, "Failed to upload file to upload URL.", null), null);
            }

        } catch (IOException e) {
            return new ProcessedRequestResult<>(new RequestResult(true, "Could not read file from filepath.", null), null);
        }

        var saveFileUploadResult = webClient.post()
                .uri("/Files/SaveUpload")
                .body(Mono.just(uploadFileRequest), UploadFileRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (saveFileUploadResult == null || saveFileUploadResult.hasError() || !saveFileUploadResult.parsedJsonResponse().containsKey("fileId"))
        {
            return new ProcessedRequestResult<>(uploadUrlResult, null);
        }

        var fileId = Integer.parseInt(String.valueOf(saveFileUploadResult.parsedJsonResponse().get("fileId")));
        return new ProcessedRequestResult<>(saveFileUploadResult, new UploadFileResponse(fileId));
    }

    record CreateDataTableRequest(int tableStructureId, String description, String filePath, String excelSheetName){}
    record CreateDataTableResponse(int dataTableId){}
    public ProcessedRequestResult<CreateDataTableResponse> createDataTable(String filePath, CreateDataTableParameters parameters) {
        var uploadFileResult = uploadFile(filePath, parameters.slopeFilePath());
        if (uploadFileResult.requestResult().hasError()) {
            return new ProcessedRequestResult<>(uploadFileResult.requestResult(), null);
        }

        var result = webClient.post()
                .uri("/DataTables")
                .body(Mono.just(new CreateDataTableRequest(parameters.tableStructureId(), parameters.description(), parameters.slopeFilePath(), parameters.excelSheetName())),
                        CreateDataTableRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (result == null || result.hasError() || !result.parsedJsonResponse().containsKey("id"))
        {
            return new ProcessedRequestResult<>(result, null);
        }

        var dataTableId = Integer.parseInt(String.valueOf(result.parsedJsonResponse().get("id")));
        return new ProcessedRequestResult<>(result, new CreateDataTableResponse(dataTableId));
    }

    record CreateScenarioTableRequest(int modelId, String description, String startDate, String yieldCurveRateType, String filePath, String delimiter){}
    record CreateScenarioTableResponse(int ScenarioTableId){}
    public ProcessedRequestResult<CreateScenarioTableResponse> createScenarioTable(String filePath, CreateScenarioTableParameters parameters) {
        var uploadFileResult = uploadFile(filePath, parameters.slopeFilePath());
        if (uploadFileResult.requestResult().hasError()) {
            return new ProcessedRequestResult<>(uploadFileResult.requestResult(), null);
        }

        var result = webClient.post()
                .uri("/ScenarioTables")
                .body(Mono.just(new CreateScenarioTableRequest(parameters.modelId(), parameters.description(),
                                DateUtils.toShortDateString(parameters.startDate()),parameters.yieldCurveRateType(),
                                parameters.slopeFilePath(), parameters.delimiter())),
                        CreateScenarioTableRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (result == null || result.hasError() || !result.parsedJsonResponse().containsKey("id"))
        {
            return new ProcessedRequestResult<>(result, null);
        }

        var scenarioTableId = Integer.parseInt(String.valueOf(result.parsedJsonResponse().get("id")));
        return new ProcessedRequestResult<>(result, new CreateScenarioTableResponse(scenarioTableId));
    }

    record CreateProjectionFromTemplateRequest(int templateId, String name){}
    record CreateProjectionFromTemplateResponse(int projectionId){}
    public ProcessedRequestResult<CreateProjectionFromTemplateResponse> createProjectionFromTemplate(int templateId, String name) {
        var result = webClient.post()
                .uri("/Projections")
                .body(Mono.just(new CreateProjectionFromTemplateRequest(templateId, name)), CreateProjectionFromTemplateRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (result == null || result.hasError() || !result.parsedJsonResponse().containsKey("id"))
        {
            return new ProcessedRequestResult<>(result, null);
        }

        var projectionId = Integer.parseInt(String.valueOf(result.parsedJsonResponse().get("id")));
        return new ProcessedRequestResult<>(result, new CreateProjectionFromTemplateResponse(projectionId));
    }
    
    record UpdateProjectionRequest(String startDate, int scenarioTableId){}
    record UpdateProjectionResponse(){}
    public ProcessedRequestResult<UpdateProjectionResponse> updateProjection(int projectionId, Date startDate, int scenarioTableId) {
        var result = webClient.patch()
                .uri("/Projections/" + projectionId)
                .body(Mono.just(new UpdateProjectionRequest(DateUtils.toShortDateString(startDate), scenarioTableId)), UpdateProjectionRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (result == null || result.hasError())
        {
            return new ProcessedRequestResult<>(result, null);
        }

        return new ProcessedRequestResult<>(result, new UpdateProjectionResponse());
    }

    record UpdateProjectionModelPointFileModelPointFileDto(int modelPointFileId){}
    record UpdateProjectionModelPointFileProductDto(String productName, UpdateProjectionModelPointFileModelPointFileDto modelPointFile){}
    record UpdateProjectionModelPointFilePortfolioDto(String portfolioName, UpdateProjectionModelPointFileProductDto[] products){}
    record UpdateProjectionModelPointFileRequest(UpdateProjectionModelPointFilePortfolioDto[] portfolios){}
    record UpdateProjectionModelPointFileResponse(){}
    public ProcessedRequestResult<UpdateProjectionModelPointFileResponse> updateProjectionModelPointFile(
            int projectionId,
            String portfolioName,
            String productName,
            int modelPointFileId) {
        var result = webClient.patch()
                .uri("/Projections/" + projectionId)
                .body(Mono.just(new UpdateProjectionModelPointFileRequest(new UpdateProjectionModelPointFilePortfolioDto[]{
                            new UpdateProjectionModelPointFilePortfolioDto(portfolioName, new UpdateProjectionModelPointFileProductDto[]{
                                    new UpdateProjectionModelPointFileProductDto(productName, new UpdateProjectionModelPointFileModelPointFileDto(modelPointFileId)),
                            }),
                        })), UpdateProjectionModelPointFileRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (result == null || result.hasError())
        {
            return new ProcessedRequestResult<>(result, null);
        }

        return new ProcessedRequestResult<>(result, new UpdateProjectionModelPointFileResponse());
    }

    record UpdateProjectionTableDataTableDto(String tableStructureName, int dataTableId){}
    record UpdateProjectionTableRequest(UpdateProjectionTableDataTableDto[] dataTables){}
    record UpdateProjectionTableResponse(){}
    public ProcessedRequestResult<UpdateProjectionTableResponse> updateProjectionTable(int projectionId, String tableName, int dataTableId) {
        var result = webClient.patch()
                .uri("/Projections/" + projectionId)
                .body(Mono.just(new UpdateProjectionTableRequest(new UpdateProjectionTableDataTableDto[]{
                        new UpdateProjectionTableDataTableDto(tableName, dataTableId),
                })), UpdateProjectionTableRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (result == null || result.hasError())
        {
            return new ProcessedRequestResult<>(result, null);
        }

        return new ProcessedRequestResult<>(result, new UpdateProjectionTableResponse());
    }
    
    record RunProjectionRequest(int projectionId){}
    record RunProjectionResponse(){}
    public ProcessedRequestResult<RunProjectionResponse> runProjection(int projectionId) {
        var result = webClient.post()
                .uri("/Projections/" + projectionId + "/run")
                .body(Mono.just(new RunProjectionRequest(projectionId)), RunProjectionRequest.class)
                .exchangeToMono(RequestResult::FromClientResponse)
                .block();

        if (result == null || result.hasError())
        {
            return new ProcessedRequestResult<>(result, null);
        }

        return new ProcessedRequestResult<>(result, new RunProjectionResponse());
    }

    record DownloadReportRequest(String elementId, String reportFormat, Map<String, String> parameters){}
    public ProcessedRequestBinaryResult<byte[]> downloadReport(String workbookId, String elementId, String reportFormatType, Map<String, String> parameters) {
        var result = webClient.post()
                .uri("/Reports/Workbooks/" + workbookId)
                .body(Mono.just(new DownloadReportRequest(elementId, reportFormatType, parameters)), DownloadReportRequest.class)
                .exchangeToMono(RequestBinaryResult::FromClientResponse)
                .block();

        if (result == null || result.hasError())
        {
            return new ProcessedRequestBinaryResult<>(result, null);
        }

        return new ProcessedRequestBinaryResult<>(result, result.data());
    }
}
