package com.slope.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.Map;

@SpringBootApplication
public class SlopeApiApplication implements CommandLineRunner {
    public static final Logger LOG = LoggerFactory.getLogger(SlopeApiApplication.class);

    public static void main(String[] args) {
        LOG.info("STARTING THE APPLICATION");
        SpringApplication.run(SlopeApiApplication.class, args);
        LOG.info("APPLICATION FINISHED");
    }

    @Override
    public void run(String... args) {
        LOG.info("EXECUTING: Slope API");

        var api = new SlopeApiService();

        api.authorize(Parameters.apiKey, Parameters.apiSecretKey);

        var scenarioTableId = api.createScenarioTable(Parameters.ScenarioTableFilePath,
                new CreateScenarioTableParameters(
                        Parameters.ModelId,
                        "Scenarios " + Parameters.ValuationDateString,
                        Parameters.ValuationDate,
                        YieldCurveRateType.BondEquivalent,
                        "Scenario Files/Scenario.csv",
                        ",")).payload().ScenarioTableId();

        var tableStructures = api.listTableStructures(Parameters.ModelId);
        LOG.info("Listing Table Structures");
        tableStructures.forEach(tableStructure -> {
            LOG.info(MessageFormat.format("Id: {0}, Name: {1}, Description: {2}", tableStructure.id(), tableStructure.name(), tableStructure.descritpion()));
        });

        var dataTableId = api.createDataTable(Parameters.DataTableFilePath,
                new CreateDataTableParameters(
                        Parameters.TableStructureId,
                        "Assumptions " + Parameters.ValuationDateString,
                        "Assumptions/Assumption Update " + Parameters.ValuationDateString + ".xlsx",
                        Parameters.DataTableFileExcelSheetName)).payload().dataTableId();

        var dataTables = api.listDataTables(Parameters.ModelId);
        LOG.info("Listing Data Tables");
        dataTables.forEach(dataTable -> {
            LOG.info(MessageFormat.format("Id: {0}, Name: {1}", dataTable.id(), dataTable.name()));
        });

        var modelPointFileId = api.uploadFile(Parameters.ModelPointFilePath, "Inforce/Inforce.csv")
                .payload().fileId();

        var projectionId = api.createProjectionFromTemplate(Parameters.TemplateId, "Valuation " + Parameters.ValuationDateString)
                .payload().projectionId();

        api.updateProjection(projectionId, Parameters.ValuationDate, scenarioTableId);
        api.updateProjectionTable(projectionId, Parameters.TableStructureName, dataTableId);
        api.updateProjectionModelPointFile(projectionId, Parameters.ModelPointPortfolioName, Parameters.ModelPointProductName, modelPointFileId);

        LOG.info("Starting Projection");

        api.runProjection(projectionId);

        while (api.isProjectionRunning(projectionId).payload().isRunning()) {
            // Check once every 15 seconds if it is done
            try {
                Thread.sleep(15000);
            } catch (InterruptedException ignored){
                break;
            }
        }

        var status = api.getProjectionStatus(projectionId).payload().status();
        LOG.info("Projection Status: " + status);

        if (status.contains("Completed")) {
            try {
                Files.write(Path.of(Parameters.ReportDownloadFilePathExcel),
                        api.downloadReport(Parameters.WorkbookId, Parameters.ElementId, ReportFormatType.Excel, Map.of("Projection-ID", String.valueOf(projectionId))).payload(),
                        StandardOpenOption.CREATE);
            } catch (IOException e) {
                LOG.info("Failed to save Excel report");
            }

            try {
                Files.write(Path.of(Parameters.ReportDownloadFilePathCsv),
                        api.downloadReport(Parameters.WorkbookId, Parameters.ElementId, ReportFormatType.Csv, Map.of("Projection-ID", String.valueOf(projectionId))).payload(),
                        StandardOpenOption.CREATE);
            } catch (IOException e) {
                LOG.info("Failed to save CSV report");
            }
        }
    }
}
