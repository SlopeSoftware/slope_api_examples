package com.slope.api;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class Parameters {
    public static final String apiKey = "";
    public static final String apiSecretKey = "";

    public static final int ModelId = 9999;  // The ID of the model to be run
    public static final String WorkbookId = "5rMaW9R0yVoehrIjyAUtew";  // The ID of the workbook with the element to download
    public static final String ElementId = "SQ_fXFwL0L";  // The ID of the element in the workbook to download
    public static final int TemplateId = 9999;  // The ID of the Projection Template to be run

    public static final int TableStructureId = 9999;  // The ID of the table structure of the data table to create
    public static final String DataTableFilePath = "C:\\Api\\Assumption Update.xlsx";
    public static final String DataTableFileExcelSheetName = "Assumptions";
    public static final String TableStructureName = "Data Table Name";

    public static final String ModelPointFilePath = "C:\\Api\\Inforce.csv";
    public static final String ModelPointPortfolioName = "Portfolio 1";
    public static final String ModelPointProductName = "Product A";

    public static final String ScenarioTableFilePath = "C:\\Api\\Scenario.csv";
    public static final Date ValuationDate = new GregorianCalendar(2021, Calendar.FEBRUARY, 31).getTime();  // The start date of the scenario table
    public static final String ValuationDateString = new SimpleDateFormat("y-M").format(ValuationDate);

    public static final String ReportDownloadFilePathExcel = "C:\\Api\\Results.xlsx";
    public static final String ReportDownloadFilePathCsv = "C:\\Api\\Results.csv";
}
