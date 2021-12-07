package com.slope.api;

public record CreateDataTableParameters(int tableStructureId, String description, String slopeFilePath, String excelSheetName){}
