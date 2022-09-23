package com.slope.api;

public record CreateDataTableParameters(int tableStructureId, String name, String slopeFilePath, String excelSheetName){}
