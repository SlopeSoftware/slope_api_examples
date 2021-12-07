package com.slope.api;

import java.util.Date;

public record CreateScenarioTableParameters(int modelId, String description, Date startDate, String yieldCurveRateType, String slopeFilePath, String delimiter){}
