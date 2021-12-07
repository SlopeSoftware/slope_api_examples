package com.slope.api;

public record ProcessedRequestBinaryResult<T>(RequestBinaryResult requestBinaryResult, T payload) {}
