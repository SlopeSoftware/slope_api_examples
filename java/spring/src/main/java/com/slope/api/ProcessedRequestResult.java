package com.slope.api;

public record ProcessedRequestResult<T>(RequestResult requestResult, T payload) {}
