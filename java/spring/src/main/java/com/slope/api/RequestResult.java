package com.slope.api;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;

import java.util.Map;

public record RequestResult(boolean hasError, String response, Map<String, Object> parsedJsonResponse) {
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Mono<RequestResult> FromClientResponse(ClientResponse response) {
        return response.bodyToMono(String.class)
                .flatMap(body -> {
                    Map<String, Object> parsedJsonResponse = null;
                    try {
                        parsedJsonResponse = mapper.readValue(body, Map.class);
                    } catch (JsonProcessingException ignored) {}

                    return Mono.just(new RequestResult(response.statusCode().isError(), body, parsedJsonResponse));
                });
    }
};
