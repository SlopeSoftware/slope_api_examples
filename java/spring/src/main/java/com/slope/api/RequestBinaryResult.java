package com.slope.api;

import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;

public record RequestBinaryResult(boolean hasError, String response, byte[] data) {
    public static Mono<RequestBinaryResult> FromClientResponse(ClientResponse response) {
        return response.bodyToMono(byte[].class)
                .flatMap(data -> {
                    if (data == null) {
                        return Mono.just(new RequestBinaryResult(true, "No data found", null));
                    }

                    return Mono.just(new RequestBinaryResult(false, null, data));
                });
    }
};
