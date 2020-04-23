/*
 Copyright 2020 Confluent Inc.
 */

package io.confluent.rest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

// Needed due to JSON:API spec
@JsonIgnoreProperties(ignoreUnknown = true)
public final class ResponseContainer<T> {
    @JsonProperty(value = "data")
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public final DataResponse<T> data;

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonProperty(value = "errors")
    public final List<ErrorResponse> errors;

    /**
     * Construct a new error response.
     */
    public static ResponseContainer<?> errorResponse(List<ErrorResponse> errors) {
        Objects.requireNonNull(errors, "errors null");
        return new ResponseContainer<>(null, errors);
    }

    /**
     * Construct a new data response.
     */
    public static <T> ResponseContainer<T> dataResponse(T data) {
        Objects.requireNonNull(data, "data null");
        DataResponse<T> dataResponse = new DataResponse<>(data);
        return new ResponseContainer<>(dataResponse, null);
    }

    /**
     * Prefer using errorResponse or dataResponse. This constructor is visible for Jackson only.
     */
    ResponseContainer(@JsonProperty(value = "data") DataResponse<T> data,
                      @JsonProperty(value = "errors") List<ErrorResponse> errors) {
        this.data = data;
        this.errors = errors;
    }

    @Override
    public String toString() {
        return "StatusResponseContainer{" +
                "data=" + data +
                ", errors=" + errors +
                '}';
    }

    /**
     * Returns the status code which should be set for this response.
     */
    private int statusCode() {
        if (errors != null && errors.size() > 0) {
            return errors.get(0).status;
        }
        return HttpServletResponse.SC_OK;
    }

    /**
     * Write the response to the HttpServletResponse, closing the ServletOutputStream when
     * completed.
     */
    public void write(ObjectMapper mapper, HttpServletResponse response) throws IOException {
        response.setContentType("application/vnd.api+json");
        response.setCharacterEncoding("UTF-8");
        response.setStatus(this.statusCode());
        try (final ServletOutputStream outputStream = response.getOutputStream()) {
            mapper.writeValue(outputStream, this);
            outputStream.flush();
        }
    }

    public final static class ErrorResponse {
        @JsonProperty("id")
        public final int id;

        @JsonProperty("status")
        public final int status;

        @JsonProperty("title")
        public final String title;

        public ErrorResponse(@JsonProperty(value = "id") int id,
                             @JsonProperty(value = "status") int status,
                             @JsonProperty(value = "title") String title) {
            this.id = id;
            this.status = status;
            this.title = title;
        }

        @Override
        public String toString() {
            return "ErrorResponse{" +
                    "id=" + id +
                    ", status=" + status +
                    ", title='" + title + '\'' +
                    '}';
        }
    }

    public final static class DataResponse<T> {
        @JsonProperty("attributes")
        public final T attributes;

        DataResponse(@JsonProperty(value = "attributes") T attributes) {
            this.attributes = attributes;
        }
    }
}
