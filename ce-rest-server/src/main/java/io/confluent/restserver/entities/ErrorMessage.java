// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class ErrorMessage {
    private final int errorCode;
    private final String message;

    @JsonCreator
    public ErrorMessage(@JsonProperty("error_code") int errorCode, @JsonProperty("message") String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    @JsonProperty("error_code")
    public int errorCode() {
        return errorCode;
    }

    @JsonProperty
    public String message() {
        return message;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ErrorMessage that = (ErrorMessage) o;
        return Objects.equals(errorCode, that.errorCode) &&
            Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorCode, message);
    }
}
