// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.security.store.MetadataStoreStatus;
import java.util.Objects;

public class StatusValue extends AuthValue {

  private final MetadataStoreStatus status;
  private final int generationId;
  private final Integer writerBrokerId;
  private final String errorMessage;

  @JsonCreator
  public StatusValue(@JsonProperty("status") MetadataStoreStatus status,
                     @JsonProperty("generationId") int generationId,
                     @JsonProperty("writerBrokerId") Integer writerBrokerId,
                     @JsonProperty("errorMessage") String errorMessage) {
    this.status = status;
    this.generationId = generationId;
    this.writerBrokerId = writerBrokerId;
    this.errorMessage = status == MetadataStoreStatus.FAILED && errorMessage == null ?
        "Unknown error" : errorMessage;
  }

  @JsonProperty
  public MetadataStoreStatus status() {
    return status;
  }

  @JsonProperty
  public int generationId() {
    return generationId;
  }

  @JsonProperty
  public Integer writerBrokerId() {
    return writerBrokerId;
  }

  @JsonProperty
  public String errorMessage() {
    return errorMessage;
  }

  @JsonIgnore
  @Override
  public AuthEntryType entryType() {
    return AuthEntryType.STATUS;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StatusValue)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    StatusValue that = (StatusValue) o;

    return Objects.equals(this.status, that.status) &&
        Objects.equals(this.generationId, that.generationId) &&
        Objects.equals(this.writerBrokerId, that.writerBrokerId) &&
        Objects.equals(this.errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return 31 * super.hashCode() + Objects.hash(status, generationId, writerBrokerId, errorMessage);
  }

  @Override
  public String toString() {
    return "StatusValue(" +
        "status=" + status +
        ", generationId=" + generationId +
        ", writerBrokerId=" + writerBrokerId +
        ", errorMessage=" + errorMessage +
        ')';
  }
}
