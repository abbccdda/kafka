// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.restserver;

import io.confluent.restserver.entities.ErrorMessage;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.ext.ExceptionMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaRestServerExceptionMapper implements ExceptionMapper<Exception> {
    private static final Logger log = LoggerFactory.getLogger(KafkaRestServerExceptionMapper.class);

    @Context
    private UriInfo uriInfo;

    @Override
    public Response toResponse(Exception exception) {
        log.debug("Uncaught exception in REST call to /{}", uriInfo.getPath(), exception);

        final int statusCode;
        if (exception instanceof WebApplicationException) {
            Response.StatusType statusInfo = ((WebApplicationException) exception).getResponse().getStatusInfo();
            statusCode = statusInfo.getStatusCode();
        } else {
            statusCode = Response.Status.INTERNAL_SERVER_ERROR.getStatusCode();
        }
        return Response.status(statusCode)
            .entity(new ErrorMessage(statusCode, exception.getMessage()))
            .build();
    }
}
