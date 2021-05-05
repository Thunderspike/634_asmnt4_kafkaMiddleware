package thunder.SurveyOnKafka;

import jakarta.ws.rs.ProcessingException;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

@Provider
public class GenericExceptionMapper extends Exception implements
ExceptionMapper<ProcessingException>  {
	public static class Error {
        public String key;
        public String message;
    }

	@Override
	public Response toResponse(ProcessingException exception) {
		Error error = new Error();
        error.key = "bad-json";
        error.message = exception.getMessage();
        return Response.status(Status.BAD_REQUEST).entity(error).build();
	}
}
