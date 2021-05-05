package thunder.SurveyOnKafka;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.gson.Gson;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

@Path("api")
public class SurveyAPI {

	static Gson gson = new Gson();
	private static String generic500message = "Something went wrong with our servers, try again shortly";
	static SurveyConsumer consumer;
	private SurveyProducer producer = new SurveyProducer();
	
	static {
		consumer = new SurveyConsumer();
		Thread foo = new Thread(consumer);
		foo.start();
	}
	
	private static HashMap<String, String> getErrObj(final String message) {
		return new HashMap<String, String>() {
			{
				put("error", message);
			}
		};
	}
	
	@POST
	@Path("/initialize")
	@Produces(MediaType.APPLICATION_JSON)
	public Response initialize() {
			
	try {
			Survey s1 = new Survey("Pol", "Ajazi", "2676 Centennial Ct", "Alexandria", "Virginia", "22311", "2024898714",
					"pol.ajazi@yahoo.com", "1618461192994", new ArrayList<String>(Arrays.asList("students", "dorms")),
					"friends", "likely");
			Survey s2 = new Survey("Flavio", "Amurrio", "7710 Kalorama Dr", "Annandale", "Virginia",
					"22003", "2407762442", "famurrio@gmu.edu", "1618461192992",
					new ArrayList<String>(Arrays.asList("students")), "internet", "veryLikely");
			List<Survey> surveys = List.of(s1, s2);
			
			surveys.forEach(survey -> producer.send(survey));
			
			return Response.ok(surveys).build();
		} catch (Exception e) {
			return Response.status(500).entity(getErrObj(e.getMessage())).build();
		}
	}
	
	@GET
	@Path("/survey/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getSurvey(@PathParam("id") int id) {
		System.out.println("\n\nGET by id:"+id);
		try {
			Survey survey = consumer.getSurvey(id);
			if(survey == null)
				return Response.status(404).entity(getErrObj("Survey with id: " + id + " not found")).build();
			return Response.ok(survey).build();
		} catch ( Exception e) {
			System.out.println((new Gson()).toJson(e));
			return Response.status(500).entity(getErrObj(generic500message)).build();
		}
	}
	
	@GET
	@Path("/surveys")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getySurveys() {
		System.out.println("\n\nGET all");
		try {
			return Response.ok(consumer.getSurveys()).build();
		} catch (Exception e) {
			System.out.println((new Gson()).toJson(e));
			return Response.status(500).entity(getErrObj(generic500message)).build();
		}
	}
	
	@POST
	@Path("/survey")
	@Produces(MediaType.APPLICATION_JSON)
	public Response addSurvey(Survey survey) {
		System.out.println("\n\nPOST:");
		System.out.println((new Gson()).toJson(survey));
		try {
			HashMap<String, String> validationResults = ValidateSurvey.validate(survey, null);
			if(validationResults.size() != 0)
				return Response.status(400).entity(validationResults).build();
			
			survey.setId(consumer.generateNewId());
			Survey postSurvey = producer.send(survey);
			
			if(postSurvey == null) throw new Exception("Unable to save survey");
			return Response.ok(postSurvey).build();
		} catch (Exception e) {
			return Response.status(500).entity(getErrObj(generic500message)).build();
		}
	}
	
	@PUT
	@Path("/survey/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response updateSurvey(@PathParam("id") int id, Survey newSurvey) {
		System.out.println("\n\nPUT:");
		System.out.println((new Gson()).toJson(newSurvey));
		try {
			Survey foundSurvey = consumer.getSurvey(id);
			if(foundSurvey == null)
				return Response.status(404).entity(getErrObj("Survey with id: " + id + " not found")).build();
			
			newSurvey.setId(id);
			Survey putSurvey = producer.send(newSurvey);
			
			if(putSurvey == null) throw new Exception("Unable to save survey");
			return Response.ok(putSurvey).build();
		} catch ( Exception e) {
			System.out.println((new Gson()).toJson(e));
			return Response.status(500).entity(getErrObj(generic500message)).build();
		}
	}
}
