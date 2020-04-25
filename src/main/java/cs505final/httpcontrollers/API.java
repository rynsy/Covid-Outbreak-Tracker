package cs505final.httpcontrollers;

import com.google.gson.Gson;
import cs505final.CEP.accessRecord;
import cs505final.Launcher;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    //check local
    //curl --header "X-Auth-API-key:1234" "http://localhost:8082/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8082/api/checkmycep"
    //curl --header "X-Auth-API-key:1234" "http://localhost:8081/api/checkmycep"

    //check remote
    //curl --header "X-Auth-API-key:1234" "http://[linkblueid].cs.uky.edu:8081/api/checkmycep"


    /*
    *  TODO: This may need to drop the auth API key
    * */
    @GET
    @Path("/mf1")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeamName(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
    * TODO this needs to return an actual status. And needs to call all the database components
    *
    * The data that doesn't change doesn't need to be reloaded.
    *
    * You're not going to store patient counts in the graph database (or don't have to), and resetting
    * the app would be much faster if you just loaded the graph database once and then didn't do it again.
    *
    * */
    @GET
    @Path("/mf2")
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetApp(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
     * TODO Does this even need to be here? Maybe the subscriber returns this. Same for other RTR functions
     * */
    @GET
    @Path("/rtr1")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertOnZip(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();  
            //Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent); // NOTE: This is how you'll access the components
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
     * TODO Does this even need to be here? Maybe the subscriber returns this. Same for other RTR functions
     * */
    @GET
    @Path("/rtr2")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertOnState(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
     * TODO Does this even need to be here? Maybe the subscriber returns this. Same for other RTR functions
     * */
    @GET
    @Path("/rtr3")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stateCounter(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
     * TODO Does this even need to be here? Maybe the subscriber returns this. Same for other RTR functions
     * */
    @GET
    @Path("/of1")
    @Produces(MediaType.APPLICATION_JSON)
    public Response routeToHospital(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
     * TODO Does this even need to be here? Maybe the subscriber returns this. Same for other RTR functions
     * */
    @GET
    @Path("/of2")
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchByMRN(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
     * TODO Does this even need to be here? Maybe the subscriber returns this. Same for other RTR functions
     * */
    @GET
    @Path("/of3")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hospitalPatientNumber(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
}
