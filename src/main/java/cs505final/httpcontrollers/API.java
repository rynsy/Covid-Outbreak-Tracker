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
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeamName(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetApp(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        responseString = gson.toJson(responseMap);

        Launcher.dbEngine.resetDB();
        /*
        * TODO: Need to reset CEP, but don't need to reset the graph
        *
        * */

        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertOnZip(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();  
            //Launcher.cepEngine.input(Launcher.inputStreamName, inputEvent); // NOTE: This is how you'll access the components
        responseMap.put("team_name","RAYN");

       /*
       *
       * Todo, need to query the CEP for zipcodes that meet the criteria and return them
       * */

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    /*
     * TODO Does this even need to be here? Maybe the subscriber returns this. Same for other RTR functions
     * */
    @GET
    @Path("/alertlist")
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
    @Path("/testcount")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stateCounter(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");

        /*
        * This one I'm unsure about. I believe the CEP for this will be downstream of the other one and listening for
        * Count events from it. Don't know how to chain these CEPs together (yet)
        *
        * */

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/of1")
    @Produces(MediaType.APPLICATION_JSON)
    public Response routeToHospital(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");

        /*
        * TODO: Parse out zip or mrn from request and use it to call the relationalDB
        *
        * */

        int hospitalId = Launcher.dbEngine.getClosestAvailableHospital("");

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatient")
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchByMRN(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");

        /*
        * TODO: Need to parse out mrn from request
        *
        * */

        int hospitalId = Launcher.dbEngine.getPatientLocation("");

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/gethospital")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hospitalPatientNumber(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");
        /*
        * return total_beds, available_beds, zipcode
        *
        * */
        int totalBeds = Launcher.dbEngine.getHospitalBedCount(0);
        int availableBeds = Launcher.dbEngine.getHospitalAvailableBeds(0);
        int zipcode = Launcher.dbEngine.getHospitalZipCode(0);

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
}
