package cs505final.httpcontrollers;

import com.google.gson.Gson;
import cs505final.Launcher;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.Map;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;
    private String id;

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


    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeamName() {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","Ryan's Team");
        responseMap.put("team_members_sids","10706998");
        responseMap.put("app_status_code","1");
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response resetApp() {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        int reset;
        try {
            Launcher.dbEngine.resetDB();
            Launcher.initCEP();             // TODO: Make sure this works.
            reset = 1;
        } catch (Exception ex) {
            reset = 0;
        }
        responseMap.put("reset_status_code",Integer.toString(reset));
        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getZipAlertList() {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();

        /*
        * TODO: Return list of zipcodes in alert status
        * */
        responseMap.put("team_name","RAYN");

       /*
       *
       * Todo, need to query the CEP for zipcodes that meet the criteria and return them
       * */

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response alertOnState() {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();
        responseMap.put("team_name","RAYN");

        /*
        *
        * TODO:
        *   Just state_status:1/0 when at least 5 zips are in alert status
        * */

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/testcount")
    @Produces(MediaType.APPLICATION_JSON)
    public Response stateCounter() {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();

        int positive = Launcher.dbEngine.getPositiveTestCount();
        int negative = Launcher.dbEngine.getNegativeTestCount();

        responseMap.put("positive_test",Integer.toString(positive));
        responseMap.put("negative_test",Integer.toString(negative));

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatient/{mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response searchByMRN(@PathParam("mrn") String mrn) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();

        int hospitalId = Launcher.dbEngine.getPatientLocation(mrn);

        responseMap.put("mrn",mrn);
        responseMap.put("location_code",Integer.toString(hospitalId));

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/gethospital/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response hospitalPatientNumber(@PathParam("id") int hospitalId) {
        String responseString = "{}";
        Map<String,String> responseMap = new HashMap<>();

        int totalBeds = Launcher.dbEngine.getHospitalBedCount(hospitalId);
        int availableBeds = Launcher.dbEngine.getHospitalAvailableBeds(hospitalId);
        int zipcode = Launcher.dbEngine.getHospitalZipCode(hospitalId);

        responseMap.put("total_beds",       Integer.toString(totalBeds));
        responseMap.put("available_beds",   Integer.toString(availableBeds));
        responseMap.put("zipcode",          Integer.toString(zipcode));

        responseString = gson.toJson(responseMap);
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
}
