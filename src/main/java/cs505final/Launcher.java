package cs505final;

import cs505final.CEP.CEPEngine;
import cs505final.graph.GraphEngine;
import cs505final.database.DBEngine;
import cs505final.Topics.TopicConnector;
import cs505final.httpfilters.AuthenticationFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.*;


public class Launcher {

    public static final String API_SERVICE_KEY = "1234"; //Change this to your student id
    public static final int WEB_PORT = 8082;
    public static String inputStreamName = null;
    public static long accessCount = -1;

    public static TopicConnector topicConnector;

    public static CEPEngine cepEngine = null;
    public static GraphEngine graphEngine = null;
    public static DBEngine dbEngine = null;

   /*
   * TODO: FIX ORANIZATION
   *
   * CEP is initialized in Launcher
   * DB is initialized in constructor
   * GraphDB is initialized in constructor
   *
   * */


    public static void initCEP() {
        System.out.println("Starting CEP...");
        //Embedded database initialization

       /*
       *  TODO: There's going to be two of these things
       *   one is going to be downstream to listen/count alerts
       *
       * */

        cepEngine = new CEPEngine();

        //START MODIFY
        inputStreamName = "PatientInStream";
        String inputStreamAttributesString = "first_name string, last_name string, mrn string, zip_code string, patient_status_code string";

        String outputStreamName = "PatientOutStream";
        String outputStreamAttributesString = "patient_status_code string, count long";


        String queryString = " " +
                "from PatientInStream#window.timeBatch(5 sec) " +
                "select patient_status_code, count() as count " +
                "group by patient_status_code " +
                "insert into PatientOutStream; ";

        //END MODIFY

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString, outputStreamAttributesString, queryString);

        System.out.println("CEP Started...");
    }

    public static void initGraphDb() {
        System.out.println("Initializing OrientDB...");

        graphEngine = new GraphEngine();

        System.out.println("OrientDB Started...");
    }
    
    public static void initDb() {
        System.out.println("Initializing RelationalDB...");

        dbEngine = new DBEngine();

        System.out.println("RelationalDB Started...");
    }

    public static void main(String[] args) throws IOException {

        initCEP();
        initGraphDb();
        initDb();

        //starting Collector
        topicConnector = new TopicConnector();
        topicConnector.connect();

        //Embedded HTTP initialization
        startServer();


        try {
            while (true) {
                Thread.sleep(5000);
            }
        }catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig()
        .packages("cs505cep.httpcontrollers")
        .register(AuthenticationFilter.class);

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static List<Map<String, String>> readCsvData(String filename)  {
        List<Map<String, String>> response = new LinkedList<Map<String,String>>();
        FileInputStream dataFile = null;
        try {
            dataFile = new FileInputStream(filename);
        } catch(FileNotFoundException ex) {
            System.out.println("Couldn't open file: " + filename);
        }
        Scanner sc = new Scanner(dataFile);
        String[] header = sc.nextLine().split(",");
        String fixed_header;
        for (int i = 0; i < header.length; i++){
            fixed_header = header[i].replaceAll("(\\W)", ""); //Removing illegal chars
            header[i] = fixed_header;
        }
        while (sc.hasNextLine()) {
            String[] dataLine = sc.nextLine().split(",");
            Map<String, String> line = new HashMap<String, String>();

            /*
            *  Fix for entries in the CSV that contain commas
            * */
            if (dataLine[0].equals("11640536") || dataLine[0].equals("5342025") || dataLine[0].equals("8742642")) {
                int h = 0;
                for (int i = 0; i < dataLine.length; i++) {
                    if ( (dataLine[0].equals("11640536")  && header[h].contains("TRAUMA"))
                            || (dataLine[0].equals("5342025") && header[h].contains("ADDRESS"))
                            || (dataLine[0].equals("8742642") && header[h].contains("ADDRESS"))) {
                        String fixed_line = new StringBuilder()
                                .append(dataLine[i].replaceAll("\"", "")).append(",")
                                .append(dataLine[i+1].replaceAll("\"", "")).toString();
                        line.put(header[h], fixed_line);
                        i += 1;
                    } else {
                        line.put(header[h], dataLine[i]);
                    }
                    h++;
                }
            } else {
                for (int i = 0; i < header.length; i++) {
                    line.put(header[i], dataLine[i]);
                }
            }
            response.add(line);
        }
        return response;
    }
}
