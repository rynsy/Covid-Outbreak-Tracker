package cs505embedded;

import cs505embedded.database.DBEngine;
import cs505embedded.httpfilters.AuthenticationFilter;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;


public class Launcher {

    //sudo docker build -t cs505-embedded .
    //sudo docker run -d --rm -p 8081:8081 cs505-embedded


    public static DBEngine dbEngine;
    public static final String API_SERVICE_KEY = "12463865"; //Change this to your student id
    public static final int WEB_PORT = 8081;

    public static void main(String[] args) throws IOException {

        System.out.println("Starting Database...");
        //Embedded database initialization
        dbEngine = new DBEngine();
        System.out.println("Database Started...");

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
                .packages("cs505embedded.httpcontrollers")
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

}
