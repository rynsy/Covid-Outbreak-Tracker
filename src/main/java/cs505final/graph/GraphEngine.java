package cs505final.graph;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import cs505final.Launcher;
import org.glassfish.grizzly.utils.Pair;

import java.util.*;
import java.util.stream.Collectors;

public class GraphEngine {
    private OrientDB orient;
    private ODatabasePool connectionPool;

    private static String databaseVhost = "orientdb";
//    private static String databaseVhost = "localhost";
    private static String databaseHost = "remote:" + databaseVhost;
    private static String databaseName = "test";
    private static String databaseUserName = "root";
    private static String databasePassword = "rootpwd";


    public GraphEngine() {

        try {
            orient = new OrientDB(databaseHost, databaseUserName, databasePassword, OrientDBConfig.defaultConfig() );
            OrientDBConfigBuilder poolCfg = OrientDBConfig.builder();
            poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MIN, 5);
            poolCfg.addConfig(OGlobalConfiguration.DB_POOL_MAX, 10);
            connectionPool = new ODatabasePool(orient, databaseName, databaseUserName, databasePassword, poolCfg.build());
            initDB();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public ODatabaseSession createSession(OrientDB odb) {
        return connectionPool.acquire();
    }

    public void initDB() {
        boolean load = !orient.exists(databaseName);
        orient.createIfNotExists(databaseName, ODatabaseType.PLOCAL);
        ODatabaseSession db = createSession(orient);

        if (db.getClass("Zip") == null) {
            OClass zip = db.createVertexClass("Zip");
            zip.createProperty("zipcode", OType.INTEGER);
            zip.createProperty("has_hospital", OType.BOOLEAN);
            zip.createIndex("Zip.zipcode", OClass.INDEX_TYPE.UNIQUE, "zipcode");
            zip.createIndex("Zip.has_hospital", OClass.INDEX_TYPE.NOTUNIQUE, "has_hospital");
        }
        if (db.getClass("Distance") == null) {
            OClass distance = db.createEdgeClass("Distance");
            distance.createProperty("distance", OType.FLOAT);
            OClass hospital = db.createEdgeClass("Hospital");
            hospital.createProperty("distance", OType.FLOAT);
            hospital.createIndex("Hospital.distance", OClass.INDEX_TYPE.NOTUNIQUE, "distance");
        }
        if (load) loadData();
    }

    public void resetDB() {
        if (orient.exists(databaseName)) {
            orient.drop(databaseName);
        }
        initDB();
    }

    public void input(String jsonPayload) {

    }

    public List<Pair<Integer, Float>> adjacent(int zip) {
       /*
       *
       * Because of the index, the iterator returns zipcodes from closest to furthest. Returning this in an array in
       * controlled batches so that we don't run an expensive query unnecessarily.
       *
       * starting_at allows you to skip over the first few results if you've already checked them. This may not be the
       * most efficient way of doing things, but I can at least pull in zipcodes in batches this way.
       * */
        ODatabaseSession db = createSession(orient);

        List<Pair<Integer, Float>> zipDistances = new ArrayList<Pair<Integer, Float>>();


        OIndex<?> zipIdx = db.getMetadata().getIndexManager().getIndex("Zip.zipcode");
        OIdentifiable zipV = (OIdentifiable) zipIdx.get(zip);
        try {
            Iterator<OEdge> hospitalEdges = ((OVertex) zipV.getRecord()).getEdges(ODirection.OUT, db.getClass("Hospital")).iterator();
            while(hospitalEdges.hasNext()) {
                OEdge edge = hospitalEdges.next();
                OVertex destination = edge.getTo();
                Float distance = edge.getProperty("distance");
                int zipDest = destination.getProperty("zipcode");
                Pair<Integer, Float> entry = new Pair<Integer, Float>(zipDest,distance);
                zipDistances.add(entry);
                if (zipDistances.size() > 5) {
                    Collections.sort(zipDistances, Comparator.comparing(x -> x.getSecond()));
                    zipDistances = zipDistances.subList(0, 5);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        db.close();


        return zipDistances;
    }

    /*
    *  Pull in the CSV, and then get a list of unique zipcodes (the zipcodes in 'from' and 'to' have the same unique
    * set of zipcodes). Save those as vertices, and then add each edge to the graph.
    *
    * Ideally, this will only happen once, and on database reset we'll just flush the CEP and re-init the relational
    * db.
    *
    * Need to look up hospital zipcodes, then check each zip to see if it is in the list. Set that property for each.
    *
    * */
    public void loadData() {
        ODatabaseSession db = createSession(orient);
        List<Map<String, String>> zipDistances = Launcher.readCsvData(Launcher.distanceFile);
        List<Map<String, String>> hospitalData = Launcher.readCsvData(Launcher.hospitalFile);

        Set<String> hospitalZips = hospitalData.stream().map(o -> o.get("ZIP"))
                .collect(Collectors.toSet());

        Set<String> uniques = zipDistances.stream().map(o -> o.get("zip_from"))
                .collect(Collectors.toSet());

        Map<String, OVertex> zipVerticies = new HashMap<String, OVertex>();

        boolean has_hospital = false;

        for( String zip : uniques) {
            OVertex zipVertex = db.newVertex("Zip");
            zipVertex.setProperty("zipcode", zip);
            has_hospital = hospitalZips.contains(zip);
            zipVertex.setProperty("has_hospital", has_hospital);
            zipVertex.save();
            zipVerticies.put(zip, zipVertex);
        }
        for( Map<String, String> d: zipDistances) {
            OVertex from = zipVerticies.get(d.get("zip_from"));
            OVertex to = zipVerticies.get(d.get("zip_to"));

            if (hospitalZips.contains(d.get("zip_to"))){
                OEdge connection = from.addEdge(to, "Hospital");
                connection.setProperty("distance", d.get("distance"));
                connection.save();
            } else {
                OEdge connection = from.addEdge(to, "Distance");
                connection.setProperty("distance", d.get("distance"));
                connection.save();
            }
        }
        db.close();
    }

}