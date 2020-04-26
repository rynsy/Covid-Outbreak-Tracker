package cs505final.graph;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import cs505final.Launcher;
import sun.security.provider.certpath.Vertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class GraphEngine {

    private OrientDB orient;
    private ODatabasePool connectionPool;

    private static String databaseHost = "remote:localhost";
    private static String databaseName = "test";
    private static String databaseUserName = "root";
    private static String databasePassword = "rootpwd";

    private static String distanceFile = "./data/kyzipdistance.csv";

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
            zip.createIndex("Zip.zipcode", OClass.INDEX_TYPE.UNIQUE, "zipcode");
        }
        if (db.getClass("Distance") == null) {
            OClass distance = db.createEdgeClass("Distance");
            distance.createProperty("distance", OType.FLOAT);
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

    public int[] adjacent(int zip, int starting_at, int num_results) {
       /*
       *
       * Because of the index, the iterator returns zipcodes from closest to furthest. Returning this in an array in
       * controlled batches so that we don't run an expensive query unnecessarily.
       *
       * starting_at allows you to skip over the first few results if you've already checked them. This may not be the
       * most efficient way of doing things, but I can at least pull in zipcodes in batches this way.
       *
       * */
        ODatabaseSession db = createSession(orient);
        int[] zipcodes = new int[num_results];
        OIndex<?> zipIdx = db.getMetadata().getIndexManager().getIndex("Zip.zipcode");
        OIdentifiable zipV = (OIdentifiable) zipIdx.get(zip);
        OVertex zipVertex = zipV.getRecord();
        Iterator<OEdge> edgeList = zipVertex.getEdges(ODirection.OUT).iterator();
        for(int i = 0; i < starting_at; i++) edgeList.next();
        for (int i = 0; i < num_results; i++) {
            OEdge edge = edgeList.next();
            Integer dest = edge.getTo().getProperty("zipcode");
            Float dist = edge.getProperty("distance");
            zipcodes[i] = dest;
        }
        db.close();
        return zipcodes;
    }

    /*
    *  Pull in the CSV, and then get a list of unique zipcodes (the zipcodes in 'from' and 'to' have the same unique
    * set of zipcodes). Save those as vertices, and then add each edge to the graph.
    *
    * Ideally, this will only happen once, and on database reset we'll just flush the CEP and re-init the relational
    * db.
    * */
    public void loadData() {
        ODatabaseSession db = createSession(orient);
        List<Map<String, String>> zipDistances = Launcher.readCsvData(distanceFile);
        Set<String> uniques = zipDistances.stream().map(o -> o.get("zip_from"))
                .collect(Collectors.toSet());
        Map<String, OVertex> zipVertecies = new HashMap<String, OVertex>();

        for( String zip : uniques) {
            OVertex zipVertex = db.newVertex("Zip");
            zipVertex.setProperty("zipcode", zip);
            zipVertex.save();
            zipVertecies.put(zip, zipVertex);
        }
        for( Map<String, String> d: zipDistances) {
            OVertex from = zipVertecies.get(d.get("zip_from"));
            OVertex to = zipVertecies.get(d.get("zip_to"));
            OEdge connection = from.addEdge(to, "Distance");
            connection.setProperty("distance", d.get("distance"));
            connection.save();
        }
        db.close();
    }

}