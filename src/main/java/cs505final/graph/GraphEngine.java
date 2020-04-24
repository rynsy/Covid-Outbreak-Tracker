package cs505final.graph;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import cs505final.Launcher;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

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
        try {
            orient.create(databaseName, ODatabaseType.PLOCAL);
        } catch (Exception ex) {
            // Database exists, carry on
        }
        ODatabaseSession db = createSession(orient);
        /* Custom classes go here brrrr */
        if (db.getClass("Person") == null) {
            db.createVertexClass("Person");
        }
        if (db.getClass("FriendOf") == null) {
            db.createEdgeClass("FriendOf");
        }
        db.close();
        loadData();
    }

    public void resetDB() {
        orient.drop(databaseName);
        initDB();
    }

    public void input(String jsonPayload) {

    }

    public void loadData() {
        ODatabaseSession db = createSession(orient);
        /*
        * TODO:
        *   Make a loop to process data
        *   Make a query to insert data
        * */
        List<Map<String, String>> zipDistances = Launcher.readCsvData(distanceFile);

        db.close();
    }

}