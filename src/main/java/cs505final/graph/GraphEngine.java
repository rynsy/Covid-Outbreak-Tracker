package cs505final.graph;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;

public class GraphEngine {

    private OrientDB orient;
    private ODatabasePool connectionPool;

    private static String databaseHost = "remote:localhost";
    private static String databaseName = "test";
    private static String databaseUserName = "root";
    private static String databasePassword = "rootpwd";

    private String distanceFile = "data/kyzipdistance.csv";

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

    public void loadData() {
        ODatabaseSession db = createSession(orient);
        /*
        * TODO:
        *   Open file
        *   Make a loop to process data
        *   Make a query to insert data
        * */
        db.close();
    }

    public void resetDB() {
        /* TODO: drop all data and custom classes */
        orient.drop(databaseName);
    }

    public void input(String jsonPayload) {

    }
}