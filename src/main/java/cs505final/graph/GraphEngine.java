package cs505final.graph;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;

public class GraphEngine {

    private OrientDB orient;

    public GraphEngine() {

        try {
            orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
            ODatabaseSession db = createSession(orient);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public static ODatabaseSession createSession(OrientDB odb) {
        return odb.open("test", "admin", "admin");
    }

    public void initDB() {
        ODatabaseSession db = createSession(orient);
        /* Custom classes go here brrrr */
        if (db.getClass("Person") == null) {
            db.createVertexClass("Person");
        }
        if (db.getClass("FriendOf") == null) {
            db.createEdgeClass("FriendOf");
        }
        db.close();
    }

    public void resetDB() {
        /* TODO: drop all data and custom classes */
        ODatabaseSession db = createSession(orient);
        /* Custom classes go here brrrr */
        if (db.getClass("Person") != null) {
            db.createVertexClass("Person");
        }
        if (db.getClass("FriendOf") != null) {
            db.createEdgeClass("FriendOf");
        }
        db.close();
    }

}