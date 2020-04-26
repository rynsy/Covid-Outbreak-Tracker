package cs505final.database;

import com.google.gson.reflect.TypeToken;
import cs505final.Launcher;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DBEngine {

    private DataSource ds;
    private String hospitalFile = "./data/hospitals.csv";
    private String databaseName = "reporting_app";
    private static String databaseUserName = "root";
    private static String databasePassword = "rootpwd";

    /*
    *   TODO: Going to replace this thing with a MySQL database, just to make things easier for me to see/manage
    *
    *   We'll also be able to pre-load data in the same way as the GraphEngine
    *
    * */

    public DBEngine() {
        try {
            String dbConnectionString = "jdbc:mysql://localhost:3306/" + databaseName;
            ds = setupDataSource(dbConnectionString);
            resetDB();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static DataSource setupDataSource(String connectURI) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
        connectionFactory = new DriverManagerConnectionFactory(connectURI, databaseUserName, databasePassword);


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public void initDB() {
        /* TODO Change table schema */
        String hospitalTableCreate = "CREATE TABLE IF NOT EXISTS hospitals" +
                "(" +
                "   id bigint," +
                "   hospital_name varchar(255)," +
                "   address varchar(255)," +
                "   city varchar(255)," +
                "   state varchar(255)," +
                "   zip int," +
                "   type varchar(255)," +
                "   beds int," +
                "   county varchar(255)," +
                "   countyfips int," +
                "   country varchar(255)," +
                "   latitude float," +
                "   longitude float," +
                "   naics_code int," +
                "   website varchar(255)," +
                "   hospital_owner varchar(255)," +
                "   trauma varchar(255)," +
                "   helipad varchar(255)," +
                "   PRIMARY KEY(id)" +
                ")";

        String patientTableCreate = "CREATE TABLE IF NOT EXISTS patients" +
                "(" +
                "   id bigint not null auto_increment," +
                "   first_name varchar(255)," +
                "   last_name varchar(255)," +
                "   mrn varchar(255)," +
                "   zip int," +
                "   patient_status_code int," +
                "   PRIMARY KEY(id)," +
                "   INDEX mrn_idx(mrn)" +
                ")";
        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(hospitalTableCreate);
                    stmt.executeUpdate(patientTableCreate);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        loadData();
    }

    public void resetDB() {
        /* TODO Change table schema
        * Also this should be dropping the database and then calling init. No need to redo everything
        * */
        dropTable("hospitals");
        dropTable("patients");
        initDB();
    }

    public void loadData() {
        List<Map<String, String>> hospitalData = Launcher.readCsvData(hospitalFile);
        /*
        * TODO: Come up with way to insert data. May need to capitalize table fields to make this easier
        *
        * */
        for( Map<String, String> hospital : hospitalData ) {
            insertHospital(hospital);
        }
    }

    public void insertHospital(Map<String, String> record) {
        String query = "INSERT INTO hospitals " +
                "(" +
                "   id ," +
                "   hospital_name ," +
                "   address ," +
                "   city ," +
                "   state ," +
                "   zip ," +
                "   type ," +
                "   beds ," +
                "   county ," +
                "   countyfips ," +
                "   country ," +
                "   latitude , " +
                "   longitude , " +
                "   naics_code ," +
                "   website ," +
                "   hospital_owner ," +
                "   trauma ," +
                "   helipad " +
                ") " +
                " VALUES (\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\",\"%s\")";

        System.out.println(record);
        String preparedQuery = String.format( query,
                record.get("ID"),
                record.get("NAME"),
                record.get("ADDRESS"),
                record.get("CITY"),
                record.get("STATE"),
                record.get("ZIP"),
                record.get("TYPE"),
                record.get("BEDS"),
                record.get("COUNTY"),
                record.get("COUNTYFIPS"),
                record.get("COUNTRY"),
                record.get("LATITUDE"),
                record.get("LONGITUDE"),
                record.get("NAICS_CODE"),
                record.get("WEBSITE"),
                record.get("OWNER"),
                record.get("TRAUMA"),
                record.get("HELIPAD")
        );
        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(preparedQuery);
                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }

    public int insertPatient() {
        /*
        * TODO:
        *   this one might be different. May need to check to see if the patient exists first
        *
        * */
        String query = "";
        return 0;
    }

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int executeInsert(String stmtString) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);  // TODO: Change to insert
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int executeUpdate(String stmtString) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int dropTable(String tableName) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DROP TABLE " + tableName;

                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    /*
    public boolean databaseExist(String databaseName)  {
        return Paths.get(databaseName).toFile().exists();
    }
    */
    public boolean databaseExist(String databaseName)  {
        boolean exist = false;
        try {

            if(!ds.getConnection().isClosed()) {
                exist = true;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean tableExist(String tableName)  {
        boolean exist = false;

        ResultSet result;
        DatabaseMetaData metadata = null;

        try {
            metadata = ds.getConnection().getMetaData();
            result = metadata.getTables(null, null, tableName.toUpperCase(), null);

            if(result.next()) {
                exist = true;
            }
        } catch(SQLException e) {
            e.printStackTrace();
        }

        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }


    /* TODO: won't need this function, but may need something similar */
    public List<Map<String,String>> getAccessLogs() {
        List<Map<String,String>> accessMapList = null;
        try {

            accessMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //fill in the query
            queryString = "SELECT * FROM accesslog";

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("remote_ip", rs.getString("remote_ip"));
                            accessMap.put("access_ts", rs.getString("access_ts"));
                            accessMapList.add(accessMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return accessMapList;
    }


    public void input(String jsonPayload) {

    }
}
