package cs505final.database;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;
import cs505final.Launcher;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import javax.xml.transform.Result;
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
        if (!tableExist("hospitals")
                || !tableExist("patients")
                || !tableExist("patient_location")) {
            createDataTables();
            loadData();
        }
    }

    public void resetDB() {
        if (tableExist("patient_location")) {
            dropTable("patient_location");
        }
        if (tableExist("hospitals")) {
            dropTable("hospitals");
        }
        if (tableExist("patients")) {
            dropTable("patients");
        }
        initDB();
    }

    public void loadData() {
        List<Map<String, String>> hospitalData = Launcher.readCsvData(hospitalFile);
        for( Map<String, String> hospital : hospitalData ) {
            insertHospital(hospital);
        }
    }

    public void createDataTables() {
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

        String locationTableCreate = "CREATE TABLE IF NOT EXISTS patient_location" +
                "(" +
                "patient_id bigint," +
                "hospital_id bigint," +
                "FOREIGN KEY (patient_id) REFERENCES patients(id)," +
                "FOREIGN KEY (hospital_id) REFERENCES hospitals (id)" +
                ")";

        Map<String, String> home_assignment = new HashMap<String, String>();
        home_assignment.put("ID", "0");
        home_assignment.put("ZIP", "0");
        home_assignment.put("BEDS", "0");
        home_assignment.put("COUNTYFIPS", "0");
        home_assignment.put("LATITUDE", "0.0");
        home_assignment.put("LONGITUDE", "0.0");
        home_assignment.put("NAICS_CODE", "0");
        Map<String, String> no_assignment = new HashMap<String, String>();
        no_assignment.put("ID", "-1");
        no_assignment.put("ZIP", "0");
        no_assignment.put("BEDS", "0");
        no_assignment.put("COUNTYFIPS", "0");
        no_assignment.put("LATITUDE", "0.0");
        no_assignment.put("LONGITUDE", "0.0");
        no_assignment.put("NAICS_CODE", "0");
        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(hospitalTableCreate);
                    stmt.executeUpdate(patientTableCreate);
                    stmt.executeUpdate(locationTableCreate);
                }
            }
            insertHospital(home_assignment);
            insertHospital(no_assignment);
        } catch(Exception ex) {
            ex.printStackTrace();
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
        executeUpdate(preparedQuery);
    }

    public void insertPatient(String jsonPayload) {
        String query = "INSERT INTO patients " +
                "(" +
                "   first_name, " +
                "   last_name, " +
                "   mrn, " +
                "   zip, " +
                "   patient_status_code " +
                ") " +
                " VALUES (%s,%s,%s,%s,%s)";

        JsonParser parser = new JsonParser();
        JsonElement jsonElem= parser.parse(jsonPayload);
        JsonObject patient = jsonElem.getAsJsonObject();

        System.out.println(patient);
        String preparedQuery = String.format( query,
                patient.get("first_name"),
                patient.get("last_name"),
                patient.get("mrn"),
                patient.get("zip_code"),
                patient.get("patient_status_code")
        );
        ResultSet result = executeQuery(preparedQuery);  // TODO: Get ID of record just inserted
        try {
            int pid = result.getInt("id");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        setPatientLocation("0", "-1"); // Put in patient ID
        /*
        * TODO: Insert patient_location with -1
        *
        * */
    }

    public int getPatientId(String mrn) {
        String query = String.format("SELECT id FROM patients WHERE mrn = %s", mrn);
        ResultSet result = executeQuery(query);
        try {
            return result.getInt("id");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public void setPatientLocation(String patientId, String locationId) {
        int result = -1;
        String query = String.format(
                "INSERT INTO patient_location (patient_id, hospital_id) VALUES (%s, %s)",
                patientId, locationId);
        result = executeUpdate(query);
        if(result != -1)
            System.out.println(result);  //TODO: Check the count?
    }

    public int getHospitalPatientCount(int hospitalId) {
        ResultSet result;
        int patientcount = -1;
        String query = String.format(
                "SELECT count(*) FROM patient_location WHERE hospital_id = %s",
                hospitalId);
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    patientcount = result.getInt(1);
                }
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return patientcount;
    }

    public int getHospitalBedCount(int hospitalId) {
        ResultSet result;
        int bedcount = -1;
        String query = String.format(
                "SELECT beds FROM hospitals WHERE id = %s",
                hospitalId);
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    bedcount = result.getInt(1);
                }
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return bedcount;
    }

    public boolean getHospitalAvailability(int hospitalId) {
        return getHospitalBedCount(hospitalId) > getHospitalPatientCount(hospitalId);
    }

    public List<Integer> findHospitalByZip(int zipcode) {
        /*
        * Return hospitals available in zipcode
        * */
        ResultSet result;
        String query = String.format(
                "SELECT id FROM hospitals WHERE zip = %s",
                zipcode);
        List<Integer> hIds = new ArrayList<>();
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);

                while(result.next()) {
                    hIds.add(result.getInt(1));
                }

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return hIds;
    }

    public int getClosestAvailableHospital(String mrn) {
        /**
         * 1. Look up patient zipcode using mrn
         * 2. Look up adjacent zipcodes through graphengine
         * 3. Check hospital bed number vs. count to determine availability
         */
        int patient_zip = getPatientId(mrn);
        int batch_size = 5;
        int position = 0;
        while(position < 730) { //TODO: Don't hard-code this, but I don't know how else to do this for now
            int[] adjacent_zipcodes = Launcher.graphEngine.adjacent(patient_zip, position, batch_size); // May need to change this number/method to check more locations
            for (int i = 0; i < adjacent_zipcodes.length; i++) {
                List<Integer> hospital_ids = findHospitalByZip(patient_zip);
                for (int id : hospital_ids) {
                    if (getHospitalAvailability(id)) {
                        return id;
                    }
                }
            }
            position += batch_size;
        }
        return -1;
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

    public ResultSet executeQuery(String stmtString) {
        ResultSet result = null;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(stmtString);
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
            result = metadata.getTables(null, null, tableName, null);

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

    public void input(String jsonPayload) {
        insertPatient(jsonPayload);
    }
}
