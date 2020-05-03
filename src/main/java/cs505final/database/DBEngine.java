package cs505final.database;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import cs505final.Launcher;
import javafx.util.Pair;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class DBEngine {
    private DataSource ds;
    private String databaseName = "reporting_app";
//    private static String databaseVhost = "mysql";
    private static String databaseVhost = "localhost";
    private static String databaseUserName = "root";
    private static String databasePassword = "rootpwd";

    public DBEngine() {
        try {
            String dbConnectionString = "jdbc:mysql://" + databaseVhost + ":3306/" + databaseName;
            ds = setupDataSource(dbConnectionString);
            initDB();
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
        if(tableExist("patient_location"))
            dropTable("patient_location");
        if(tableExist("patients"))
            dropTable("patients");
        if(tableExist("hospitals"))
            dropTable("hospitals");
        createDataTables();
        loadData();
    }

    public void reset() {
        deleteDataFromTable("patient_location");
        deleteDataFromTable("patients");
    }

    public void loadData() {
        List<Map<String, String>> hospitalData = Launcher.readCsvData(Launcher.hospitalFile);
        for( Map<String, String> hospital : hospitalData ) {
            insertHospital(hospital);
        }
    }

    public void createDataTables() {
        String hospitalTableCreate = "CREATE TABLE hospitals" +
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

        String patientTableCreate = "CREATE TABLE patients" +
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

    public boolean patientNeedsHospital(int statusCode) {
        return statusCode == 3 || statusCode == 5 || statusCode == 6;
    }

    public boolean patientCritical(int statusCode) {
        return statusCode == 6;
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

        String insertPatientQuery = String.format( query,
                patient.get("first_name"),
                patient.get("last_name"),
                patient.get("mrn"),
                patient.get("zip_code"),
                patient.get("patient_status_code")
        );
        String selectPatientIdQuery = String.format("" +
                 "SELECT id FROM patients WHERE mrn = %s", patient.get("mrn"));
        int pid = -1, result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(insertPatientQuery);
                if (result > 0) {
                    ResultSet idResult = stmt.executeQuery(selectPatientIdQuery);
                    while (idResult.next()) {
                        pid = idResult.getInt(1);
                    }
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
        // TODO: Get it to insert a patient and update this table
        if (pid > 0) {
            int statusCode =  patient.get("patient_status_code").getAsInt();
            if (!patientNeedsHospital(statusCode)) {
                setPatientLocation(pid, 0); // Put in patient ID
            } else {
                int hid = getClosestAvailableHospital(patient.get("mrn").getAsString());
                setPatientLocation(pid, hid); // Put in patient ID
            }
        }
    }

    public int getPatientZip(String mrn) {
        String query = String.format("SELECT zip FROM patients WHERE mrn = '%s'", mrn);
        int patientZip = -1;
        ResultSet result;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    patientZip = result.getInt(1);
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
        return patientZip;
    }

    public int setPatientLocation(int patientId, int locationId) {
        int result = -1;
        String query = String.format(
                "INSERT INTO patient_location (patient_id, hospital_id) VALUES (%s, %s)",
                patientId, locationId);
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(query);
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

    public int getPatientLocation(String mrn) {
        ResultSet result;
        int patientLocationId = -1;
        String query = String.format(
                "SELECT hospital_id FROM patient_location JOIN patients ON id = patient_id WHERE mrn = \"%s\"",
                mrn);
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    patientLocationId = result.getInt(1);
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
        return patientLocationId;
    }

    public int getPatientStatus(String mrn) {
        ResultSet result;
        int patientStatusCode = -1;
        String query = String.format(
                "SELECT patient_status_code FROM patients WHERE mrn = \"%s\"",
                mrn);
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    patientStatusCode = result.getInt(1);
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
        return patientStatusCode;
    }

    public int getPositiveTestCount() {
        ResultSet result;
        String query = "SELECT count(*) FROM patients WHERE patient_status_code IN (2,5,6)";
        int testCount = 0;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    testCount = result.getInt(1);
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
        return testCount;
    }

    public int getNegativeTestCount() {
        ResultSet result;
        String query = "SELECT count(*) FROM patients WHERE patient_status_code IN (1,4)";
        int testCount = 0;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    testCount = result.getInt(1);
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
        return testCount;
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

    public int getHospitalZipCode(int hospitalId) {
        ResultSet result;
        int zipcode = -1;
        String query = String.format(
                "SELECT zip FROM hospitals WHERE id = %s",
                hospitalId);
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeQuery(query);
                while (result.next()) {
                    zipcode = result.getInt(1);
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
        return zipcode;
    }

    public boolean getHospitalAvailability(int hospitalId) {
        return getHospitalBedCount(hospitalId) > getHospitalPatientCount(hospitalId);
    }

    public int getHospitalAvailableBeds(int hospitalId) {
        return getHospitalBedCount(hospitalId) - getHospitalPatientCount(hospitalId);
    }

    public List<Integer> findHospitalByZip(int zipcode, boolean high_level) {
        /*
        * Return hospitals available in zipcode
        * */
        ResultSet result;
        String query;
        if (high_level) {
            query = String.format(
                    "SELECT id FROM hospitals WHERE zip = %s AND trauma LIKE '%%LEVEL%%'",
                    zipcode);
        } else {
            query = String.format(
                    "SELECT id FROM hospitals WHERE zip = %s",
                    zipcode);
        }
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
         *
         * Retrieves a batch of zipcodes and checks hospitals in each zipcode for availability
         *
         * A lot can be done to speed this up. The traversal could all be offloaded to the graphdatabase, but that would
         * require storing more information there. Right now it just has the distances between zips and hospitals.
         */
        int patient_zip = getPatientZip(mrn);
        int patient_status = getPatientStatus(mrn);
        boolean high_level_facility = patientCritical(patient_status);
        List<Integer> hospital_ids = findHospitalByZip(patient_zip, high_level_facility);

        if (hospital_ids.size() > 0) {
            for (int id : hospital_ids) {
                if (getHospitalAvailability(id)) {
                    return id;
                }
            }
        }

        List<Pair<Integer, Float>> adjacent_zipcodes = Launcher.graphEngine.adjacent(patient_zip);
        if (adjacent_zipcodes.size() <= 0) {
            return -1;
        }
        for (Pair<Integer,Float> entry : adjacent_zipcodes) {
            int zip = entry.getKey();
            hospital_ids = findHospitalByZip(zip, high_level_facility);
            for (int id : hospital_ids) {
                if (getHospitalAvailability(id)) {
                    return id;
                }
            }
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

                stmtString = "DROP TABLE IF EXISTS " + tableName;

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

    public int deleteDataFromTable(String tableName) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DELETE FROM " + tableName + " WHERE 1=1";

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
        if (Launcher.appAvailable) {
            insertPatient(jsonPayload);
        }
    }
}
