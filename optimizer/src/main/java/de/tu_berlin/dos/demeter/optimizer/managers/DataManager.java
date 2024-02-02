package de.tu_berlin.dos.demeter.optimizer.managers;

import de.tu_berlin.dos.demeter.optimizer.utils.CheckedConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class DataManager {

    public static class Profile {

        public Integer expId;
        public String genType;
        public String jobName;
        public int scaleOut;
        public int chkInt;
        public double cpuVal;
        public int memVal;
        public int taskSlots;
        public double avgThr;
        public double avgLat;
        public double recTime;
        public Long startTs;
        public Long stopTs;

        public Profile(
                Integer expId, String genType, String jobName, int scaleOut, int chkInt, double cpuVal, int memVal,
                int taskSlots, double avgThr, double avgLat, double recTime, Long startTs, Long stopTs) {

            this.expId = expId;
            this.genType = genType;
            this.jobName = jobName;
            this.scaleOut = scaleOut;
            this.chkInt = chkInt;
            this.cpuVal = cpuVal;
            this.memVal = memVal;
            this.taskSlots = taskSlots;
            this.avgThr = avgThr;
            this.avgLat = avgLat;
            this.recTime = recTime;
            this.startTs = startTs;
            this.stopTs = stopTs;
        }

        @Override
        public String toString() {
            return "{" +
                    "expId:" + expId +
                    ",genType:'" + genType + '\'' +
                    ",jobName:'" + jobName + '\'' +
                    ",scaleOut:" + scaleOut +
                    ",chkInt:" + chkInt +
                    ",cpuVal:" + cpuVal +
                    ",memVal:" + memVal +
                    ",taskSlots:" + taskSlots +
                    ",avgThr:" + avgThr +
                    ",avgLat:" + avgLat +
                    ",recTime:" + recTime +
                    ",startTs:" + startTs +
                    ",stopTs:" + stopTs +
                    '}';
        }
    }

    public record Prediction(int expId, String genType, long timestamp, int scaleOut, double avgThr, double avgLat, double recTime) {

        @Override
        public String toString() {
            return "{" +
                    "expId:" + expId +
                    ",genType:" + genType +
                    ",timestamp:" + timestamp +
                    ",scaleOut:" + scaleOut +
                    ",avgThr:" + avgThr +
                    ",avgLat:" + avgLat +
                    ",recTime:" + recTime +
                    '}';
        }
    }

    /******************************************************************************
     * STATIC VARIABLES
     ******************************************************************************/

    private static final Logger LOG = LogManager.getLogger(DataManager.class);
    private static final String DB_FILE_NAME = "demeter";

    /******************************************************************************
     * STATIC BEHAVIOURS
     ******************************************************************************/

    public static DataManager create() {

        return new DataManager();
    }

    private static Connection connect() throws Exception {

        Class.forName("org.sqlite.JDBC");
        return DriverManager.getConnection(String.format("jdbc:sqlite:%s.db", DB_FILE_NAME));
    }

    private static void executeUpdate(String query) {

        try (Connection conn = connect();
             Statement statement = conn.createStatement()) {

            statement.executeUpdate(query);
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    private static void executeQuery(String query, CheckedConsumer<ResultSet> callback) {

        try (Connection connection = connect();
             Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(query)) {

            while (resultSet.next()) {

                callback.accept(resultSet);
            }
        }
        catch (Exception e) {

            LOG.error(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    /******************************************************************************
     * CONSTRUCTOR(S)
     ******************************************************************************/

    private DataManager() { }

    /******************************************************************************
     * INSTANCE BEHAVIOUR
     ******************************************************************************/

    public void initProfiles(int expId, String genType, boolean removePrevious) {

        String createTable =
            "CREATE TABLE IF NOT EXISTS profiles " +
            "(expId INTEGER NOT NULL, " +
            "genType TEXT NOT NULL, " +
            "jobName TEXT NOT NULL, " +
            "scaleOut INTEGER NOT NULL, " +
            "chkInt INTEGER NOT NULL, " +
            "cpuVal REAL NOT NULL, " +
            "memVal INTEGER NOT NULL, " +
            "taskSlots INTEGER NOT NULL, " +
            "avgThr REAL NOT NULL, " +
            "avgLat REAL NOT NULL, " +
            "recTime REAL NOT NULL, " +
            "startTs INTEGER NOT NULL, " +
            "stopTs INTEGER NOT NULL);";
        DataManager.executeUpdate(createTable);
        if (removePrevious)
            DataManager.executeUpdate(String.format(
                "DELETE FROM profiles " +
                "WHERE expId = %d " +
                "AND genType = '%s';",
            expId, genType));
    }

    public void addProfile(Profile profile
            /*Integer expId, String genType, String jobName, int scaleOut, int chkInt, double cpuVal, int memVal,
            int taskSlots, double avgThr,  double avgLat, double recTime, Long startTs, Long stopTs*/) {

        String insertValue = String.format(
                "INSERT INTO profiles ( " +
                "expId, genType, jobName, scaleOut, chkInt, cpuVal, memVal, taskSlots, avgThr, avgLat, recTime, startTs, stopTs ) " +
                "VALUES (" +
                "%d, '%s', '%s', %d, %d, %f, %d, %d, %f, %f, %f, %d, %d);",
                profile.expId, profile.genType, profile.jobName, profile.scaleOut,
                profile.chkInt, profile.cpuVal, profile.memVal, profile.taskSlots,
                profile.avgThr, profile.avgLat, profile.recTime, profile.startTs, profile.stopTs);
        DataManager.executeUpdate(insertValue);
    }

    public void updateRecTime(int expId, String genType, String jobName, double recTime, long stopTs) {

        String updateValue = String.format(
            "UPDATE profiles " +
            "SET recTime = %f " +
            "WHERE expId = %d " +
            "AND genType = '%s' " +
            "AND jobName = '%s' " +
            "AND stopTs = %d;",
            recTime, expId, genType, jobName, stopTs);
        DataManager.executeUpdate(updateValue);
    }

    public List<Profile> getProfiles(int expId, String genType) {

        List<Profile> profiles = new ArrayList<>();
        String selectValues = String.format(
                "SELECT " +
                "expId, genType, jobName, scaleOut, chkInt, cpuVal, memVal, taskSlots, avgThr, avgLat, recTime, startTs, stopTs " +
                "FROM profiles " +
                "WHERE expId IN (%s) " +
                "AND genType = '%s' " +
                "ORDER BY scaleOut ASC, stopTs ASC;",
            expId, genType);
        DataManager.executeQuery(selectValues, (rs) -> {
            profiles.add(
                new Profile(
                    rs.getInt("expId"),
                    rs.getString("genType"),
                    rs.getString("jobName"),
                    rs.getInt("scaleOut"),
                    rs.getInt("chkInt"),
                    rs.getDouble("cpuVal"),
                    rs.getInt("memVal"),
                    rs.getInt("taskSlots"),
                    rs.getDouble("avgThr"),
                    rs.getDouble("avgLat"),
                    rs.getDouble("recTime"),
                    rs.getLong("startTs"),
                    rs.getLong("stopTs")));
        });
        return profiles;
    }

    public DataManager deleteRow(int expId, String genType) {

        DataManager.executeUpdate(String.format("DELETE FROM profiles WHERE expId = %d AND genType = '%s';", expId, genType));
        return this;
    }

    /**********************************************************************/

    public void initPredictions(int expId, String genType, boolean removePrevious) {

        String createTable =
            "CREATE TABLE IF NOT EXISTS predictions " +
            "(expId INTEGER NOT NULL, " +
            "genType TEXT NOT NULL, " +
            "timestamp INTEGER NOT NULL, " +
            "scaleOut INTEGER NOT NULL, " +
            "avgThr INTEGER NOT NULL, " +
            "avgLat REAL NOT NULL, " +
            "recTime REAL NOT NULL);";
        DataManager.executeUpdate(createTable);
        if (removePrevious) DataManager.executeUpdate(String.format("DELETE FROM predictions WHERE expId = %d AND genType = '%s';", expId, genType));
    }

    public void addPrediction(int expId, String genType, long timestamp, int scaleOut, double avgThr, double avgLat, double recTime) {

        String insertValue = String.format(
            "INSERT INTO predictions ( " +
            "expId, genType, timestamp, scaleOut, avgThr, avgLat, recTime) " +
            "VALUES (" +
            "%d, '%s', %d, %d, %f, %f, %f);",
            expId, genType, timestamp, scaleOut, avgThr, avgLat, recTime);
        DataManager.executeUpdate(insertValue);
    }

    public List<Prediction> getPredictions(List<Integer> expIds) {

        List<Prediction> predictions = new ArrayList<>();
        String selectValues = String.format(
                "SELECT " +
                "expId, genType, timestamp, scaleOut, avgThr, avgLat, recTime " +
                "FROM predictions " +
                "WHERE expId IN (%s) " +
                "ORDER BY expId ASC, timestamp ASC;",
                expIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        DataManager.executeQuery(selectValues, (rs) -> {
            predictions.add(
                new Prediction(
                    rs.getInt("expId"),
                    rs.getString("genType"),
                    rs.getLong("timestamp"),
                    rs.getInt("scaleOut"),
                    rs.getDouble("avgThr"),
                    rs.getDouble("avgLat"),
                    rs.getDouble("recTime")));
        });
        return predictions;
    }
}
