package org.wso2.cepperf.distprof;

import java.sql.*;

/**
 * Created by miyurud on 2/23/17.
 */
public class DistProfAPI {
    private String connectionString;
    private Connection con;
    private int sequenceNumber;
    private String comments;


    public DistProfAPI(String host, String sessionSpecificComments, int sequenceNum){
        connectionString = "jdbc:hsqldb:hsql://"+host+":3342/distprof_meta;ifexists=true";
        con = getDBConnection();
        if(sequenceNum < 0){
            sequenceNumber = getSequenceNumber();
        }else {
            sequenceNumber = sequenceNum;
        }
        comments = sessionSpecificComments;
    }

    public DistProfAPI(String host, String sessionSpecificComments){
        connectionString = "jdbc:hsqldb:hsql://"+host+":3342/distprof_meta;ifexists=true";
        con = getDBConnection();
        sequenceNumber = getSequenceNumber() + 1;
        comments = sessionSpecificComments;
    }

    private Connection getDBConnection() {
        try {
            Class.forName("org.hsqldb.jdbcDriver").newInstance();
            Connection con = DriverManager.getConnection(connectionString,
                    "SA", "");

            if (con != null) {
                return con;
            }
        } catch (SQLException e) {
            if (e.getMessage().contains("Database does not exists")) {
                System.out.println("No Distprof DB");
                return null;
            } else {
                System.out.println("Error : " + e.getMessage());
                return null;
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (InstantiationException e) {
            e.printStackTrace();
            return null;
        } catch (IllegalAccessException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }

    public void shutdown(){
        try {
            con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String insertNormalRecord(long evttime, String host, float throughputInWindow,
                               float throughputFull,
                                float totalElapsedTime, float avgLatencyPerEvtm, float avgLatencyPerEvtFull,
                                long totalNoOfEventsReceived){
        String result = null;
        PreparedStatement stmt;
        String query = "INSERT INTO DISTPROF_META.performance(sequence_number, evttime, recordtime, host, throughputInWindow, " +
                "throughputFull, totalElapsedTime, avgLatencyPerEvt, avgLatencyPerEvtFull, " +
                "totalNoOfEventsReceived, typeOfEvt, comments) VALUES (" + sequenceNumber
                                                                          + "," + evttime
                                                                          + "," + System.currentTimeMillis()
                                                                          + ",'"+ host
                                                                          +"'," +  throughputInWindow
                                                                          +"," +  throughputFull
                                                                          +"," +  totalElapsedTime
                                                                          +"," +  avgLatencyPerEvtm
                                                                          +"," +  avgLatencyPerEvtFull
                                                                          +"," +  totalNoOfEventsReceived
                                                                          + "," + TypeOfEvent.Normal.getType()
                                                                          + ",\'" + comments
                                                                          + "\')";

        try {
            stmt = con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            stmt.executeUpdate();
            ResultSet rs = stmt.getGeneratedKeys();

            boolean first = true;
            while (rs.next()) {
                if (first) {
                    first = false;
                    result = rs.getString(1);
                } else {
                    result += "," + rs.getString(1);
                }
            }
        } catch (SQLException e) {
            result = "Error : " + e.getMessage();
        }
        finally {
            try {
                con.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
            return result;

    }

    public String insertFailureRecord(long evttime){
        String result = null;

        PreparedStatement stmt;
        String query = "INSERT INTO DISTPROF_META.performance(sequence_number, evttime, recordtime, host, throughputInWindow, " +
                "throughputFull, totalElapsedTime, avgLatencyPerEvt, avgLatencyPerEvtFull, " +
                "totalNoOfEventsReceived, typeOfEvt, comments) VALUES (" + sequenceNumber
                + "," + evttime
                + "," + System.currentTimeMillis()
                + ",''," +  -1
                +"," +  -1
                +"," +  -1
                +"," +  -1
                +"," +  -1
                +"," +  -1
                + "," + TypeOfEvent.Failure.getType()
                + ",\'" + comments
                + "\')";

        try {
            stmt = con.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
            stmt.executeUpdate();
            ResultSet rs = stmt.getGeneratedKeys();

            boolean first = true;
            while (rs.next()) {
                if (first) {
                    first = false;
                    result = rs.getString(1);
                } else {
                    result += "," + rs.getString(1);
                }
            }
        } catch (SQLException e) {
            result = "Error : " + e.getMessage();
        } finally {
            try {
                con.commit();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

            return result;

    }

    public int getSequenceNumber(){
        int result = -1;
        Statement stmt;

        try {
            stmt = con.createStatement();

            ResultSet rs = stmt.executeQuery("SELECT TOP 1 SEQUENCE_NUMBER FROM \"DISTPROF_META\".\"PERFORMANCE\" " +
                    "ORDER BY SEQUENCE_NUMBER DESC;");

            if(rs.next()){
                result = Integer.parseInt(rs.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return result;
    }

    public void setSequenceNumber(int sequenceNumber){
        this.sequenceNumber = sequenceNumber;
    }
}
