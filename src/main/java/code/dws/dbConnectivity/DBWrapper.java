/**
 * 
 */

package code.dws.dbConnectivity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import antlr.collections.impl.Vector;
import code.dws.core.cluster.vector.VectorCluster;
import code.dws.experiment.evaluation.FactDao;
import code.dws.markovLogic.EvidenceBuilder;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * Wrapper class to initiate the DB operations. Used on top of {@link DBConnection}
 * 
 * @author Arnab Dutta
 */
public class DBWrapper
{

    public static final String GS_DELIMITER = "~~";

    // define Logger
    static Logger logger = Logger.getLogger(DBWrapper.class.getName());

    // DB connection instance, one per servlet
    static Connection connection = null;

    // DBCOnnection
    static DBConnection dbConnection = null;

    // prepared statement instance
    static PreparedStatement pstmt = null;

    static PreparedStatement insertDBPTypePrepstmnt = null;

    static PreparedStatement insertOIEPFxdPrepstmnt = null;

    static PreparedStatement fetchDbpTypePrepstmnt = null;

    static int batchCounter = 0;

    /**
     * initiates the connection parameters
     * 
     * @param sql
     */
    public static void init(String sql)
    {
        try {
            // instantiate the DB connection
            dbConnection = new DBConnection();

            // retrieve the freshly created connection instance
            connection = dbConnection.getConnection();

            // create a statement
            pstmt = connection.prepareStatement(sql);
            // for DBPedia types
            insertDBPTypePrepstmnt = connection.prepareStatement(Constants.INSERT_DBP_TYPES);
            insertOIEPFxdPrepstmnt = connection.prepareStatement(Constants.OIE_POSTFIXED);
            fetchDbpTypePrepstmnt = connection.prepareStatement(Constants.GET_DBPTYPE);

            connection.setAutoCommit(false);

        } catch (SQLException ex) {
            ex.printStackTrace();
            logger.error("Connection Failed! Check output console" + ex.getMessage());
        }
    }

    public static void saveToOIEPostFxd(String oieSub, String oiePred, String oieObj, String oieSubPfxd,
        String oieObjPfxd)
    {

        try {

            insertOIEPFxdPrepstmnt.setString(1, oieSub);
            insertOIEPFxdPrepstmnt.setString(2, oiePred);
            insertOIEPFxdPrepstmnt.setString(3, oieObj);
            insertOIEPFxdPrepstmnt.setString(4, oieSubPfxd);
            insertOIEPFxdPrepstmnt.setString(5, oieObjPfxd);
            insertOIEPFxdPrepstmnt.setString(6, "X");
            insertOIEPFxdPrepstmnt.setString(7, "X");

            insertOIEPFxdPrepstmnt.addBatch();
            insertOIEPFxdPrepstmnt.clearParameters();

            batchCounter++;
            if (batchCounter % Constants.BATCH_SIZE == 0 && batchCounter > Constants.BATCH_SIZE) { // batches are
                // flushed at
                // a time
                // execute batch update
                insertOIEPFxdPrepstmnt.executeBatch();

                logger.info("FLUSHED TO OIE_REFINED...");

                connection.commit();
                insertOIEPFxdPrepstmnt.clearBatch();
            }

        } catch (SQLException e) {
            logger.error("Error with batch insertion of OIE_REFINED .." + e.getMessage());
        }

    }

    public static void saveToDBPediaTypes(String instance, String instType)
    {

        try {

            insertDBPTypePrepstmnt.setString(1, instance);
            insertDBPTypePrepstmnt.setString(2, instType);

            insertDBPTypePrepstmnt.addBatch();
            insertDBPTypePrepstmnt.clearParameters();

            batchCounter++;
            if (batchCounter % Constants.BATCH_SIZE == 0 && batchCounter > Constants.BATCH_SIZE) { // batches are
                // flushed at
                // a time
                // execute batch update
                insertDBPTypePrepstmnt.executeBatch();

                logger.info("FLUSHED TO DBPEDIA_TYPES");
                connection.commit();
                insertDBPTypePrepstmnt.clearBatch();
            }

        } catch (SQLException e) {
            logger.error("Error with batch insertion of DBPEDIA_TYPES .." + e.getMessage());
        }

    }

    public static List<String> getFullyMappedFacts()
    {
        List<String> types = new ArrayList<String>();

        try {

            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                types.add(rs.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return types;
    }

    /**
     * works for both somain and range
     * 
     * @param param
     * @return
     */
    public static List<String> getOIEFeatures(String param)
    {
        List<String> types = new ArrayList<String>();

        try {
            pstmt.setString(1, param);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                types.add(rs.getString(1));
                if (!VectorCluster.featureSpace.contains(rs.getString(1)))
                    VectorCluster.featureSpace.add(rs.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return types;
    }

    public static FactDao getRefinedDBPFact(FactDao key)
    {

        String dbpSub = null;
        String dbpObj = null;

        try {

            pstmt.setString(1, key.getSub());
            pstmt.setString(2, key.getRelation());
            pstmt.setString(3, key.getObj());
            ResultSet rs = pstmt.executeQuery();

            if (rs.next()) {
                dbpSub = (rs.getString(1).equals("X")) ? "?" : rs.getString(1);
                dbpObj = (rs.getString(2).equals("X")) ? "?" : rs.getString(2);

                return new FactDao(Utilities.utf8ToCharacter(dbpSub), "?", Utilities.utf8ToCharacter(dbpObj));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static List<String> getDBPInstanceType(String instance)
    {
        List<String> types = new ArrayList<String>();

        try {
            // if not cached
            if (!EvidenceBuilder.INSTANCE_TYPES.containsKey(instance)) {

                fetchDbpTypePrepstmnt.setString(1, instance);
                ResultSet rs = fetchDbpTypePrepstmnt.executeQuery();

                while (rs.next()) {
                    types.add(rs.getString(1));
                }
                // cache it
                EvidenceBuilder.INSTANCE_TYPES.put(instance, types);
            } else {
                return EvidenceBuilder.INSTANCE_TYPES.get(instance);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return types;
    }

    public static List<String> fetchWikiTitles(String arg)
    {
        ResultSet rs = null;
        List<String> results = null;

        try {

            pstmt.setString(1, arg.trim());
            // pstmt.setInt(2, Constants.ATLEAST_LINKS);
            pstmt.setInt(2, Constants.TOP_K_MATCHES);
            // run the query finally
            rs = pstmt.executeQuery();
            results = new ArrayList<String>();

            while (rs.next()) {
                results.add(rs.getString(1));
            }

        } catch (Exception e) {
            logger.error(" exception while fetching " + arg + " " + e.getMessage());
        }

        return results;
    }

    /**
     * returns a pair of all possible surface forms, for a given pair of KB instances
     * 
     * @param dbpSub
     * @param dbpObj
     * @return
     */
    public static List<Pair<String, String>> getSurfaceForms(String dbpSub, String dbpObj)
    {
        ResultSet rs = null;
        ResultSetMetaData rsmd = null;
        List<Pair<String, String>> results = new ArrayList<Pair<String, String>>();

        try {
            pstmt.setString(1, dbpSub);
            pstmt.setString(2, dbpObj);

            rs = pstmt.executeQuery();
            rsmd = rs.getMetaData();

            results = new ArrayList<Pair<String, String>>();

            while (rs.next()) {
                if (rsmd.getColumnCount() == 2)
                    results.add(new ImmutablePair<String, String>(rs.getString(1), rs.getString(2)));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return results;
    }

    /**
     * method to find the refined mappings for the oie subject and object from DB
     * 
     * @param oieSub
     * @param pred
     * @param oieObj
     * @return
     */
    public static List<String> fetchRefinedMapping(String oieSub, String pred, String oieObj)
    {
        ResultSet rs = null;
        List<String> results = null;

        try {
            pstmt.setString(1, oieSub);
            pstmt.setString(2, pred);
            pstmt.setString(3, oieObj);

            rs = pstmt.executeQuery();
            results = new ArrayList<String>();

            while (rs.next()) {

                results.add(Utilities.characterToUTF8((Utilities.utf8ToCharacter(rs.getString(1)))
                    .replaceAll("\\s", "_").replaceAll("\\[", "\\(").replaceAll("\\]", "\\)")));
                results.add(Utilities.characterToUTF8((Utilities.utf8ToCharacter(rs.getString(2)))
                    .replaceAll("\\s", "_").replaceAll("\\[", "\\(").replaceAll("\\]", "\\")));
            }

        } catch (Exception e) {
            logger.error(e.getMessage());
        }

        return results;
    }

    /**
     * find the top k candidates for a given surface form/term/ oie instance
     * 
     * @param arg
     * @param limit
     * @return
     */
    public static List<String> fetchTopKLinksWikiPrepProb(String arg, int limit)
    {
        ResultSet rs = null;
        List<String> results = null;
        List<String> temp = null;

        DecimalFormat decimalFormatter = new DecimalFormat("0.00000");

        try {
            if (!EvidenceBuilder.INSTANCE_CANDIDATES.containsKey(arg)) {

                pstmt.setString(1, arg.trim());
                pstmt.setString(2, arg.trim());
                pstmt.setInt(3, limit);

                rs = pstmt.executeQuery();
                results = new ArrayList<String>();
                temp = new ArrayList<String>();

                while (rs.next()) {

                    results.add(Utilities.characterToUTF8((rs.getString(1)).replaceAll("\\s", "_")) + "\t"
                        + decimalFormatter.format(rs.getDouble(2)));

                    temp.add(Utilities.characterToUTF8((rs.getString(1)).replaceAll("\\s", "_")) + "\t"
                        + decimalFormatter.format(rs.getDouble(2)));
                }

                EvidenceBuilder.INSTANCE_CANDIDATES.put(arg, temp);
            } else {
                return EvidenceBuilder.INSTANCE_CANDIDATES.get(arg);
            }
        } catch (Exception e) {

            logger.error(" exception while fetching " + arg + " " + e.getMessage());
        }

        return results;
    }

    public static void saveResidualOIERefined()
    {
        try {
            if (batchCounter % Constants.BATCH_SIZE != 0) {
                insertOIEPFxdPrepstmnt.executeBatch();
                logger.info("FLUSHED TO OIE_REFINED...");
                connection.commit();
            }
        } catch (SQLException e) {
        }
    }

    public static void updateResidualOIERefined()
    {
        try {
            if (batchCounter % Constants.BATCH_SIZE != 0) {
                pstmt.executeBatch();
                logger.info("FLUSHED TO OIE_REFINED...");
                connection.commit();
            }
        } catch (SQLException e) {
        }
    }

    public static void updateOIEPostFxd(String oieSub, String oiePred, String oieObj, String dbpS, String dbpO)
    {

        try {

            pstmt.setString(1, dbpS);
            pstmt.setString(2, dbpO);

            pstmt.setString(3, oieSub);
            pstmt.setString(4, oieObj);
            pstmt.setString(5, oiePred);

            pstmt.addBatch();
            pstmt.clearParameters();

            batchCounter++;

            if (batchCounter % Constants.BATCH_SIZE == 0 && batchCounter > Constants.BATCH_SIZE) { // batches are
                // flushed at
                // a time
                // execute batch update
                pstmt.executeBatch();

                logger.info("FLUSHED TO OIE_REFINED");
                connection.commit();
                pstmt.clearBatch();
            }

        } catch (SQLException e) {
            logger.error("Error with batch update of OIE_REFINED .." + e.getMessage());
        }

    }

    public static void saveResidualDBPTypes()
    {
        try {
            if (batchCounter % Constants.BATCH_SIZE != 0) {
                insertDBPTypePrepstmnt.executeBatch();
                logger.info("FLUSHED TO DBPEDIA_TYPES...");
                connection.commit();
            }
        } catch (SQLException e) {
        }
    }

    public static void shutDown()
    {

        if (pstmt != null) {
            try {
                pstmt.close();
            } catch (Exception excp) {
            }
        }

        if (fetchDbpTypePrepstmnt != null) {
            try {
                fetchDbpTypePrepstmnt.close();
            } catch (Exception excp) {
            }
        }

        if (insertDBPTypePrepstmnt != null) {
            try {
                insertDBPTypePrepstmnt.close();
            } catch (Exception excp) {
            }
        }

        if (insertOIEPFxdPrepstmnt != null) {
            try {
                insertOIEPFxdPrepstmnt.close();
            } catch (Exception excp) {
            }
        }

        dbConnection.shutDown();

    }

}
