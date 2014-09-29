/**
 * 
 */

package code.dws.dbConnectivity;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * Wrapper class to initiate the DB operations. Used on top of
 * {@link DBConnection}
 * 
 * @author Arnab Dutta
 */
public class DBWrapper {

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
	 * initiats the connection parameters
	 * 
	 * @param sql
	 */
	public static void init(String sql) {
		try {
			// instantiate the DB connection
			dbConnection = new DBConnection();

			// retrieve the freshly created connection instance
			connection = dbConnection.getConnection();

			// create a statement
			pstmt = connection.prepareStatement(sql);
			// for DBPedia types
			insertDBPTypePrepstmnt = connection
					.prepareStatement(Constants.INSERT_DBP_TYPES);
			insertOIEPFxdPrepstmnt = connection
					.prepareStatement(Constants.OIE_POSTFIXED);
			fetchDbpTypePrepstmnt = connection
					.prepareStatement(Constants.GET_DBPTYPE);

			connection.setAutoCommit(false);

		} catch (SQLException ex) {
			ex.printStackTrace();
			logger.error("Connection Failed! Check output console"
					+ ex.getMessage());
		}
	}

	public static List<String> fetchWikiTitles(String arg) {
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
			logger.error(" exception while fetching " + arg + " "
					+ e.getMessage());
		}

		return results;
	}

	public static void saveToOIEPostFxd(String oieSub, String oiePred,
			String oieObj, String oieSubPfxd, String oieObjPfxd) {

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
			if (batchCounter % Constants.BATCH_SIZE == 0) { // batches are
															// flushed at
															// a time
				// execute batch update
				insertOIEPFxdPrepstmnt.executeBatch();

				logger.info("FLUSHED TO OIE_REFINED...");

				connection.commit();
				insertOIEPFxdPrepstmnt.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("Error with batch insertion of OIE_REFINED .."
					+ e.getMessage());
		}

	}

	public static void saveToDBPediaTypes(String instance, String instType) {

		try {

			insertDBPTypePrepstmnt.setString(1, instance);
			insertDBPTypePrepstmnt.setString(2, instType);

			insertDBPTypePrepstmnt.addBatch();
			insertDBPTypePrepstmnt.clearParameters();

			batchCounter++;
			if (batchCounter % Constants.BATCH_SIZE == 0
					&& batchCounter > Constants.BATCH_SIZE) { // batches are
				// flushed at
				// a time
				// execute batch update
				insertDBPTypePrepstmnt.executeBatch();

				logger.info("FLUSHED TO DBPEDIA_TYPES");
				connection.commit();
				insertDBPTypePrepstmnt.clearBatch();
			}

		} catch (SQLException e) {
			logger.error("Error with batch insertion of DBPEDIA_TYPES .."
					+ e.getMessage());
		}

	}

	public static List<String> getDBPInstanceType(String instance) {
		List<String> types = new ArrayList<String>();

		try {
			fetchDbpTypePrepstmnt.setString(1, instance);
			ResultSet rs = fetchDbpTypePrepstmnt.executeQuery();

			while (rs.next()) {
				types.add(rs.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return types;
	}

	public static List<String> fetchTopKLinksWikiPrepProb(String arg, int limit) {
		ResultSet rs = null;
		List<String> results = null;

		DecimalFormat decimalFormatter = new DecimalFormat("0.00000");

		try {
			pstmt.setString(1, arg.trim());
			pstmt.setString(2, arg.trim());
			pstmt.setInt(3, limit);

			rs = pstmt.executeQuery();
			results = new ArrayList<String>();

			while (rs.next()) {

				results.add(Utilities.characterToUTF8((rs.getString(1))
						.replaceAll("\\s", "_"))
						+ "\t"
						+ decimalFormatter.format(rs.getDouble(2)));
			}

		} catch (Exception e) {

			logger.error(" exception while fetching " + arg + " "
					+ e.getMessage());
		}

		return results;
	}

	public static void saveResidualOIERefined() {
		try {
			if (batchCounter % Constants.BATCH_SIZE != 0) {
				insertOIEPFxdPrepstmnt.executeBatch();
				logger.info("FLUSHED TO OIE_REFINED...");
				connection.commit();
			}
		} catch (SQLException e) {
		}
	}

	public static void saveResidualDBPTypes() {
		try {
			if (batchCounter % Constants.BATCH_SIZE != 0) {
				insertDBPTypePrepstmnt.executeBatch();
				logger.info("FLUSHED TO DBPEDIA_TYPES...");
				connection.commit();
			}
		} catch (SQLException e) {
		}
	}

	public static void shutDown() {

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
