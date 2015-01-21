/**
 * 
 */

package code.dws.dbConnectivity;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import code.dws.utils.Constants;

/**
 * Basic DB functionality class. On top of this {@link DBWrapper} runs as a
 * wrapper.
 * 
 * @author Arnab Dutta
 */
public class DBConnection {

	// logger
	static Logger logger = Logger.getLogger(DBConnection.class.getName());

	// Connection reference
	private Connection connection = null;

	// Statement
	Statement statement = null;

	// DB Driver name
	public static String driverName = Constants.DRIVER_NAME;

	// Url to conenct to the Database
	public static String connectionURL = Constants.CONNECTION_URL;

	// name of the database
	public static String dbName = Constants.DB_NAME;

	// user of the database. Make sure this user is created for the DB
	public static String dbUser = Constants.DB_USER;

	// password for the user
	public static String dbUserPassword = Constants.DB_PWD;

	/**
	 * initialize the DB in the constructor
	 * 
	 * @throws SQLException
	 */
	public DBConnection() throws SQLException {
		initDB();
	}

	/**
	 * @return the connection
	 */
	public Connection getConnection() {
		return this.connection;
	}

	/**
	 * @param connection
	 *            the connection to set
	 */
	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	/**
	 * @return the statement
	 */
	public Statement getStatement() {
		return statement;
	}

	/**
	 * @param statement
	 *            the statement to set
	 */
	public void setStatement(Statement statement) {
		this.statement = statement;
	}

	static {
		registerDriver();

		logger.debug(driverName + " Registered!\n");

	}

	public Connection initDB() throws SQLException {
		this.connection = DriverManager.getConnection(connectionURL + dbName,
				dbUser, dbUserPassword);

		if (this.connection != null) {
			return getConnection();

		} else {
			logger.info("Failed to make connection!");
		}

		return null;

	}

	/**
	 * register the driver
	 */
	public static void registerDriver() {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			logger.error("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			return;

		}
	}

	/**
	 * performs the query execution
	 * 
	 * @param queryString
	 *            input select query to be executed
	 * @return a result set, can be null
	 */
	public ResultSet getResults(String queryString) {
		try {
			return this.statement.executeQuery(queryString);
		} catch (SQLException e) {
			logger.error("Exception while selecting " + queryString + "  "
					+ e.getMessage());
			return null;
		}
	}

	/**
	 * Closes the database
	 */
	public void shutDown() {

		try {
			if (this.statement != null)
				this.statement.close();
			if (this.connection != null)
				this.connection.close();

		} catch (SQLException e) {
			logger.error("DB Closing failed...");
		}

	}
}
