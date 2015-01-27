/**
 * 
 */
package code.dws.experiment.goldstandard;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;

/**
 * @author adutta
 *
 */
public class GSSampleCreator {

	// define Logger
	public static Logger logger = Logger.getLogger(GSSampleCreator.class
			.getName());

	/**
	 * 
	 */
	public GSSampleCreator() {
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// load constants file
		Constants.loadConfigParameters(new String[] { "", "CONFIG.cfg" });

		String location = new File(Constants.OIE_DATA_PATH).getParent();

		// load the annotated OIE relations
		loadAnnotatedProperties();

		// load the fminus File, randomly sampling lines
		sampleFile(location + "/fMinus.dat");
	}

	/**
	 * 
	 */
	private static void loadAnnotatedProperties() {

		Map<String, List<List<String>>> ANNO_PROPS = new HashMap<String, List<List<String>>>();

		// init the DB
		DBWrapper.init(Constants.GET_OIE_PROPERTIES_ANNOTATED);
		ANNO_PROPS = DBWrapper.getAnnoPairs();

		for (Map.Entry<String, List<List<String>>> e : ANNO_PROPS.entrySet()) {
			for (List<String> lst : e.getValue()) {
				logger.debug(e.getKey() + "\t" + lst);
			}
		}
		DBWrapper.shutDown();
	}

	/**
	 * sample for the triples which should be used for annotation
	 * 
	 * @param fMinusFile
	 */
	private static void sampleFile(String fMinusFile) {
		String line = null;
		Scanner scan = null;

		try {
			scan = new Scanner(new File(fMinusFile), "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// iterate the file
		while (scan.hasNextLine()) {
			line = scan.nextLine();
			logger.info(line);
		}
	}
}
