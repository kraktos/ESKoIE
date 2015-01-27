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
import code.dws.utils.Utilities;

/**
 * @author adutta
 *
 */
public class GSSampleCreator {

	// define Logger
	public static Logger logger = Logger.getLogger(GSSampleCreator.class
			.getName());

	private static Map<String, List<List<String>>> ANNO_PROPS = new HashMap<String, List<List<String>>>();

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
		String oieSub = null;
		String oieObj = null;

		String[] elems = null;

		List<String> candidateSubjs = null;
		List<String> candidateObjs = null;

		try {
			scan = new Scanner(new File(fMinusFile), "UTF-8");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		// iterate the file
		while (scan.hasNextLine()) {
			line = scan.nextLine();
			elems = line.split(Constants.OIE_DATA_SEPERARTOR);

			// valid line which can be used for evaluation
			if (ANNO_PROPS.containsKey(elems[1])) {

				logger.info(line);

				oieSub = elems[0];
				oieObj = elems[2];

				// get the top-k concepts for the subject
				candidateSubjs = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
						.cleanse(oieSub).replaceAll("\\_+", " ").trim(),
						Constants.TOP_K_MATCHES);

				// get the top-k concepts for the object
				candidateObjs = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
						.cleanse(oieObj).replaceAll("\\_+", " ").trim(),
						Constants.TOP_K_MATCHES);

				writeOut(candidateSubjs, candidateObjs,
						ANNO_PROPS.get(elems[1]));

			}

		}
		DBWrapper.shutDown();
	}

	private static void writeOut(List<String> candidateSubjs,
			List<String> candidateObjs, List<List<String>> list) {

	}
}
