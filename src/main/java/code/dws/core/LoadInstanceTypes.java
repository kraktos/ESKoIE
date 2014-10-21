/**
 * 
 */
package code.dws.core;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * Scan the input file and load into DB the type of each instances. This should
 * run once, makes the whole pipeline faster
 * 
 * @author adutta
 * 
 */
public class LoadInstanceTypes {

	// define Logger
	static Logger logger = Logger.getLogger(LoadInstanceTypes.class.getName());

	static THashMap<String, Long> COUNT_PROPERTY_INST = new THashMap<String, Long>();

	/**
	 * 
	 */
	public LoadInstanceTypes() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		logger.info("\n\n ======== RUNNING TYPE LOADER... ============= \n ");

		long start = Utilities.startTimer();

		Constants.loadConfigParameters(new String[] { "", "CONFIG.cfg" });

		getReverbProperties(Constants.OIE_DATA_PATH, -1, 10L);

		Utilities.endTimer(start, "Filetered property set loading  takes = ");

		logger.debug(COUNT_PROPERTY_INST.size());

		long s1 = Utilities.startTimer();
		typeLoad(Constants.OIE_DATA_PATH);
		Utilities.endTimer(s1, "Type loading takes = ");
	}

	private static void typeLoad(String OIE_DATA_PATH) throws IOException {

		long cnt = 0;
		String line = null;
		String[] arr = null;

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		try {
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(OIE_DATA_PATH));
			long start = Utilities.startTimer();
			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split(";");
				if (COUNT_PROPERTY_INST.get(arr[1]) >= 10) {
					cnt++;
					// get the top-k concepts, confidence pairs
					// UTF-8 at this stage
					try {
						saveTypes(arr[0]);
						saveTypes(arr[2]);

					} catch (IOException e) {
						e.printStackTrace();
					}
				}

				if (cnt % Constants.BATCH_SIZE == 0
						&& cnt > Constants.BATCH_SIZE) {
					Utilities.endTimer(start, (100 * (double) cnt / 2578152)
							+ "% completion takes ");
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		// flush residuals
		DBWrapper.saveResidualDBPTypes();
		// shutdown DB
		DBWrapper.shutDown();

	}

	public static void saveTypes(String oieInstance) throws IOException {

		List<String> candidates;
		String conc;

		// get the top-k concepts, confidence pairs
		// UTF-8 at this stage
		candidates = DBWrapper.fetchTopKLinksWikiPrepProb(
				Utilities.cleanse(oieInstance).replaceAll("\\_+", " "),
				Constants.TOP_K_MATCHES);

		for (String val : candidates) {

			// if one instance-dbpedia pair is already in, skip it

			// back to character again
			conc = Utilities.utf8ToCharacter(val.split("\t")[0]);

			// the type info are written out to the writer object
			generateDBPediaTypes(conc);

		}

	}

	/**
	 * create type of DBP instances
	 * 
	 * @param pair
	 * @param isOfTypeEvidenceWriter
	 * @param identifier
	 * @throws Exception
	 */
	private static void generateDBPediaTypes(String dbPediaInstance) {

		List<String> listTypes;

		String tempInst = null;

		tempInst = dbPediaInstance;

		// get DBPedia types
		listTypes = SPARQLEndPointQueryAPI.getInstanceTypes(tempInst);

		if (listTypes.size() == 0
				|| listTypes.get(0).indexOf(Constants.UNTYPED) != -1) {

			DBWrapper.saveToDBPediaTypes(Utilities.characterToUTF8(tempInst),
					Constants.UNTYPED);

		} else if (listTypes.size() > 0) {

			// get the most specific type

			listTypes = SPARQLEndPointQueryAPI.getLowestType(listTypes);

			for (String type : listTypes) {

				// for faster future processing, store types in Database,
				// flip side is this may be old data, so change CONFIG
				// parameters to
				// reload fresh data and should be run once in a week or
				// so..
				DBWrapper.saveToDBPediaTypes(
						Utilities.characterToUTF8(tempInst), type);
			}
		}
	}

	/**
	 * get the list of Reverb properties
	 * 
	 * CAn be used to get both top-k properties, or properties with atleast x
	 * number of instances
	 * 
	 * @param OIE_FILE
	 * @param TOPK_REV_PROPS
	 * @param atLeastInstancesCount
	 * @return List of properties
	 */
	public static void getReverbProperties(String OIE_FILE, int TOPK_REV_PROPS,
			Long atLeastInstancesCount) {

		String line = null;
		String[] arr = null;
		long val = 0;

		try {
			@SuppressWarnings("resource")
			Scanner scan = new Scanner(new File(OIE_FILE));

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split(";");
				if (COUNT_PROPERTY_INST.containsKey(arr[1])) {
					val = COUNT_PROPERTY_INST.get(arr[1]);
					val++;
				} else {
					val = 1;
				}
				COUNT_PROPERTY_INST.put(arr[1], val);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		// // load the properties with atleast k instances each
		// COUNT_PROPERTY_INST = Utilities.sortByValue(COUNT_PROPERTY_INST,
		// atLeastInstancesCount);
		//
		// for (Entry<String, Long> e : COUNT_PROPERTY_INST.entrySet()) {
		// if (e.getValue() >= 10)
		// ret.add(e.getKey());
		// }

		// return COUNT_PROPERTY_INST;
	}
}
