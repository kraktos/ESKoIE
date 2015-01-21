package code.dws.experiment.goldstandard;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.indexer.DataSearcher;
import code.dws.utils.Constants;

public class StreamProcessor {

	// define Logger
	public static Logger logger = Logger.getLogger(StreamProcessor.class
			.getName());

	private static Map<String, Long> OIE_PROP_COUNT_MAP = new HashMap<String, Long>();

	/**
	 * read the changing file to fetch OIe data set
	 */

	public StreamProcessor() {

	}

	public static void main(String[] args) throws IOException,
			InterruptedException {

		Constants.loadConfigParameters(new String[] { "", args[0] });

		// logger.info("Loading OIE facts...");
		loadOIEInMemory();
		// logger.info("Loaded " + ALL_OIE.size() + " lines of OIE facts");

		DBWrapper.init(Constants.INSERT_GS_PROP);

		// make the lucene index directory ready
		DataSearcher.main(new String[] { args[1] });

		// Create the monitor
		FileMonitor monitor = FileMonitor.getInstance();
		monitor.addFileChangeListener(monitor.new FileChangerListenerImpl(),
				new File(KBSeeder.SEED_KB), 3000);

		// Avoid program exit
		while (true)
			;
	}

	/**
	 * load the oie file base in memory
	 */
	private static void loadOIEInMemory() {
		String[] arr = null;
		long count;
		try {
			List<String> oieTriples = FileUtils.readLines(new File(
					Constants.OIE_DATA_PATH), "UTF-8");

			for (String oieTriple : oieTriples) {
				count = 1;

				arr = oieTriple.split(";");

				if (OIE_PROP_COUNT_MAP.containsKey(arr[1].toLowerCase())) {
					count = OIE_PROP_COUNT_MAP.get(arr[1].toLowerCase());
					count = count + 1;
				}
				OIE_PROP_COUNT_MAP.put(arr[1].toLowerCase(), count);
			}

			logger.info("Loaded " + OIE_PROP_COUNT_MAP.size() + ", properties");
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * routine to search the in memory OIE file for a match
	 * 
	 * @param oieSub
	 * @param kbRel
	 * @param oieObj
	 * @param arr2
	 * @param arr
	 * @return
	 */
	public static void existsInOIEDataSet(String oieSub, String kbRel,
			String oieObj, String kbSub, String kbObj) {
		List<String> reltns;
		try {
			reltns = DataSearcher.doSearch(oieSub, oieObj);

			if (reltns != null) { // populate DB
				for (String rel : reltns) {
					System.out.println(Constants.INSTANCE_THRESHOLD);
					if (OIE_PROP_COUNT_MAP.get(rel.toLowerCase().trim()) >= Long
							.parseLong(Constants.INSTANCE_THRESHOLD)) {
						logger.info(rel
								+ "("
								+ OIE_PROP_COUNT_MAP.get(rel.toLowerCase()
										.trim()) + ") => " + kbRel + "\tD");

						// this is direct
						DBWrapper.insertIntoPropGS(oieSub, rel, oieObj, kbRel,
								"N");
					}
				}
			} else {
				reltns = DataSearcher.doSearch(oieObj, oieSub);
				if (reltns != null) { // populate DB
					for (String rel : reltns) {
						if (OIE_PROP_COUNT_MAP.get(rel.toLowerCase().trim()) >= Long
								.parseLong(Constants.INSTANCE_THRESHOLD)) {
							logger.info(rel + " => " + kbRel + "\tI");
							// this is inverse
							DBWrapper.insertIntoPropGS(oieSub, rel, oieObj,
									kbRel, "Y");
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
