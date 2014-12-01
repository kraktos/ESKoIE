package code.dws.experiment.goldstandard;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
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
		long count = 1;
		try {
			List<String> oieTriples = FileUtils.readLines(new File(
					Constants.OIE_DATA_PATH), "UTF-8");

			for (String oieTriple : oieTriples) {
				arr = oieTriple.split(";");

				if (OIE_PROP_COUNT_MAP.containsKey(arr[1])) {
					count = OIE_PROP_COUNT_MAP.get(arr[1]);
					count = count + 1;
				}
				OIE_PROP_COUNT_MAP.put(arr[1].toLowerCase().trim(), count);
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * routine to search the in memory OIE file for a match
	 * 
	 * @param sub
	 * @param kbRel
	 * @param obj
	 * @return
	 */
	public static void existsInOIEDataSet(String sub, String kbRel, String obj) {
		List<String> reltns;
		try {
			reltns = DataSearcher.doSearch(sub, obj);

			if (reltns != null) { // populate DB
				for (String rel : reltns) {
					if (OIE_PROP_COUNT_MAP.get(rel.toLowerCase().trim()) >= Long
							.parseLong(Constants.INSTANCE_THRESHOLD)) {
						logger.info(rel + " => " + kbRel + "\tD");
						
						// this is direct
						DBWrapper.insertIntoPropGS(rel, kbRel, "N");
					}
				}
			} else {
				reltns = DataSearcher.doSearch(obj, sub);
				if (reltns != null) { // populate DB
					for (String rel : reltns) {
						if (OIE_PROP_COUNT_MAP.get(rel.toLowerCase().trim()) >= Long
								.parseLong(Constants.INSTANCE_THRESHOLD)) {
							logger.info(rel + " => " + kbRel + "\tI");
							// this is inverse
							DBWrapper.insertIntoPropGS(rel, kbRel, "Y");
						}
					}
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
