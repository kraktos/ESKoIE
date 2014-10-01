package code.dws.experiment.evaluation;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import code.dws.utils.Constants;

public class Evaluation {

	// define Logger
	static Logger logger = Logger.getLogger(Evaluation.class.getName());

	private static final String GOLD_FILE = "/home/adutta/git/ReverbGS1.tsv";

	private static final String METHOD_FILE = "";

	public Evaluation() {
	}

	public static void main(String[] args) {

		THashMap<FactDao, FactDao> gold;
		try {
			gold = loadGoldFile(GOLD_FILE);

			// extract out exactly these gold facts from the algorithm output
			// to check the precision, recall, F1
			// hence we will create another collection of FactDao => FactDao and
			// compare these two maps

			for (Map.Entry<FactDao, FactDao> entry : gold.entrySet()) {

			}
			// List<FactDao> method = loadMethodOutputFile(METHOD_FILE);

		} catch (Exception e) {
			e.printStackTrace();
		}

		// compare(gold, method);
	}

	private static void compare(List<FactDao> gold, List<FactDao> method) {

	}

	/**
	 * load the gold standard file
	 * 
	 * @param goldFile
	 * @return
	 * @throws Exception
	 */
	private static THashMap<FactDao, FactDao> loadGoldFile(String goldFile)
			throws Exception {

		String[] arr = null;

		FactDao oieFact = null;
		FactDao dbpFact = null;

		THashMap<FactDao, FactDao> goldFacts = new THashMap<FactDao, FactDao>();

		// load the NELL file in memory as a collection
		List<String> gold = FileUtils.readLines(new File(goldFile));

		for (String line : gold) {

			if (line.indexOf("\tIP") != -1 || line.indexOf("\tUC") != -1) {
				// validGSLines.add(line); // all the lines actually annotated

				if (line.indexOf(Constants.DBPEDIA_NAMESPACE) != -1
						&& line.indexOf("\tUC") == -1) {
					arr = line.split("\t");
					oieFact = new FactDao(arr[0], arr[1], arr[2]);
					dbpFact = new FactDao(arr[3], arr[4], arr[5]);

					goldFacts.put(oieFact, dbpFact);

				}
			}
		}

		for (Entry<FactDao, FactDao> e : goldFacts.entrySet()) {
			logger.debug(e.getKey() + "==>" + e.getValue());
		}
		return goldFacts;
	}

	private static List<FactDao> loadMethodOutputFile(String methodFile) {

		return null;
	}

}
