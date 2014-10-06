package code.dws.experiment.evaluation;

import gnu.trove.map.hash.THashMap;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;

public class Evaluation {

	// define Logger
	static Logger logger = Logger.getLogger(Evaluation.class.getName());

	private static final String GOLD_FILE = "/home/adutta/git/ReverbGS1.1.tsv";

	/**
	 * gold standard file collection
	 */
	static THashMap<FactDao, FactDao> gold = new THashMap<FactDao, FactDao>();

	/**
	 * method output collection
	 */
	static THashMap<FactDao, FactDao> algo = new THashMap<FactDao, FactDao>();

	public Evaluation() {
	}

	public static void main(String[] args) {

		// load the respective gold standard and methods in memeory
		setup();

		// perform comparision
		compare();
	}

	/**
	 * 
	 */
	public static void setup() {
		FactDao dbpFact = null;
		FactDao annotatedGoldFact = null;

		try {

			// init DB
			DBWrapper.init(Constants.GET_REFINED_FACT);

			// extract out exactly these gold facts from the algorithm output
			// to check the precision, recall, F1
			// hence we will create another collection of FactDao => FactDao and
			// compare these two maps

			for (Map.Entry<FactDao, FactDao> entry : loadGoldFile(GOLD_FILE)
					.entrySet()) {
				annotatedGoldFact = entry.getValue();
				dbpFact = DBWrapper.getRefinedDBPFact(entry.getKey());

				if (dbpFact != null) {
					logger.info(entry.getKey());
					logger.info("==>" + annotatedGoldFact);
					logger.info("==>" + dbpFact);

					// take the instances in Gold standard which have a
					// corresponding refinement done.
					algo.put(entry.getKey(), dbpFact);
					gold.put(entry.getKey(), annotatedGoldFact);

				}
			}

			logger.info("GS Size = " + gold.size());
			logger.info("Algo Size = " + algo.size());

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DBWrapper.shutDown();
		}
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
		int cnt = 0;
		for (String line : gold) {

			if (line.indexOf("\tIP") != -1 || line.indexOf("\tUC") != -1) {

				// if (line.indexOf(Constants.DBPEDIA_NAMESPACE) != -1) {
				arr = line.split("\t");
				oieFact = new FactDao(arr[0], arr[1], arr[2]);
				dbpFact = new FactDao(StringUtils.replace(arr[3],
						Constants.DBPEDIA_INSTANCE_NS, ""), arr[4],
						StringUtils.replace(arr[5],
								Constants.DBPEDIA_INSTANCE_NS, ""));

				goldFacts.put(oieFact, dbpFact);

				cnt++;
			}

			// }
		}

		return goldFacts;
	}

	private static void compare() {

		double prec = computeScore("P");
		double recall = computeScore("R");

		logger.info("Precision = " + prec);
		logger.info("Recall = " + recall);
		logger.info("F1 = " + (double) 2 * recall * prec / (recall + prec));
	}

	/**
	 * @param string
	 * @return
	 * 
	 */
	public static double computeScore(String identifier) {
		long numer = 0;
		long denom = 0;

		FactDao algoFact = null;
		FactDao goldFact = null;

		if (identifier.equals("P")) {
			// FOR PRECISION
			for (Map.Entry<FactDao, FactDao> entry : algo.entrySet()) {
				algoFact = entry.getValue();
				goldFact = gold.get(entry.getKey());

				// subjects
				if (algoFact.getSub().equals(goldFact.getSub())) {
					// && !algoFact.getSub().equals("?")) {
					numer++;
				}
				// if (!goldFact.getSub().equals("?"))
				denom++;

				// objects
				if (algoFact.getObj().equals(goldFact.getObj())) {
					// && !algoFact.getObj().equals("?")) {
					numer++;
				}
				// if (!goldFact.getObj().equals("?"))
				denom++;
			}
		}
		if (identifier.equals("R")) {
			for (Map.Entry<FactDao, FactDao> entry : gold.entrySet()) {
				algoFact = algo.get(entry.getKey());
				goldFact = entry.getValue();

				// subjects
				if (algoFact.getSub().equals(goldFact.getSub())) {
					// && !goldFact.getSub().equals("?")) {
					numer++;
				}
				// if (!goldFact.getSub().equals("?"))
				denom++;

				// objects
				if (algoFact.getObj().equals(goldFact.getObj())) {
					// && !goldFact.getObj().equals("?")) {
					numer++;
				}
				// if (!goldFact.getObj().equals("?"))
				denom++;

			}
		}
		return (double) numer / denom;
	}
}
