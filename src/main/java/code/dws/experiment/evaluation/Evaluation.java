package code.dws.experiment.evaluation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import code.dws.utils.FileUtil;

public class Evaluation {

	// define Logger
	static Logger logger = Logger.getLogger(Evaluation.class.getName());

	private static final String GOLD_FILE = "/input/ReverbGS1.tsv";
	private static final String METHOD_FILE = "";

	public Evaluation() {
	}

	public static void main(String[] args) {

		Map<FactDao, FactDao> gold = loadGoldFile(GOLD_FILE);
		logger.info(gold.size());
		for (Entry<FactDao, FactDao> fact : gold.entrySet()) {
			logger.info(fact.toString());
		}

		List<FactDao> method = loadMethodOutputFile(METHOD_FILE);

//		compare(gold, method);
	}

	private static void compare(List<FactDao> gold, List<FactDao> method) {

	}

	private static Map<FactDao, FactDao> loadGoldFile(String goldFile) {

		Map<FactDao, FactDao> goldFacts = new HashMap<FactDao, FactDao>();

		// load the NELL file in memory as a collection
		ArrayList<ArrayList<String>> gold = FileUtil.genericFileReader(
				Evaluation.class.getResourceAsStream(goldFile), "\t", false);

		for (ArrayList<String> line : gold) {

			if (line.size() >= 6
					&& (!(line.get(3).equals("?") && line.get(5).equals("?")))) {

				if (line.get(5).equals("IP"))
					System.out.println();

				goldFacts.put(
						new FactDao(line.get(0), line.get(1), line.get(2)),
						new FactDao(line.get(3), line.get(4), line.get(5)));

			}

		}
		return goldFacts;
	}

	private static List<FactDao> loadMethodOutputFile(String methodFile) {

		return null;
	}

}
