/**
 * 
 */
package code.dws.experiment;

import gnu.trove.map.hash.THashMap;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Scanner;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * A gold standard creation for property mapping
 * 
 * @author adutta
 * 
 */
public class PropertyGoldStandard {

	// define class logger
	public final static Logger logger = LoggerFactory
			.getLogger(PropertyGoldStandard.class);

	private static final String SAMPLE_OIE_FILE_PATH = "src/main/resources/input/sample.500.csv";
	public static int TOPK_REV_PROPS = 500;
	private static String OIE_FILE_PATH = null;
	private static THashMap<String, Long> COUNT_PROPERTY_INST = new THashMap<String, Long>();
	private static THashMap<String, Long> EMPTY_PROPERTY_MAP = new THashMap<String, Long>();
	private static THashMap<Long, Long> COUNT_FREQUENY = new THashMap<Long, Long>();

	private static Map<String, Long> revbProps = null;

	private static final String HEADER = "http://dbpedia.org/resource/";

	// top-k wikipedia links
	private static final int TOP_K = 5;

	// number of gold standard facts
	private static final int SIZE = 10000;

	/**
	 * 
	 */
	public PropertyGoldStandard() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		if (args.length == 1)
			OIE_FILE_PATH = args[0];

		// READ THE INPUT RAW FILE AND FETCH THE PROPERTIES with atleast k
		// instances
		getReverbProperties(OIE_FILE_PATH, -1, 10L);

		logger.info("Distinct Properties in data set = "
				+ COUNT_PROPERTY_INST.size());

		doFrequencyAnalysis();

		logger.info("Loaded " + COUNT_PROPERTY_INST.size() + " properties");

		// read the file again to randomly select from those finally filtered
		// property
		createGoldStandard();

		for (Entry e : EMPTY_PROPERTY_MAP.entrySet()) {
			logger.info(e.getKey() + "\t" + e.getValue() + "\t"
					+ COUNT_PROPERTY_INST.get(e.getKey()));
		}

	}

	private static void doFrequencyAnalysis() throws IOException {
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				OIE_FILE_PATH).getParent()
				+ "/reverb.properties.instances.distribution.tsv"));

		Long occCount;

		long sum = 0;

		for (Entry<String, Long> e : COUNT_PROPERTY_INST.entrySet()) {
			sum = sum + e.getValue();

			if (COUNT_FREQUENY.containsKey(e.getValue())) {
				occCount = COUNT_FREQUENY.get(e.getValue());
				occCount = occCount + 1;
			} else {
				occCount = 1L;
			}

			COUNT_FREQUENY.put(e.getValue(), occCount);
		}

		logger.info("Number of instances = " + sum);

		for (Entry<Long, Long> e : COUNT_FREQUENY.entrySet()) {
			writer.write(e.getKey() + "\t" + e.getValue() + "\n");
		}
		writer.flush();
		writer.close();
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
	 * 
	 * @return List of properties
	 */
	public static List<String> getReverbProperties(String OIE_FILE,
			int TOPK_REV_PROPS, Long atLeastInstancesCount) {

		String line = null;
		String[] arr = null;
		long val = 0;
		int c = 0;
		List<String> ret = new ArrayList<String>();
		COUNT_PROPERTY_INST = new THashMap<String, Long>();

		try {
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
		// load the properties with atleast 500 instances each
		COUNT_PROPERTY_INST = Utilities.sortByValue(COUNT_PROPERTY_INST,
				atLeastInstancesCount);

		for (Entry<String, Long> e : COUNT_PROPERTY_INST.entrySet()) {
			ret.add(e.getKey());
			c++;
			if (TOPK_REV_PROPS != -1 && c == TOPK_REV_PROPS)
				return ret;
		}

		return ret;
	}

	/**
	 * 
	 * @throws IOException
	 */
	private static void createGoldStandard() throws IOException {
		String line = null;
		String[] arr = null;
		String oieSub = null;
		String oieProp = null;
		String oieObj = null;

		List<String> topkSubjects = null;
		List<String> topkObjects = null;
		List<String> lines = new ArrayList<String>();

		// writing annotation file to
		BufferedWriter writer = new BufferedWriter(new FileWriter(new File(
				OIE_FILE_PATH).getParent()
				+ "/test.gs.reverb.sample."
				+ SIZE
				+ ".csv"));

		// Reading from
		Scanner scan = new Scanner(new File(OIE_FILE_PATH));

		// init DB
		DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		// select the lines from input relevant
		while (scan.hasNextLine()) {
			line = scan.nextLine();
			arr = line.split(";");
			oieProp = arr[1];

			// if this is the selected property, add it
			if (COUNT_PROPERTY_INST.containsKey(oieProp))
				lines.add(line);
		}

		// randomize the list so as to avoid one type of facts in contiguous
		// locations
		Collections.shuffle(lines);

		Random rand = new Random();

		Set<Integer> randomNumSet = new HashSet<Integer>();

		while (randomNumSet.size() < SIZE) {

			Integer randomNum = rand.nextInt(lines.size()) + 1;

			if (!randomNumSet.contains(randomNum)) {

				logger.debug("Reading line " + randomNum);

				line = lines.get(randomNum);

				arr = line.split(";");
				oieSub = Utilities.clean(arr[0]);
				oieProp = arr[1];
				oieObj = Utilities.clean(arr[2]);

				// get top-k candidates of the subject
				topkSubjects = DBWrapper.fetchTopKLinksWikiPrepProb(oieSub,
						TOP_K);

				// get the topk instances for oieObj
				topkObjects = DBWrapper.fetchTopKLinksWikiPrepProb(oieObj,
						TOP_K);

				if (!linkExists(topkSubjects, topkObjects)) {

					randomNumSet.add(randomNum);

					writer.write(oieSub + "\t" + oieProp + "\t" + oieObj + "\t"
							+ "?" + "\t" + "?" + "\t" + "?" + "\t" + "IP\n");
					ioRoutine(oieProp, topkSubjects, topkObjects, writer);
				}

				writer.write("\n");
				writer.flush();
			}

			if (randomNumSet.size() % 1000 == 0)
				logger.info("Completed " + 100
						* ((double) randomNumSet.size() / SIZE) + "%");
		}

		randomNumSet.clear();
		COUNT_PROPERTY_INST.clear();
		writer.close();
		DBWrapper.shutDown();
	}

	private static boolean linkExists(List<String> topkSubjects,
			List<String> topkObjects) {

		String candSubj = null;
		String candObj = null;
		String sparql = null;
		List<QuerySolution> s = null;

		if (topkSubjects == null || topkSubjects.size() == 0)
			return true;

		if (topkObjects == null || topkObjects.size() == 0)
			return true;

		for (String sub : topkSubjects) {
			for (String obj : topkObjects) {

				candSubj = Utilities.utf8ToCharacter(sub.split("\t")[0]);
				candObj = Utilities.utf8ToCharacter(obj.split("\t")[0]);

				try {
					sparql = "select * where {<"
							+ Constants.DBPEDIA_INSTANCE_NS
							+ candSubj
							+ "> ?val <"
							+ Constants.DBPEDIA_INSTANCE_NS
							+ candObj
							+ ">. "
							+ "?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>."
							+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageWikiLink'))}";

					s = SPARQLEndPointQueryAPI.queryDBPediaEndPoint(sparql);

					if (s.size() > 0) {
						// logger.info(candSubj + "\t"
						// + s.get(0).get("val").toString() + "\t"
						// + candObj);
						return true;
					}
				} catch (Exception e) {
					continue;
				}
			}
		}
		return false;
	}

	/**
	 * @param oieProp
	 * @param topkSubjects
	 * @param topkObjects
	 * @param writer
	 * @throws IOException
	 */
	public static void ioRoutine(String oieProp, List<String> topkSubjects,
			List<String> topkObjects, BufferedWriter writer) throws IOException {

		if (topkSubjects.size() > 0 && topkObjects.size() > 0) {

			String candSub = null;
			String candObj = null;

			for (int j = 0; j < ((topkSubjects.size() > topkObjects.size()) ? topkSubjects
					.size() : topkObjects.size()); j++) {

				candSub = (j > topkSubjects.size() - 1) ? "-" : HEADER
						+ topkSubjects.get(j).split("\t")[0];

				candObj = (j > topkObjects.size() - 1) ? "-" : HEADER
						+ topkObjects.get(j).split("\t")[0];

				writer.write("\t" + "\t" + "\t"
						+ Utilities.utf8ToCharacter(candSub) + "\t" + "" + "\t"
						+ Utilities.utf8ToCharacter(candObj) + "\n");
			}
		}

		if (topkSubjects.size() > 0
				&& (topkObjects == null || topkObjects.size() == 0)) {
			for (String candSub : topkSubjects) {
				writer.write("\t" + "\t" + "\t" + HEADER
						+ Utilities.utf8ToCharacter(candSub.split("\t")[0])
						+ "\t" + "-" + "\t" + "-" + "\n");
			}
		}
		if ((topkSubjects == null || topkSubjects.size() == 0)
				&& topkObjects != null) {
			for (String candObj : topkObjects) {
				writer.write("\t" + "\t" + "\t" + "-" + "\t" + "-" + "\t"
						+ HEADER
						+ Utilities.utf8ToCharacter(candObj.split("\t")[0])
						+ "\n");
			}
		}
		if ((topkSubjects == null || topkSubjects.size() == 0)
				&& (topkObjects == null || topkObjects.size() == 0)) {
			writer.write("\t" + "\t" + "\t" + "-" + "\t" + "" + "\t" + "-"
					+ "\n");
		}
	}
}
