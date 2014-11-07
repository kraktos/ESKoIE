/**
 * 
 */
package code.dws.knowGen;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.regression.SimpleRegression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.core.cluster.analysis.CompareClusters;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.FileUtil;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * Responsible for computing the property statistics
 * 
 * @author adutta
 */
public class PropertyStatisticsImproved {
	
	// define class logger
	public final static Logger log = LoggerFactory
			.getLogger(PropertyStatisticsImproved.class);

	// path seperator for the output property files
	public static final String PATH_SEPERATOR = "\t";

	private static boolean INVERSE = false;

	// threshold to consider mappable predicates. It means consider NELL
	// predicates
	// which are atleast x % map-able
	private static final double OIE_PROPERTY_MAPPED_THRESHOLD = 3;

	private static final String PROP_STATS = "src/main/resources/input/PROP_STATISTICS_REVERB_"
			+ (INVERSE ? "INVERSE_" : "DIRECT_")
			+ +OIE_PROPERTY_MAPPED_THRESHOLD + "_.tsv";

	private static final String ITEMS_RULES = "src/main/resources/input/PROP_TRANSC_DIRECT_REVERB.tsv";

	private static final String NEW_TRIPLES = "src/main/resources/input/NEW_TRIPLES_REVERB_"
			+ (INVERSE ? "INVERSE_" : "DIRECT_")
			+ OIE_PROPERTY_MAPPED_THRESHOLD + "_.tsv";

	private static final String DISTRIBUTION_NEW_TRIPLES = "src/main/resources/input/NEW_TRIPLES_REVERB_DOM_RAN_"
			+ (INVERSE ? "INVERSE_" : "DIRECT_")
			+ OIE_PROPERTY_MAPPED_THRESHOLD + "_.tsv";

	private static Map<String, Map<String, Map<Pair<String, String>, Long>>> GLOBAL_TRANSCS_MAP = new HashMap<String, Map<String, Map<Pair<String, String>, Long>>>();

	// tolerance of error, 1.1 means 10%
	private static final double ERROR_TOLERANCE = 1.1;

	// total triples that can be reconstructed
	private static int newTriples = 0;

	// map to hold the nell properties and the equivalent DBpedia properties
	// key is the nell property, value is a map with the dbp property and the
	// corresponding count
	private static Map<String, Map<String, Integer>> MAP_OIE_IE_PROP_COUNTS = new HashMap<String, Map<String, Integer>>();

	/**
	 * collection to store the final mapped tripels from NELL to dbpedia
	 */
	public static Map<String, List<String>> FINAL_MAPPINGS = new HashMap<String, List<String>>();

	static DecimalFormat twoDForm = new DecimalFormat("#.######");

	static SimpleRegression regression = new SimpleRegression(true);

	// static OLSMultipleLinearRegression regression2 = new
	// OLSMultipleLinearRegression();

	// map keeping count of the nell predicate occurrence, should be identical
	// to
	// the number of triples with that
	// property in the raw input file
	private static Map<String, Integer> MAP_PRED_COUNT = new HashMap<String, Integer>();

	private static Map<String, String> CACHED_SUBCLASSES = new HashMap<String, String>();

	/**
	 * entry point
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		String inputLog = null;
		Map<String, String> clusterNames = new HashMap<String, String>();

		Constants.loadConfigParameters(new String[] { "", args[0] });

		buildClassHierarchy();

		GenerateNewProperties.init(args[0]);

		if (!INVERSE)
			inputLog = GenerateNewProperties.DIRECT_PROP_LOG;
		else
			inputLog = GenerateNewProperties.INVERSE_PROP_LOG;

		try {
			newTriples = 0;

			// for REverb, the property names will be cluster names
			if (!Constants.IS_NELL && !Constants.WORKFLOW_NORMAL)
				clusterNames = getClusterNames(clusterNames);

			PropertyStatisticsImproved.run(inputLog, clusterNames);

			if (OIE_PROPERTY_MAPPED_THRESHOLD > 0)
				PropertyStatisticsImproved.createNewTriples(inputLog,
						clusterNames);
		} finally {

			MAP_PRED_COUNT.clear();
			MAP_OIE_IE_PROP_COUNTS.clear();
		}
	}

	/**
	 * map a given Reverb property phrase to its cluster name
	 * 
	 * @param clusterNames
	 * @return
	 */
	private static Map<String, String> getClusterNames(
			Map<String, String> clusterNames) {
		String line = null;
		String[] arr = null;
		List<String> list = new ArrayList<String>();

		if (Constants.WORKFLOW == 2) {
			try {
				CompareClusters.main(new String[] { "" });
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			Map<String, List<String>> propertyClusterNames = CompareClusters
					.getCluster();

			for (Map.Entry<String, List<String>> entry : propertyClusterNames
					.entrySet()) {
				for (String s : entry.getValue()) {
					clusterNames.put(s, entry.getKey());
				}
			}

		} else if (Constants.WORKFLOW == 3) {
			try {
				@SuppressWarnings("resource")
				Scanner scan = new Scanner(new File(
						"src/main/resources/input/cluster.names.out"), "UTF-8");

				// iterate the file
				while (scan.hasNextLine()) {
					line = scan.nextLine();
					line = line.replaceAll("\\[", "").replaceAll("\\]", "");
					arr = line.split("\t");
					list = new ArrayList<String>();
					for (int i = 1; i < arr.length; i++) {
						clusterNames.put(arr[i], arr[0]);
					}
				}
			} catch (FileNotFoundException e) {
				log.error(e.getMessage());
			}
		}

		return clusterNames;
	}

	/**
	 * USE THE MAPPED PROPERTY, AND MAPPED INSTANCES TO GENERATE NEW-TRIPLES
	 * FROM THE NON-MAPPED CASES
	 * 
	 * @param filePath
	 * @param clusterNames
	 * @throws IOException
	 */
	private static void createNewTriples(String filePath,
			Map<String, String> clusterNames) throws IOException {
		int cnt = 0;
		// nell property in concern
		String oieProp = null;

		// write transactions to the file for analysis
		BufferedWriter triplesWriter = new BufferedWriter(new FileWriter(
				NEW_TRIPLES));
		BufferedWriter statStriplesWriter = new BufferedWriter(new FileWriter(
				DISTRIBUTION_NEW_TRIPLES));

		// read the file into memory
		ArrayList<ArrayList<String>> directPropsFile = FileUtil
				.genericFileReader(new FileInputStream(filePath),
						PATH_SEPERATOR, false);

		// init DB for getting the most frequebt URI for the NELL terms

		// MOST FREQUENT CASE
		// DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		// REFINED CASE
		DBWrapper.init(Constants.GET_REFINED_MAPPINGS_SQL);

		// iterate through them
		for (ArrayList<String> line : directPropsFile) {
			oieProp = line.get(1);

			if (!Constants.IS_NELL && !Constants.WORKFLOW_NORMAL)
				oieProp = clusterNames.get(oieProp);

			if (line.size() == 3) {

				if (FINAL_MAPPINGS.containsKey(oieProp)) {
					cnt++;

					List<String> dbProps = FINAL_MAPPINGS.get(oieProp);

					reCreateTriples(dbProps, line, triplesWriter,
							statStriplesWriter);
				}
			}
		}

		triplesWriter.close();
		statStriplesWriter.close();
	}

	public static void reCreateTriples(List<String> dbProps,
			ArrayList<String> line, BufferedWriter triplesWriter,
			BufferedWriter statStriplesWriter) throws IOException {
		String domainType = null;
		String rangeType = null;

		String oieRawSubj = null;
		String oieRawProp = null;
		String oieRawObj = null;

		List<String> candidateSubjs = null;
		List<String> candidateObjs = null;
		List<String> candidates = null;

		// get the nell subjects and objects
		oieRawSubj = line.get(0);
		oieRawProp = line.get(1);
		oieRawObj = line.get(2);

		// get the top-k concepts for the subject
		// candidateSubjs = DBWrapper.fetchTopKLinksWikiPrepProb(Utilities
		// .cleanse(nellRawSubj).replaceAll("\\_+", " ").trim(),
		// SAMEAS_TOPK);
		//
		// // get the top-k concepts for the object
		// candidateObjs = DBWrapper.fetchTopKLinksWikiPrepProb(
		// Utilities.cleanse(nellRawObj).replaceAll("\\_+", " ").trim(),
		// SAMEAS_TOPK);

		candidates = DBWrapper.fetchRefinedMapping(Utilities
				.cleanse(oieRawSubj).trim(), (Constants.IS_NELL) ? oieRawProp
				.trim().replaceAll("\\s+", "_") : oieRawProp.trim(), Utilities
				.cleanse(oieRawObj).trim());

		try {
			candidateSubjs = new ArrayList<String>();
			if (candidates.size() > 0 && candidates.get(0) != null)
				candidateSubjs.add(candidates.get(0));
			else
				candidateSubjs.add("X");

			candidateObjs = new ArrayList<String>();
			if (candidates.size() > 1 && candidates.get(1) != null)
				candidateObjs.add(candidates.get(1));
			else
				candidateObjs.add("X");

		} catch (IndexOutOfBoundsException e) {
			e.printStackTrace();
		}

		try {
			if ((candidateSubjs.get(0) != null && !candidateSubjs.get(0)
					.equals("X"))
					&& (candidateObjs.get(0) != null && !candidateObjs.get(0)
							.equals("X"))) {

				try {
					// find domain type
					domainType = getTypeInfo(candidateSubjs.get(0).split("\t")[0]);

					// find range type
					rangeType = getTypeInfo(candidateObjs.get(0).split("\t")[0]);

					if (!INVERSE)
						shoudBeIn(dbProps, domainType, rangeType, line,
								triplesWriter, statStriplesWriter,
								candidateSubjs.get(0).split("\t")[0],
								candidateObjs.get(0).split("\t")[0]);
					else
						shoudBeIn(dbProps, rangeType, domainType, line,
								triplesWriter, statStriplesWriter,
								candidateObjs.get(0).split("\t")[0],
								candidateSubjs.get(0).split("\t")[0]);

				} catch (Exception e) {
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * does the domain and range of this mapped triple satisfy the allowed
	 * dbprop domain range
	 * 
	 * @param dbProps
	 * @param domainType
	 * @param rangeType
	 * @param triplesWriter
	 * @param line
	 * @param dbpObj
	 * @param dbpSub
	 * @return
	 * @throws IOException
	 */
	private static void shoudBeIn(List<String> dbProps, String domainType,
			String rangeType, ArrayList<String> line,
			BufferedWriter triplesWriter, BufferedWriter statStriplesWriter,
			String dbpSub, String dbpObj) throws IOException {
		String allowedDomain;
		String allowedRange;

		for (String dbprop : dbProps) {

			allowedDomain = null;
			allowedRange = null;

			try {
				allowedDomain = SPARQLEndPointQueryAPI
						.queryDBPediaEndPoint(
								"select ?dom where {<"
										+ dbprop
										+ "> <http://www.w3.org/2000/01/rdf-schema#domain> ?dom}")
						.get(0).get("dom").toString();

				allowedDomain = allowedDomain.replaceAll(
						Constants.ONTOLOGY_NAMESPACE, "");

			} catch (Exception e) {
				// allowedDomain = "XX";
			}
			try {
				allowedRange = SPARQLEndPointQueryAPI
						.queryDBPediaEndPoint(
								"select ?ran where {<"
										+ dbprop
										+ "> <http://www.w3.org/2000/01/rdf-schema#range> ?ran}")
						.get(0).get("ran").toString();

				allowedRange = allowedRange.replaceAll(
						Constants.ONTOLOGY_NAMESPACE, "");

			} catch (Exception e) {
				// allowedRange = "XX";
			}

			// all good case
			if (isSuperClass3(allowedDomain, domainType)
					&& isSuperClass3(allowedRange, rangeType)) {
				triplesWriter.write(line.get(0) + "\t" + line.get(1) + "\t"
						+ line.get(2) + "\t" + Constants.DBPEDIA_INSTANCE_NS
						+ Utilities.utf8ToCharacter(dbpSub) + "\t" + dbprop
						+ "\t" + Constants.DBPEDIA_INSTANCE_NS
						+ Utilities.utf8ToCharacter(dbpObj) + "\n");
				triplesWriter.flush();

				statStriplesWriter.write(line.get(1) + "\t" + domainType + "\t"
						+ allowedDomain + "\t" + dbprop + "\t" + rangeType
						+ "\t" + allowedRange + "\n");

				statStriplesWriter.flush();
			} else {
				statStriplesWriter.write(line.get(1) + "~!" + "\t" + domainType
						+ "\t" + allowedDomain + "\t" + dbprop + "\t"
						+ rangeType + "\t" + allowedRange + "\n");

				statStriplesWriter.flush();
			}
		}

	}

	private static String getTypeInfo(String inst) {
		String mostSpecificVal = null;

		List<String> types = SPARQLEndPointQueryAPI.getInstanceTypes(Utilities
				.utf8ToCharacter(inst));

		try {
			mostSpecificVal = SPARQLEndPointQueryAPI.getLowestType(types)
					.get(0);
		} catch (IndexOutOfBoundsException e) {
		} finally {
			return mostSpecificVal;
		}
	}

	/**
	 * method to read the property distribution files in memory
	 * 
	 * @param clusterNames
	 * 
	 * @throws IOException
	 */
	public static void run(String filePath, Map<String, String> clusterNames)
			throws IOException {

		double percentageMapped = 0D;

		int blankMapCntr = 0;
		int nonBlankMapCntr = 0;

		// nell property in concern
		String oieProp = null;

		// read the file into memory
		ArrayList<ArrayList<String>> directPropsFile = FileUtil
				.genericFileReader(new FileInputStream(filePath),
						PATH_SEPERATOR, false);

		// write transactions to the file for analysis
		BufferedWriter itemsWriter = new BufferedWriter(new FileWriter(
				ITEMS_RULES));

		// writer to dump the property stats
		BufferedWriter propStatsWriter = new BufferedWriter(new FileWriter(
				PROP_STATS));

		List<String> possibleProps = null;
		List<String> possibleTypes = null;

		// iterate through them
		for (ArrayList<String> line : directPropsFile) {

			oieProp = line.get(1);

			if (!Constants.IS_NELL && !Constants.WORKFLOW_NORMAL)
				oieProp = clusterNames.get(oieProp);

			// for the clustered scenario, the property names are the cluster
			// names
			if (oieProp != null) {

				if (line.size() == 3) {
					blankMapCntr++;
					updateMapValues(oieProp, "NA");
				} else { // cases which could be mapped
					possibleProps = new ArrayList<String>();
					possibleTypes = new ArrayList<String>();

					nonBlankMapCntr++;

					for (int cnt = 3; cnt < line.size(); cnt++) {
						if (line.get(cnt)
								.contains(Constants.ONTOLOGY_NAMESPACE)) {
							possibleProps.add(line.get(cnt));
							updateMapValues(oieProp, line.get(cnt));
						} else {
							possibleTypes.add(line.get(cnt));
						}
					}

					// small routine to dump separately the property
					// transactions with classes associate
					try {
						for (String prop : possibleProps) {
							itemsWriter.write(oieProp + "\t" + prop + "\t"
									+ possibleTypes.get(0) + "\t"
									+ possibleTypes.get(1) + "\n");
						}
					} catch (Exception e) {
						log.error("Problem with line = " + line);
					}
					possibleProps.clear();
				}

				// update the count of the occurrence of this predicate
				updateCount(oieProp);
			}

			itemsWriter.flush();
		}

		loadPropDistributionInCollection();

		// read the transactions and create the tau values for each
		// association..
		// this prepares the regression model

		// DEBUG POINT, print the distribution
		for (Map.Entry<String, Map<String, Map<Pair<String, String>, Long>>> entry : GLOBAL_TRANSCS_MAP
				.entrySet()) {
			double probMax = 0D;

			long oiePropCount = getOiePropCount(entry.getKey());

			// compute the mappable ratio for this predicate
			percentageMapped = 100 * (1 - ((double) MAP_OIE_IE_PROP_COUNTS.get(
					entry.getKey()).get("NA") / oiePropCount));

			for (Map.Entry<String, Map<Pair<String, String>, Long>> nellVal : entry
					.getValue().entrySet()) {

				long nellDbpPredCount = getNellAndDbpPropCount(entry.getKey(),
						nellVal.getKey());

				for (Map.Entry<Pair<String, String>, Long> pairs : nellVal
						.getValue().entrySet()) {

					double support = (double) getNellAndBothDbpTypeCount(
							entry.getKey(), pairs.getKey().getLeft(), pairs
									.getKey().getRight())
							/ oiePropCount; // domProb * ranProb * countProb;

					// look for the max probability of two classes occurring
					// together
					if (support > probMax) {
						probMax = support;
					}

					log.debug(entry.getKey()
							+ "("
							+ oiePropCount
							+ ")\t"
							+ nellVal.getKey()
							+ "("
							+ nellDbpPredCount
							+ ")\t"
							+ pairs.getKey().getLeft()
							+ "("
							+ getNellAndDbpTypeCount(entry.getKey(), pairs
									.getKey().getLeft(), true)
							+ ")\t"
							+ pairs.getKey().getRight()
							+ "("
							+ getNellAndDbpTypeCount(entry.getKey(), pairs
									.getKey().getRight(), false) + ")\t"
							+ pairs.getValue() + "\t" + support);
				}
			}

			double tau = (double) (MAP_OIE_IE_PROP_COUNTS.get(entry.getKey())
					.get("NA")) / (oiePropCount * probMax);

			log.debug(entry.getKey() + "(" + getOiePropCount(entry.getKey())
					+ ")" + "\tNA\t"
					+ MAP_OIE_IE_PROP_COUNTS.get(entry.getKey()).get("NA")
					+ "\t" + probMax + "\t" + tau);

			log.info(entry.getKey() + "\t" + percentageMapped);

			if (percentageMapped >= OIE_PROPERTY_MAPPED_THRESHOLD) {
				// train the regression model by feedin the data observed by the

				// underlying data set
				// adding maximum tau for the nell property to the regression
				// model
				regression.addData(
						Double.valueOf(twoDForm.format(percentageMapped)),
						Double.valueOf(twoDForm.format(tau)));

				// THIS IS A FILE WHICH WRITES OUT THE k VS TAU VALUES
				propStatsWriter.write(Double.valueOf(twoDForm
						.format(percentageMapped))
						+ "\t"
						+ Double.valueOf(twoDForm.format(tau)) + "\n");
			}
		}

		// regression.addData(100, 0);
		propStatsWriter.write("100\t0");

		// apply selection based on the underlying regression line
		// THIS NOW USES THE TRAINED REGRESSION MODEL TO FILTER PROPERTIES
		// FALLING BEYOND THE ALLOWABLE PREDICTED REGRESSION VALUES
		performPredicateBasedAnalysis();

		// some stats
		log.info("TOTAL TRIPLES = " + directPropsFile.size());

		log.info("TOTAL NON-MAPABLE TRIPLES = "
				+ blankMapCntr
				+ " i.e "
				+ Math.round((double) (blankMapCntr * 100)
						/ directPropsFile.size()) + "%");

		log.info("TOTAL MAPPED TRIPLES = "
				+ nonBlankMapCntr
				+ " i.e "
				+ Math.round((double) (100 * nonBlankMapCntr)
						/ directPropsFile.size()) + "%");

		log.info("NEW TRIPLES THAT CAN BE GENERATED = " + newTriples + "("
				+ 100 * (double) newTriples / blankMapCntr + "%)\n\n");

		log.info("TOTAL PROPERTIES = " + MAP_OIE_IE_PROP_COUNTS.size() + "\n\n");

		for (Entry<String, List<String>> entry : FINAL_MAPPINGS.entrySet()) {
			log.debug(entry.getKey() + "\t" + entry.getValue());

		}

		itemsWriter.close();
		propStatsWriter.flush();
		propStatsWriter.close();

	}

	/**
	 * loads the entire property distribution of oie over dbpedia in a
	 * collection
	 * 
	 * @throws FileNotFoundException
	 */
	private static void loadPropDistributionInCollection()
			throws FileNotFoundException {

		// read the file into memory
		ArrayList<ArrayList<String>> propRules = FileUtil.genericFileReader(
				new FileInputStream(ITEMS_RULES), PATH_SEPERATOR, false);

		String oieProp = null;
		String dbProp = null;

		String dom = null;
		String ran = null;
		long count = 0;

		for (ArrayList<String> line : propRules) {
			oieProp = line.get(0);
			dbProp = line.get(1);
			dom = line.get(2);
			ran = line.get(3);

			Map<String, Map<Pair<String, String>, Long>> oiePropMap = null;
			Map<Pair<String, String>, Long> dbpPropMap = null;

			Pair<String, String> pair = new ImmutablePair<String, String>(dom,
					ran);

			if (GLOBAL_TRANSCS_MAP.containsKey(oieProp)) {
				oiePropMap = GLOBAL_TRANSCS_MAP.get(oieProp);

				if (oiePropMap.containsKey(dbProp)) {
					dbpPropMap = oiePropMap.get(dbProp);

					if (dbpPropMap.containsKey(pair)) {
						count = dbpPropMap.get(pair);
						count++;
						dbpPropMap.put(pair, count);
					} else {
						dbpPropMap.put(pair, 1L);
					}
				} else {
					dbpPropMap = new HashMap<Pair<String, String>, Long>();
					dbpPropMap.put(pair, 1L);
				}
			} else {
				oiePropMap = new HashMap<String, Map<Pair<String, String>, Long>>();
				dbpPropMap = new HashMap<Pair<String, String>, Long>();
				dbpPropMap.put(new ImmutablePair<String, String>(dom, ran), 1L);
			}
			oiePropMap.put(dbProp, dbpPropMap);

			GLOBAL_TRANSCS_MAP.put(oieProp, oiePropMap);
		}

	}

	/**
	 * update the nell pred counts in the whole raw NEll file
	 * 
	 * @param nellProp
	 */
	private static void updateCount(String nellProp) {
		int count = 0;

		if (!MAP_PRED_COUNT.containsKey(nellProp)) {
			count = 1;
		} else {
			count = MAP_PRED_COUNT.get(nellProp);
			count++;
		}
		MAP_PRED_COUNT.put(nellProp, count);

	}

	/**
	 * iterate all the predicates stored in memory and find the statistics for
	 * each
	 * 
	 * @throws IOException
	 */
	private static void performPredicateBasedAnalysis() throws IOException {

		double percentageMapped = 0D;
		double tau = 0D;

		// iterate over nell properties
		for (Map.Entry<String, Map<String, Map<Pair<String, String>, Long>>> entry : GLOBAL_TRANSCS_MAP
				.entrySet()) {

			long nellPredCount = getOiePropCount(entry.getKey());

			// compute the mappable value for this predicate
			percentageMapped = 100 * (1 - ((double) MAP_OIE_IE_PROP_COUNTS.get(
					entry.getKey()).get("NA") / nellPredCount));

			// iterate over dbpedia properties
			for (Map.Entry<String, Map<Pair<String, String>, Long>> nellVal : entry
					.getValue().entrySet()) {

				long nellDbpPredCount = getNellAndDbpPropCount(entry.getKey(),
						nellVal.getKey());

				// iterate over all possible class types
				for (Map.Entry<Pair<String, String>, Long> pairs : nellVal
						.getValue().entrySet()) {

					double jointProb = (double) pairs.getValue()
							/ nellPredCount;

					jointProb = (double) getNellAndBothDbpTypeCount(
							entry.getKey(), pairs.getKey().getLeft(), pairs
									.getKey().getRight())
							/ nellPredCount; // domProb * ranProb * countProb;

					tau = (double) MAP_OIE_IE_PROP_COUNTS.get(entry.getKey())
							.get("NA") / (nellPredCount * jointProb);

					log.debug(entry.getKey()
							+ "("
							+ nellPredCount
							+ ")\t"
							+ nellVal.getKey()
							+ "("
							+ nellDbpPredCount
							+ ")\t"
							+ pairs.getKey().getLeft()
							+ "("
							+ getNellAndDbpTypeCount(entry.getKey(), pairs
									.getKey().getLeft(), true)
							+ ")\t"
							+ pairs.getKey().getRight()
							+ "("
							+ getNellAndDbpTypeCount(entry.getKey(), pairs
									.getKey().getRight(), false) + ")\t"
							+ pairs.getValue() + "\t" + tau + "\t"
							+ Math.round(percentageMapped) + "%\t"
							+ regression.predict(percentageMapped));

					// FINALLY SELECTED PROPERTIES
					if (tau <= ERROR_TOLERANCE
							* Math.abs(regression.predict(percentageMapped))) {
						// store in memory
						storeMappings(entry.getKey(), nellVal.getKey());
					}
				}
			}

			if (FINAL_MAPPINGS.containsKey(entry.getKey())) {
				log.debug("GENERATING " + percentageMapped + " cases for "
						+ entry.getKey() + " with "
						+ MAP_OIE_IE_PROP_COUNTS.get(entry.getKey()).get("NA"));

				int numProps = FINAL_MAPPINGS.get(entry.getKey()).size();
				newTriples = newTriples
						+ MAP_OIE_IE_PROP_COUNTS.get(entry.getKey()).get("NA");
			}
		}
	}

	/**
	 * store the finally learnt property mappings
	 * 
	 * @param oie
	 *            property
	 * @param dbpedia
	 *            property
	 * @return
	 */
	private static int storeMappings(String oieProp, String dbpProp) {
		List<String> possibleCands = null;
		if (FINAL_MAPPINGS.containsKey(oieProp)) {
			possibleCands = FINAL_MAPPINGS.get(oieProp);
			if (!possibleCands.contains(dbpProp))
				possibleCands.add(dbpProp);
		} else {
			possibleCands = new ArrayList<String>();
			possibleCands.add(dbpProp);
		}
		// possibleCands = filterGeneralMostProperties2(possibleCands);
		FINAL_MAPPINGS.put(oieProp, possibleCands);

		return possibleCands.size();
	}

	/*
	 * update the prop counts
	 */
	private static void updateMapValues(String oieProp, String dbProp) {

		Map<String, Integer> mapValues = null;
		dbProp = dbProp.replaceAll(Constants.DBPEDIA_CONCEPT_NS, "dbo:");

		if (!MAP_OIE_IE_PROP_COUNTS.containsKey(oieProp)) { // no key inserted
															// for nell
															// prop, create
															// one entry
			mapValues = new HashMap<String, Integer>();
			mapValues.put(dbProp, 1);
		} else { // if nell prop key exists

			// retrieve the existing collection first
			mapValues = MAP_OIE_IE_PROP_COUNTS.get(oieProp);

			// check and update the count of the dbprop values
			if (!mapValues.containsKey(dbProp)) {
				mapValues.put(dbProp, 1);
			} else {
				int val = mapValues.get(dbProp);
				mapValues.put(dbProp, val + 1);
			}
		}

		MAP_OIE_IE_PROP_COUNTS.put(oieProp, mapValues);
	}

	/**
	 * number of itemsets wher a nell and dbp property occur
	 * 
	 * @param nellProp
	 * @param dbpProp
	 * @return
	 */
	private static long getNellAndDbpPropCount(String nellProp, String dbpProp) {
		long val = 0;

		for (Map.Entry<String, Map<String, Map<Pair<String, String>, Long>>> entry : GLOBAL_TRANSCS_MAP
				.entrySet()) {

			if (entry.getKey().equals(nellProp)) {
				for (Map.Entry<String, Map<Pair<String, String>, Long>> nellVal : entry
						.getValue().entrySet()) {
					if (nellVal.getKey().equals(dbpProp)) {
						for (Map.Entry<Pair<String, String>, Long> pairs : nellVal
								.getValue().entrySet()) {
							val = val + pairs.getValue();
						}
					}
				}
			}
		}

		return val;
	}

	/**
	 * number of itemsets where the given nell prop occurs
	 * 
	 * @param oieProp
	 * @return
	 */
	private static long getOiePropCount(String oieProp) {
		long val = 0;

		for (Map.Entry<String, Map<String, Map<Pair<String, String>, Long>>> entry : GLOBAL_TRANSCS_MAP
				.entrySet()) {

			if (entry.getKey().equals(oieProp)) {
				for (Map.Entry<String, Map<Pair<String, String>, Long>> nellVal : entry
						.getValue().entrySet()) {
					for (Map.Entry<Pair<String, String>, Long> pairs : nellVal
							.getValue().entrySet()) {
						val = val + pairs.getValue();
					}
				}
			}
		}

		return val + MAP_OIE_IE_PROP_COUNTS.get(oieProp).get("NA");
	}

	/**
	 * number of itemsets where the nell propertz and a given type (domain or
	 * range ) occurs
	 * 
	 * @param nellProp
	 * @param type
	 * @param isDomain
	 * @return
	 */
	private static long getNellAndDbpTypeCount(String nellProp, String type,
			boolean isDomain) {
		long val = 0;

		boolean flag = false;

		for (Map.Entry<String, Map<String, Map<Pair<String, String>, Long>>> entry : GLOBAL_TRANSCS_MAP
				.entrySet()) {

			if (entry.getKey().equals(nellProp)) {
				for (Map.Entry<String, Map<Pair<String, String>, Long>> nellVal : entry
						.getValue().entrySet()) {
					for (Map.Entry<Pair<String, String>, Long> pairs : nellVal
							.getValue().entrySet()) {
						flag = (isDomain) ? (pairs.getKey().getLeft()
								.equals(type)) : (pairs.getKey().getRight()
								.equals(type));
						if (flag) {
							val = val + pairs.getValue();
						}
					}
				}
			}
		}

		return val;
	}

	/**
	 * number of itemsets where the nell property and a given type (domain or
	 * range ) occurs
	 * 
	 * @param nellProp
	 * @param type
	 * @param isDomain
	 * @return
	 */
	private static long getNellAndBothDbpTypeCount(String nellProp,
			String domain, String range) {
		long val = 0;

		boolean flag = false;

		for (Map.Entry<String, Map<String, Map<Pair<String, String>, Long>>> entry : GLOBAL_TRANSCS_MAP
				.entrySet()) {

			if (entry.getKey().equals(nellProp)) {
				for (Map.Entry<String, Map<Pair<String, String>, Long>> nellVal : entry
						.getValue().entrySet()) {
					for (Map.Entry<Pair<String, String>, Long> pairs : nellVal
							.getValue().entrySet()) {
						flag = (isSuperClass(domain, pairs.getKey().getLeft()))
								&& isSuperClass(range, pairs.getKey()
										.getRight());

						if (flag) {
							val = val + pairs.getValue();
						}
					}
				}
			}
		}

		return val;
	}

	private static boolean isSuperClass3(String generalClass,
			String particularClass) {

		if (generalClass == null)
			return true;

		if (generalClass == null && particularClass == null)
			return true;

		// if (particularClass == null)
		// return true;

		if (generalClass.equals(particularClass))
			return true;

		List<String> trailCol = new ArrayList<String>();
		List<String> allSuperClasses = getAllMyParents(particularClass,
				trailCol);
		log.debug("SUPER CLASSES of " + particularClass + " = "
				+ allSuperClasses.toString());
		if (allSuperClasses.contains(generalClass))
			return true;

		return false;

	}

	private static boolean isSuperClass(String generalClass,
			String particularClass) {

		if (generalClass.equals(particularClass))
			return true;

		// if (generalClass == null || particularClass == null)
		// return true;

		List<String> trailCol = new ArrayList<String>();
		List<String> allSuperClasses = getAllMyParents(particularClass,
				trailCol);
		log.debug("SUPER CLASSES of " + particularClass + " = "
				+ allSuperClasses.toString());
		if (allSuperClasses.contains(generalClass))
			return true;

		return false;
	}

	private static List<String> getAllMyParents(String particularClass,
			List<String> coll) {
		String superCls = CACHED_SUBCLASSES.get(particularClass);
		if (CACHED_SUBCLASSES.containsKey(superCls)) {
			coll.add(superCls);
			getAllMyParents(superCls, coll);
		} else {
			coll.add(superCls);
		}
		return coll;
	}

	public static void buildClassHierarchy() {
		String getAll = "SELECT * WHERE  { ?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> ?superclass}";
		List<QuerySolution> allPairs = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(getAll);

		for (QuerySolution querySol : allPairs) {
			// Get the next result row
			// QuerySolution querySol = results.next();
			if (querySol.get("subclass").toString()
					.indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1
					&& querySol.get("superclass").toString()
							.indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1) {

				CACHED_SUBCLASSES.put(
						querySol.get("subclass").toString()
								.replaceAll(Constants.DBPEDIA_CONCEPT_NS, ""),
						querySol.get("superclass").toString()
								.replaceAll(Constants.DBPEDIA_CONCEPT_NS, ""));
			}
		}

		log.debug(CACHED_SUBCLASSES.toString());
	}
}
