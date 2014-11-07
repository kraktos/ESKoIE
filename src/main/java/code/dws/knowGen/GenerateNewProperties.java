/**
 * 
 */
package code.dws.knowGen;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.log4j.Logger;

import code.dws.core.cluster.analysis.CompareClusters;
import code.dws.dbConnectivity.DBWrapper;
import code.dws.experiment.ExperimentAutomation;
import code.dws.experiment.PropertyGoldStandard;
import code.dws.markovLogic.YagoDbpediaMapping;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * This takes in the property hopped log files and tries to generate new
 * properties
 * 
 * @author Arnab Dutta
 */
public class GenerateNewProperties {

	public static String INVERSE_PROP_LOG = null;

	public static String DIRECT_PROP_LOG = null;

	// define Logger
	public static Logger log = Logger.getLogger(GenerateNewProperties.class
			.getName());

	private static final String PROPERTY_LOGS_PATH = "./src/main/resources/output";

	// location of the raw downloaded NELL path
	public static final String NELL_FILE_PATH = "/input/Nell_truncated.csv";

	// //"/input/small.csv";

	// data separator of the NELL data file
	private static final String PATH_SEPERATOR = (Constants.IS_NELL) ? ","
			: ";";

	// defines the top-k candidate to be fetched for each NELL term
	private static final int SAMEAS_TOPK = 1;

	private static Map<String, List<String>> GLOBAL_PROPERTY_MAPPINGS = new HashMap<String, List<String>>();

	// flag to determine the property fetch mode. If set to false, then just
	// queries the sparql endpoint, else looks for sophisticated
	// graph exploratory search modes.
	private static boolean multiHop = false;

	private static boolean refinedMode = true;

	private static Map<String, List<String>> propertyClusterNames;
	private static List<String> propertyNames = new ArrayList<String>();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		init(args[0]);

		// if (Constants.OIE_IS_NELL)
		// readFiles(NELL_FILE_PATH);
		// else
		readFiles(Constants.OIE_DATA_PATH);

	}

	/**
	 * @throws IOException
	 * 
	 */
	public static void init(String configFile) throws IOException {

		Constants.loadConfigParameters(new String[] { "", configFile });

		// initiate different files depending on the condition
		if (multiHop) {
			INVERSE_PROP_LOG = "INVERSE_PROP_MULTI.log";
			DIRECT_PROP_LOG = "DIRECT_PROP_MULTI.log";
		} else {
			INVERSE_PROP_LOG = (Constants.IS_NELL) ? "src/main/resources/input/INVERSE_PROP.log"
					: "src/main/resources/input/REVERB_INVERSE_PROP.log";
			DIRECT_PROP_LOG = (Constants.IS_NELL) ? "src/main/resources/input/DIRECT_PROP.log"
					: "src/main/resources/input/REVERB_DIRECT_PROP.log";
		}

		// initiate yago info
		if (Constants.INCLUDE_YAGO_TYPES)
			YagoDbpediaMapping.main(new String[] { "" });

		if (!Constants.WORKFLOW_NORMAL) {
			if (Constants.WORKFLOW == 2) {
				CompareClusters.main(new String[] { "" });
				log.info("Optimal Inflation for workflow " + Constants.WORKFLOW
						+ " = " + CompareClusters.getOptimalInflation());

				// retrieve only the properties relevant to the given cluster
				// name
				propertyClusterNames = CompareClusters.getCluster();
				log.info(propertyClusterNames.size());

			}

		} else { // normal workflow, without cluster
			List<String> props = PropertyGoldStandard.getReverbProperties(
					Constants.OIE_DATA_PATH, 500,
					Long.parseLong(Constants.INSTANCE_THRESHOLD));

			for (String s : props) {
				propertyNames.add(s);
			}
		}

		log.info("total properties added = " + propertyNames.size());
	}

	/**
	 * read the NELL file to extract the subject-predicate-object
	 * 
	 * @param oieFilePath
	 * @throws IOException
	 */
	public static void readFiles(String oieFilePath) throws IOException {

		int lineCounter = 0;

		String line = null;
		String oieRawSubj = null;
		String oieRawProp = null;
		String oieRawObj = null;

		String[] elems = null;

		List<String> candidates = null;
		List<String> candidateSubjs = null;
		List<String> candidateObjs = null;

		BufferedWriter directPropWriter = new BufferedWriter(new FileWriter(
				DIRECT_PROP_LOG));
		BufferedWriter inversePropWriter = new BufferedWriter(new FileWriter(
				INVERSE_PROP_LOG));

		// init DB for getting the most frequent URI for the NELL terms
		if (!refinedMode)
			DBWrapper.init(Constants.GET_WIKI_LINKS_APRIORI_SQL);

		// another run mode with refined mapping, not just top-1
		if (refinedMode)
			DBWrapper.init(Constants.GET_REFINED_MAPPINGS_SQL);

		@SuppressWarnings("resource")
		Scanner scan = new Scanner(new File(oieFilePath), "UTF-8");

		// iterate the file
		while (scan.hasNextLine()) {
			line = scan.nextLine();

			elems = line.split(PATH_SEPERATOR);
			try {
				log.debug(elems[0] + "\t" + elems[1] + "\t" + elems[2] + "\n");

				// get the nell subjects and objects
				oieRawSubj = elems[0];
				oieRawProp = elems[1];
				oieRawObj = elems[2];

				if (isRelevantProperty(oieRawProp)) {

					if (!refinedMode) {
						// get the top-k concepts for the subject
						candidateSubjs = DBWrapper.fetchTopKLinksWikiPrepProb(
								Utilities.cleanse(oieRawSubj)
										.replaceAll("\\_+", " ").trim(),
								SAMEAS_TOPK);
						// get the top-k concepts for the object
						candidateObjs = DBWrapper.fetchTopKLinksWikiPrepProb(
								Utilities.cleanse(oieRawObj)
										.replaceAll("\\_+", " ").trim(),
								SAMEAS_TOPK);
					} else {

						candidates = DBWrapper.fetchRefinedMapping(
								Utilities.cleanse(oieRawSubj).trim(),
								(Constants.IS_NELL) ? oieRawProp.trim()
										.replaceAll("\\s+", "_") : oieRawProp
										.trim(), Utilities.cleanse(oieRawObj)
										.trim());

						try {
							candidateSubjs = new ArrayList<String>();
							if (candidates != null && candidates.size() > 0)
								candidateSubjs.add(candidates.get(0));

							candidateObjs = new ArrayList<String>();
							if (candidates != null && candidates.size() > 0)
								candidateObjs.add(candidates.get(1));

						} catch (IndexOutOfBoundsException e) {
							log.error(e.getMessage());
						}
					}

					// use the SPARQL endpoint for querying the direct and
					// inverse
					// relation betwen the sub-obj pairs
					findDirectIndirectProps(elems, candidateSubjs,
							candidateObjs, directPropWriter, inversePropWriter);

					// update GLOBAL_PROPERTY_MAPPINGS with the possible values
					// updateTheCollection(nellRawPred, directProperties);
				}
				if (lineCounter++ % 10000 == 0)
					log.info("Completed " + lineCounter + " lines ");

			} catch (Exception e) {
				log.error("Problem with line " + line.toString());
				e.printStackTrace();
				continue;
			}
		}

		// close streams
		directPropWriter.close();
		inversePropWriter.close();

	}

	/**
	 * often no need to look for every property in the data set..for reverb we
	 * are dealing with top-k properties..so makes it time efficient
	 * 
	 * @param oieRawProp
	 * @return
	 */
	private static boolean isRelevantProperty(String oieRawProp) {
		boolean flag = false;

		if (Constants.IS_NELL)
			flag = true;
		else {
			if (propertyNames.contains(oieRawProp)) {
				flag = true;
			} else {
				if (!Constants.WORKFLOW_NORMAL) {
					for (Entry<String, List<String>> e : propertyClusterNames
							.entrySet()) {
						for (String s : e.getValue()) {
							if (!propertyNames.contains(s)) {
								propertyNames.add(s);

								if (s.equals(oieRawProp))
									flag = true;
							}
						}
					}
				}
			}
		}

		return flag;
	}

	/**
	 * this method takes the possible set of candidates and tries to find the
	 * connecting property path beteween thema
	 * 
	 * @param line
	 * @param candidateSubj
	 * @param candidateObj
	 * @param directPropWriter
	 * @param inversePropWriter
	 * @return
	 * @throws IOException
	 */
	public static void findDirectIndirectProps(String[] line,
			List<String> candidateSubj, List<String> candidateObj,
			BufferedWriter directPropWriter, BufferedWriter inversePropWriter)
			throws IOException {

		boolean blankDirect = false;
		boolean blankInverse = false;
		String domainType = null;
		String rangeType = null;

		String domainTypeInv = null;
		String rangeTypeInv = null;

		List<String> directPropList = new ArrayList<String>();
		List<String> inversePropList = new ArrayList<String>();

		// for the current NELL predicate get the possible db:properties from
		// SPARQL endpoint

		for (String subjCand : candidateSubj) {
			if (!subjCand.equals("X")) {
				for (String objCand : candidateObj) {

					if (!objCand.equals("X")) {
						// DIRECT PROPERTIES
						String directProperties = getPredsFromEndpoint(
								subjCand.split("\t")[0], objCand.split("\t")[0]);

						if (directProperties.length() > 0) {
							directPropList.add(directProperties);

							// find domain type
							domainType = getTypeInfo(subjCand.split("\t")[0]);
							domainType = (domainType.length() == 0) ? "null"
									: domainType;

							// find range type
							rangeType = getTypeInfo(objCand.split("\t")[0]);
							rangeType = (rangeType.length() == 0) ? "null"
									: rangeType;

						}

						// INDIRECT PROPERTIES
						String inverseProps = getPredsFromEndpoint(
								objCand.split("\t")[0], subjCand.split("\t")[0]);

						if (inverseProps.length() > 0) {
							inversePropList.add(inverseProps);

							// find domain type
							if (rangeType != null) {
								domainTypeInv = rangeType;
							} else {
								domainTypeInv = getTypeInfo(objCand.split("\t")[0]);
								domainTypeInv = (domainTypeInv.length() == 0) ? "null"
										: domainTypeInv;
							}

							// find range type
							if (domainType != null) {
								rangeTypeInv = domainType;
							} else {
								rangeTypeInv = getTypeInfo(subjCand.split("\t")[0]);
								rangeTypeInv = (rangeTypeInv.length() == 0) ? "null"
										: rangeTypeInv;

							}
						}
					}
				}
			}
		}

		// write it out to log files
		if (directPropList.size() > 0) {
			blankDirect = true;
			directPropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\t");
			log.debug(line + "\t");
			for (String elem : directPropList) {
				directPropWriter.write(elem + "\t");
				log.debug(elem + "\t");
			}
			directPropWriter.write(domainType + "\t" + rangeType);

			directPropWriter.write("\n");
			log.debug("\n");
			directPropWriter.flush();

		}

		if (inversePropList.size() > 0) {
			blankInverse = true;

			inversePropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\t");
			log.debug(line + "\t");
			for (String elem : inversePropList) {
				inversePropWriter.write(elem + "\t");
				log.debug(elem + "\t");
			}
			inversePropWriter.write(domainTypeInv + "\t" + rangeTypeInv);
			inversePropWriter.write("\n");
			log.debug("\n");
			inversePropWriter.flush();
		}

		// if all possible candidate pairs have no predicates mapped, just
		// add one entry in each log file, not
		// multiple blank entries
		if (!blankDirect) {
			directPropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\n");
			directPropWriter.flush();
		}

		if (!blankInverse) {
			inversePropWriter.write(line[0] + "\t" + line[1] + "\t" + line[2]
					+ "\n");
			inversePropWriter.flush();
		}
	}

	@SuppressWarnings("finally")
	private static String getTypeInfo(String inst) {
		String mostSpecificVal = "";
		String mostGeneralVal = "";

		List<String> types = SPARQLEndPointQueryAPI.getInstanceTypes(Utilities
				.utf8ToCharacter(inst));

		try {
			mostSpecificVal = SPARQLEndPointQueryAPI.getLowestType(types)
					.get(0);
			// mostGeneralVal = SPARQLEndPointQueryAPI.getHighestType(types)
			// .get(0);
		} catch (IndexOutOfBoundsException e) {
		} finally {
			// return mostSpecificVal + "\t" + mostGeneralVal;
			return mostSpecificVal;
		}
	}

	/**
	 * updates a global collection of NELL property to possible mappings
	 * 
	 * @param nellRawPred
	 * @param probablePredicates
	 */
	private static void updateTheCollection(String nellRawPred,
			List<String> probablePredicates) {
		List<String> propValues = null;
		if (!GLOBAL_PROPERTY_MAPPINGS.containsKey(nellRawPred)) {
			propValues = new ArrayList<String>();

		} else {
			propValues = GLOBAL_PROPERTY_MAPPINGS.get(nellRawPred);
		}
		propValues.addAll(probablePredicates);
		GLOBAL_PROPERTY_MAPPINGS.put(nellRawPred, propValues);
	}

	/**
	 * get the possible predicates for a particular combination
	 * 
	 * @param candSubj
	 * @param candObj
	 * @return
	 */
	private static String getPredsFromEndpoint(String candSubj, String candObj) {
		StringBuffer sBuf = new StringBuffer();

		// remove all utf-8 characters and convert them to characters
		candSubj = Utilities.utf8ToCharacter(candSubj);
		candObj = Utilities.utf8ToCharacter(candObj);

		if (candSubj.endsWith("%"))
			candSubj = candSubj.replaceAll("%", "");

		if (candObj.endsWith("%"))
			candObj = candObj.replaceAll("%", "");

		if (multiHop) {

			// graph exploratory method is too slow

			// ArrayList<String> paths = new ArrayList<String>();
			//
			// log.info("new way.." + candSubj + "\t" + candObj);
			//
			// RelationExplorer relExp = new RelationExplorer(
			// Constants.DBPEDIA_INSTANCE_NS + candSubj,
			// Constants.DBPEDIA_INSTANCE_NS + candObj, (int) 2);
			// paths = relExp.init();
			//
			// if (paths.size() > 0) {
			// for (String path : paths) {
			// // add the sub classes to a set
			// sBuf.append(path + "\t");
			// }
			// log.info(paths.toString());
			// }

			// method using SPARQL query
			sBuf = multiHopQuery(candSubj, candObj, 2);

		} else {
			// possible predicate variable
			String possiblePred = null;

			// return list of all possible predicates
			List<String> returnPredicates = new ArrayList<String>();

			String sparqlQuery = "select * where {<"
					+ Constants.DBPEDIA_INSTANCE_NS
					+ candSubj
					+ "> ?val <"
					+ Constants.DBPEDIA_INSTANCE_NS
					+ candObj
					+ ">. "
					+ "?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>."
					+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageWikiLink'))}";

			log.debug(sparqlQuery);

			// fetch the result set
			List<QuerySolution> listResults = SPARQLEndPointQueryAPI
					.queryDBPediaEndPoint(sparqlQuery);

			for (QuerySolution querySol : listResults) {
				possiblePred = querySol.get("val").toString();

				// add the sub classes to a set
				returnPredicates.add(possiblePred);
				sBuf.append(possiblePred + "\t");
			}
		}
		return sBuf.toString().trim();

	}

	/*
	 * search the SPARQL endpoint with maximum k hops
	 */
	private static StringBuffer multiHopQuery(String candSubj, String candObj,
			int k) {

		StringBuffer sBuf = new StringBuffer();

		// possible predicate variable
		String possiblePred = null;

		// intermediate object variable
		String possiblePred2 = null;

		// two hop case
		if (k == 2) {

			// spawn two queries, one from the subject and second from the
			// object
			// the intersecting entity carves the property paths
			String sparqlQuery = "select ?val ?val2 where {<"
					+ Constants.DBPEDIA_INSTANCE_NS
					+ candSubj
					+ "> ?val ?obj. ?obj ?val2 <"
					+ Constants.DBPEDIA_INSTANCE_NS
					+ candObj
					+ ">.?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>. "
					+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageWikiLink')). "
					+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageExternalLink')). "
					+ "?val2 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>. "
					+ "FILTER(!regex(str(?val2), 'http://dbpedia.org/ontology/wikiPageWikiLink')). "
					+ "FILTER(!regex(str(?val), 'http://dbpedia.org/ontology/wikiPageRedirects')). "
					+ "FILTER(!regex(str(?val2), 'http://dbpedia.org/ontology/wikiPageRedirects')). "
					+ "FILTER(!regex(str(?val2), 'http://dbpedia.org/ontology/wikiPageExternalLink'))}";

			// fetch the result set
			List<QuerySolution> listResults = SPARQLEndPointQueryAPI
					.queryDBPediaEndPoint(sparqlQuery);

			for (QuerySolution querySol : listResults) {
				possiblePred = querySol.get("val").toString();
				possiblePred2 = querySol.get("val2").toString();

				log.info(candSubj + " ===> " + possiblePred + " ====> "
						+ possiblePred2 + " ===> " + candObj);

				sBuf.append(possiblePred + "\t" + possiblePred2 + "\t");

			}

		}
		return sBuf;
	}

	/**
	 * read the files generated from the property learner
	 */
	public static void readFiles() {
		File folder = null;
		File[] paths;

		try {
			// create new file
			folder = new File(PROPERTY_LOGS_PATH);

			FileFilter filter = new FileFilter() {
				@Override
				public boolean accept(File pathname) {
					return pathname.isFile() && pathname.length() > 0;
				}
			};

			// returns pathnames for files and directory
			paths = folder.listFiles(filter);

			// for each pathname in pathname array
			for (File path : paths) {

				// individually parse each file and get the properties with hop
				// lengths greater than one
				// these signify the ones which are not directly map-able to
				// DBpedia

				processFile(path.toString());

			}
		} catch (Exception e) {
			// if any error occurs
			e.printStackTrace();
		}
	}

	/**
	 * take the individual property files and parse them for likely paths
	 * 
	 * @param path
	 * @throws IOException
	 */
	private static void processFile(String path) throws IOException {

		InputStream is = GenerateNewProperties.class.getResourceAsStream(path
				.toString().replaceAll("./src/main/resources", ""));

		Scanner scan = new Scanner(is, "UTF-8");

		while (scan.hasNextLine()) {

			String line = scan.nextLine();
			String[] arr = line.split("\t");
			if (arr.length > 2)
				log.info(String.valueOf(line));

		}
	}
}
