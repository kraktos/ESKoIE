/**
 * 
 */
package code.dws.markovLogic;

import gnu.trove.map.hash.THashMap;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.utils.Constants;
import code.dws.utils.FileUtil;

/**
 * this class creates yago to dbpedia mapping of classes. Important since this
 * can complement scenarios where DBPedia type is missing
 * 
 * 
 * @author adutta
 * 
 */
public class YagoDbpediaMapping {

	// define class logger
	public final static Logger log = LoggerFactory
			.getLogger(YagoDbpediaMapping.class);

	private static final String DBP_YAGO_CLASS_EQUIV = "src/main/resources/input/dbpedia_yago_matching.csv";

	private static final String WORDNET_YAGO_CLASS_EQUIV = "src/main/resources/input/yagoDBpediaClasses.tsv";

	private static final String WORDNET_SUBCLASS = "src/main/resources/input/yagoSimpleTaxonomy.tsv";

	private static final String YAGO_SUBCLASS_OUTPUT = "src/main/resources/input/yago.out";

	private static final String EVIDENCE = "DBP.YAGO.EVIDENCES.db";

	// stores the YAGO class hierarchy reasoned with MLN
	private static final THashMap<String, String> YAGO_SUBCLASS_MAP = new THashMap<String, String>();

	// stores the YAGO to DBP mappings already known
	private static final THashMap<String, String> YAGO_DBP_MAP = new THashMap<String, String>();

	private static ArrayList<ArrayList<String>> dbpYagoClassEquiv;

	private static ArrayList<ArrayList<String>> wordnetYagoClassEquiv;

	private static ArrayList<ArrayList<String>> wordnetSubClasses;

	/**
	 * 
	 */
	public YagoDbpediaMapping() {

	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {

			// laod the files in memory
			init();

			// read those and create evidence files to be processed by rockit
			createEvidences();

			// parse the rockitoutput file for getting yago subclasses relations
			parseOutputFile();

			// final exposed api for getting the dbp mappings
			// System.out
			// .println(getDBPClass("http://dbpedia.org/class/yago/Municipality108626283"));

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * load the relevant files in memory
	 */
	private static void init() {

		// read the file into memory
		try {
			dbpYagoClassEquiv = FileUtil.genericFileReader(new FileInputStream(
					DBP_YAGO_CLASS_EQUIV), ";", true);
		} catch (FileNotFoundException e) {
			log.error("Problem while loading " + DBP_YAGO_CLASS_EQUIV);
		}

		try {
			wordnetYagoClassEquiv = FileUtil.genericFileReader(
					new FileInputStream(WORDNET_YAGO_CLASS_EQUIV), "\t", false);
		} catch (FileNotFoundException e) {
			log.error("Problem while loading " + WORDNET_YAGO_CLASS_EQUIV);

		}

		try {
			wordnetSubClasses = FileUtil.genericFileReader(new FileInputStream(
					WORDNET_SUBCLASS), "\t", false);
		} catch (FileNotFoundException e) {
			log.error("Problem while loading " + WORDNET_SUBCLASS);

		}

	}

	private static void createEvidences() throws IOException {

		// DEFINE AN EVIDENCE WRITER
		BufferedWriter eviWriter = new BufferedWriter(new FileWriter(EVIDENCE));

		writeOutEvidences(eviWriter);

		log.info("DBPedia - YAGO Class equivalence = "
				+ dbpYagoClassEquiv.size());
		log.info("Wordnet - YAGO Class equivalence = "
				+ wordnetYagoClassEquiv.size());
		log.info("Wordnet Subclass assertions = " + wordnetSubClasses.size());

	}

	/**
	 * takes the loaded in mmeory class evidences and writes out to a
	 * rockit-friendly format
	 * 
	 * @param eviWriter
	 * @param wordnetYagoClassEquiv
	 * @param wordnetSubClasses
	 * @param dbpYagoClassEquiv
	 * @throws IOException
	 */
	private static void writeOutEvidences(BufferedWriter eviWriter)
			throws IOException {

		String val = null;

		// write out the equivalences first
		for (ArrayList<String> list : wordnetYagoClassEquiv) {

			if (list.toString().indexOf("wikicategory_") == -1) {

				val = "";
				eviWriter.write("equivWY(\"");
				for (String elem : list) {
					elem = elem.replaceAll("<", "").replaceAll(">", "");
					if (!elem.equals("owl:equivalentClass"))
						val = val + ",\"" + elem + "\"";
				}
				eviWriter.write(val.replaceFirst(",\"", "").replaceAll(
						Constants.YAGO_HEADER + "/", "YAGO:")
						+ ")\n");
			}
		}

		// flush evidences
		eviWriter.flush();

		// write out the subclasses
		for (ArrayList<String> list : wordnetSubClasses) {
			if (list.get(1).indexOf("rdfs:subClassOf") != -1) {
				val = "";
				eviWriter.write("subClass(\"");
				for (String elem : list) {
					elem = elem.replaceAll("<", "").replaceAll(">", "");
					if (!elem.equals("rdfs:subClassOf"))
						val = val + ",\"" + elem + "\"";
				}
				eviWriter.write(val.replaceFirst(",\"", "") + ")\n");
			}
		}
		// flush evidences
		eviWriter.flush();

		// flush evidences
		eviWriter.flush();

		// write out the subclasses
		for (ArrayList<String> list : dbpYagoClassEquiv) {
			val = "";
			eviWriter.write("equivDY(\"");
			for (String elem : list) {
				elem = elem.replaceAll(Constants.ONTOLOGY_NAMESPACE, "DBP:")
						.replaceAll(Constants.YAGO_HEADER + "/", "YAGO:");

				val = val + "," + elem + "";

			}

			// also create an in memory collection for these mappings
			YAGO_DBP_MAP.put(
					list.get(1)
							.replaceAll(Constants.YAGO_HEADER + "/", "YAGO:")
							.replaceAll("\"", ""),
					list.get(0)
							.replaceAll(Constants.ONTOLOGY_NAMESPACE, "DBP:")
							.replaceAll("\"", ""));

			eviWriter.write(val.replaceFirst(",\"", "") + ")\n");

		}
		// flush evidences
		eviWriter.flush();

		eviWriter.close();

	}

	/**
	 * reads the rockit output file to create an in memory collection
	 * 
	 * @throws FileNotFoundException
	 */
	public static void parseOutputFile() throws FileNotFoundException {
		ArrayList<ArrayList<String>> yagoSubClasses = FileUtil
				.genericFileReader(new FileInputStream(YAGO_SUBCLASS_OUTPUT),
						";", false);

		String val = null;

		for (ArrayList<String> list : yagoSubClasses) {
			val = list.get(0).replaceAll("subClassOf\\(", "")
					.replaceAll("\\)", "");

			YAGO_SUBCLASS_MAP.put(
					val.split(",")[0].replaceAll("\"", "").trim(),
					val.split(",")[1].replaceAll("\"", "").trim());
		}
	}

	/**
	 * returns the DBP class for a given yago class logic is to look for a
	 * direct mapping of YAGO class to a DBPedia from the collection, if it does
	 * not have, we go for its higher class and try for its mapping
	 * 
	 * @param yagoClass
	 * @return
	 */
	public static String getDBPClass(String yagoClass) {
		String dbpClass = null;
		String yagoSuperClass = null;
		yagoClass = yagoClass.replaceAll(Constants.YAGO_HEADER + "/", "YAGO:");

		try {
			// fetch the mapping if directly available
			if (YAGO_DBP_MAP.containsKey(yagoClass)) {
				dbpClass = YAGO_DBP_MAP.get(yagoClass);
			} else {

				// get the superClass of this yago class
				if (YAGO_SUBCLASS_MAP.containsKey(yagoClass)) {
					yagoSuperClass = YAGO_SUBCLASS_MAP.get(yagoClass);

					if (yagoSuperClass != null) {
						dbpClass = YAGO_DBP_MAP.get(yagoSuperClass);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return dbpClass;

	}
}
