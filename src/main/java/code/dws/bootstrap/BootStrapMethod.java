/**
 * 
 */

package code.dws.bootstrap;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import code.dws.core.AutomatedNodeScoringWrapper;
import code.dws.utils.Constants;

/**
 * This is a class to perform some bootstrapping on the generated output by the
 * reasoner. The idea is to reuse this output and re-reason with them to come up
 * with a more polished output.
 * 
 * @author Arnab Dutta
 */
public class BootStrapMethod {

	private static Map<String, Double> SAME_AS_CONF_MAP = new HashMap<String, Double>();

	public static List<Pair<String, String>> SAME_AS_LIST = new ArrayList<Pair<String, String>>();

	static List<String> NELL_SUBJECTS = new ArrayList<String>();
	static List<String> NELL_OBJECTS = new ArrayList<String>();

	static Set<String> SET_FULL_SUB_TYPES = new HashSet<String>();
	static Set<String> SET_FULL_OBJ_TYPES = new HashSet<String>();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Constants.loadConfigParameters(args);
		Constants.BOOTSTRAP = true;
		Constants.RELOAD_TYPE = false;

		// take the output file generated
		createSameAsHypotheses("/home/adutta/git/OIE-Integration/src/main/resources/output/ds_"
				+ Constants.PREDICATE + "/out.db");

		createSubObjLists("/home/adutta/git/OIE-Integration/src/main/resources/output/ds_"
				+ Constants.PREDICATE + "/out.db");

		BufferedWriter domRangPrefWriter = new BufferedWriter(new FileWriter(
				Constants.DOMAIN_RANGE_BS_PREFERENCE_FILE));

		BufferedWriter domRangEvidenceWriter = new BufferedWriter(
				new FileWriter(Constants.DOMAIN_RANGE_BS_EVIDENCE_FILE));

		System.out.println("BUILDING NEW ALPHA TREE FROM THE OUTPUT FILE ..");
		AutomatedNodeScoringWrapper nodeScorer = new AutomatedNodeScoringWrapper();
		nodeScorer.buildDomRan(Constants.PREDICATE);

		nodeScorer.generateClassHierarchy(Constants.DOMAIN);
		domRangPrefWriter
				.write("\n======  DOMAIN PREFERENCE ================================\n");

		nodeScorer.rankTypes(Constants.SUB_SET_TYPES, SET_FULL_SUB_TYPES,
				domRangPrefWriter, domRangEvidenceWriter, Constants.DOMAIN);

		nodeScorer.generateClassHierarchy(Constants.RANGE);
		domRangPrefWriter
				.write("======  RANGE PREFERENCE ================================\n");
		nodeScorer.rankTypes(Constants.OBJ_SET_TYPES, SET_FULL_OBJ_TYPES,
				domRangPrefWriter, domRangEvidenceWriter, Constants.RANGE);

		domRangPrefWriter.close();
		domRangEvidenceWriter.close();
	}

	private static void loadOldAlphaNodes(String domainRangePreferenceFile)
			throws IOException {
		// read the file contents
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(domainRangePreferenceFile)));

		String line;
		boolean flag = false;

		while ((line = reader.readLine()) != null) {
			if (line.indexOf("DOMAIN PREFERENCE") != -1) {
				flag = false;
			}
			if (line.indexOf("RANGE PREFERENCE") != -1) {
				flag = true;
			}

			if (line.indexOf(Constants.ONTOLOGY_NAMESPACE) != -1 && !flag) {
				SET_FULL_SUB_TYPES.add(line.split("\t")[0].replaceAll(
						Constants.ONTOLOGY_NAMESPACE, ""));
			}

			if (line.indexOf(Constants.ONTOLOGY_NAMESPACE) != -1 && flag) {
				SET_FULL_OBJ_TYPES.add(line.split("\t")[0].replaceAll(
						Constants.ONTOLOGY_NAMESPACE, ""));
			}
		}

	}

	/**
	 * @param args
	 * @param args
	 * @throws IOException
	 */
	// public static void doBootStrapping(String[] args) throws IOException {
	//
	// ExperimentAutomation.loadConfigParameters(args);
	//
	// //
	// createSameAsConf("/home/arnab/Workspaces/SchemaMapping/linking-IE/resource/output/ds_"
	// // +
	// // ExperimentAutomation.PREDICATE +
	// // "/evidenceT3.db");
	//
	// // take the output file generated
	// createSameAsHypotheses("/home/arnab/Workspaces/SchemaMapping/linking-IE/resource/output/ds_"
	// + ExperimentAutomation.PREDICATE +
	// "/outT3.db");
	// //
	// createSubObjLists("/home/arnab/Workspaces/SchemaMapping/linking-IE/resource/output/ds_"
	// +
	// ExperimentAutomation.PREDICATE +
	// "/outT3.db");
	//
	// bootStrap(OIE.NELL);
	//
	// BufferedWriter domRangPrefWriter = new BufferedWriter(
	// new FileWriter(Constants.DOMAIN_RANGE_BS_PREFERENCE_FILE));
	//
	// BufferedWriter domRangEvidenceWriter = new BufferedWriter(
	// new FileWriter(Constants.DOMAIN_RANGE_BS_EVIDENCE_FILE));
	//
	// AutomatedNodeScoringWrapper nodeScorer = new
	// AutomatedNodeScoringWrapper();
	// nodeScorer.buildDomRan(ExperimentAutomation.PREDICATE);
	//
	// nodeScorer.generateClassHierarchy(Constants.DOMAIN);
	// domRangPrefWriter.write("\n======  DOMAIN PREFERENCE ================================\n");
	// //
	// nodeScorer.rankTypes(GenericConverter.SUB_SET_TYPES,
	// domRangPrefWriter,
	// domRangEvidenceWriter, Constants.DOMAIN);
	// //
	// nodeScorer.generateClassHierarchy(Constants.RANGE);
	// domRangPrefWriter.write("======  RANGE PREFERENCE ================================\n");
	// nodeScorer.rankTypes(GenericConverter.OBJ_SET_TYPES,
	// domRangPrefWriter,
	// domRangEvidenceWriter, Constants.RANGE);
	//
	// domRangPrefWriter.close();
	// domRangEvidenceWriter.close();
	// }

	private static void createSameAsConf(String outputEvidenceFilePath)
			throws IOException {

		String line;

		Pattern pattern = Pattern.compile("sameAsConf\\((.*?)\\)");
		Matcher m = null;

		// read the file contents
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputEvidenceFilePath)));

		while ((line = reader.readLine()) != null) {
			m = pattern.matcher(line);
			while (m.find()) {
				SAME_AS_CONF_MAP.put(m.group(1).split(",")[0]
						+ m.group(1).split(",")[1],
						Double.parseDouble(m.group(1).split(",")[2]));
			}
		}
	}

	public static void createSameAsHypotheses(String outputEvidenceFilePath)
			throws IOException {

		String line;

		Pattern pattern = Pattern.compile("sameAs\\((.*?)\\)");
		Matcher m = null;

		// read the file contents
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputEvidenceFilePath)));

		while ((line = reader.readLine()) != null) {
			m = pattern.matcher(line);
			while (m.find()) {
				// 0th is DBPedia instance
				// 1st is Nell instance
				SAME_AS_LIST.add(new ImmutablePair<String, String>(m.group(1)
						.split(",")[0], m.group(1).split(",")[1]));
			}
		}
	}

	public static void createSubObjLists(String outputEvidenceFilePath)
			throws IOException {

		// read the file contents
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputEvidenceFilePath)));

		Pattern pattern = Pattern.compile("propAsst\\((.*?)\\)");
		Matcher m = null;

		String line;

		while ((line = reader.readLine()) != null) {
			m = pattern.matcher(line);
			while (m.find()) {
				NELL_SUBJECTS.add(m.group(1).split(",")[1]);
				NELL_OBJECTS.add(m.group(1).split(",")[2]);
			}
		}
	}

	// /**
	// * read the reasoner produced output file for the set of sameAs statements
	// *
	// * @param outputEvidenceFilePath
	// * @param oieType
	// * @throws IOException
	// */
	// private static void bootStrap(OIE oieType)
	// throws IOException {
	//
	// List<String> candidates = null;
	// List<String> entityTypes = null;
	//
	// boolean isSubject = false;
	//
	// // initiate DB
	// DBWrapper.init(Constants.GET_WIKI_TITLES_SQL);
	//
	// for (String nellInst : SAME_AS_LIST) {
	//
	// isSubject = isSubject(nellInst);
	// nellInst = nellInst.replaceAll("NELL#Instance/", "").replaceAll("\"",
	// "");
	//
	// Constants.TOP_ANCHORS = 1;
	// // just look for top-1
	// // use the concepts to get the types, most specific type
	// candidates = DBWrapper
	// .fetchWikiTitles((oieType == Constants.OIE.NELL) ? Utilities
	// .cleanse(nellInst).replaceAll("\\_+", " ") :
	// Utilities.cleanse(
	// nellInst).replaceAll(
	// "\\_+",
	// " "));
	//
	// // call the DistantSupervised method for types and UPDATE the
	// // domainClassMap and rangeClassMap
	// if (isSubject)
	// DistantSupervised.getTypes(candidates, Constants.DOMAIN);
	// if (!isSubject)
	// DistantSupervised.getTypes(candidates, Constants.RANGE);
	//
	// // iterate through the concept, should be just 1
	// for (String dbpEntity : candidates) {
	//
	// dbpEntity = dbpEntity.replaceAll("\\s+", "_");
	//
	// entityTypes = SPARQLEndPointQueryAPI.getInstanceTypes(dbpEntity);
	// entityTypes = SPARQLEndPointQueryAPI.getLowestType(entityTypes);
	//
	// if (entityTypes.size() != 0) {
	// for (String type : entityTypes) {
	// if (isSubject)
	// GenericConverter.SUB_SET_TYPES.add(type);
	// if (!isSubject)
	// GenericConverter.OBJ_SET_TYPES.add(type);
	// }
	// }
	// }
	// }
	//
	// System.out.println(GenericConverter.SUB_SET_TYPES);
	// System.out.println(GenericConverter.OBJ_SET_TYPES);
	//
	// }

	public static boolean isSubject(String nell) {
		if (NELL_SUBJECTS.contains(nell))
			return true;
		else
			return false;
	}

	// bw.close();
}
