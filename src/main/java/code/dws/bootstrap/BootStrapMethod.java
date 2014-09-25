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
import java.util.HashSet;
import java.util.List;
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
		createSameAsHypotheses("src/main/resources/output/ds_"
				+ Constants.PREDICATE + "/out.db");

		createSubObjLists("src/main/resources/output/ds_" + Constants.PREDICATE
				+ "/out.db");

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

	public static void createSameAsHypotheses(String outputEvidenceFilePath)
			throws IOException {

		String line;

		Pattern pattern = Pattern.compile("sameAs\\((.*?)\\)");
		Matcher m = null;

		// read the file contents
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				new FileInputStream(outputEvidenceFilePath)));

		try {
			while ((line = reader.readLine()) != null) {
				m = pattern.matcher(line);
				while (m.find()) {
					// 0th is DBPedia instance
					// 1st is Nell instance
					SAME_AS_LIST.add(new ImmutablePair<String, String>(m.group(
							1).split(",")[0], m.group(1).split(",")[1]));
				}
			}
		} finally {
			if (reader != null)
				reader.close();
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

		try {
			while ((line = reader.readLine()) != null) {
				m = pattern.matcher(line);
				while (m.find()) {
					NELL_SUBJECTS.add(m.group(1).split(",")[1]);
					NELL_OBJECTS.add(m.group(1).split(",")[2]);
				}
			}
		} finally {
			if (reader != null)
				reader.close();
		}
	}

	public static boolean isSubject(String nell) {
		if (NELL_SUBJECTS.contains(nell))
			return true;
		else
			return false;
	}

	// bw.close();
}
