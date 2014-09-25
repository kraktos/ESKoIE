/**
 * 
 */

package code.dws.core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import code.dws.dao.GenericTreeNode;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * @author Arnab Dutta
 */
public class AutomatedNodeScoringWrapper {

	DBPediaTreeOperation dbTOp = null;

	/**
	 * @param property
	 * @throws IOException
	 */
	public void buildDomRan(String property) throws IOException {

		this.dbTOp = new DBPediaTreeOperation(Constants.DBPEDIA_TBOX);

		// run the learner to score the nodes
		// the class tree hierarchy is to be build just for top-1 candidate
		// matches
		this.dbTOp.buildDomRan(new String[] { property, "1" });
	}

	public void generateClassHierarchy(String identifier) throws IOException {
		this.dbTOp.generateClassHierarchy(identifier);
	}

	public GenericTreeNode getUNTYPEDNodeDetails() {
		return this.dbTOp.getUNTYPEDNODE();
	}

	/**
	 * @param set
	 * @param fullSetOfTpes
	 * @param domRangPrefWriter
	 * @param domRangEvidenceWriter
	 * @param range
	 * @throws IOException
	 */
	public void rankTypes(Set<String> set, Set<String> fullSetOfTpes,
			BufferedWriter domRangPrefWriter,
			BufferedWriter domRangEvidenceWriter, String identifier)
			throws IOException {

		DecimalFormat df = new DecimalFormat("0.00000000000");

		List<String> dbPediaTypes = new ArrayList<String>(set);

		// System.out.println("  ***************************************** *");
		// System.out.println(set);

		this.dbTOp.getUNTYPEDNODE();

		Map<String, Pair<Double, Double>> testMap = this.dbTOp.getNodeScore(
				dbPediaTypes, fullSetOfTpes);

		for (Map.Entry<String, Pair<Double, Double>> entry : testMap.entrySet()) {

			domRangPrefWriter.write(entry.getKey() + "\t("
					+ entry.getValue().getLeft() + ",\t"
					+ entry.getValue().getRight() + ")\n");

			// System.out.println(entry.getKey().replaceAll(Constants.ONTOLOGY_NAMESPACE,
			// "DBP#ontology/"));

			// domRangEvidenceWriter.write("isOfType" + identifier +
			// "Conf(\""
			// + entry.getKey().replaceAll(Constants.ONTOLOGY_NAMESPACE,
			// "DBP#ontology/") + "\"," + entry.getValue().getSecond() + ")\n");

			if (identifier.equals(Constants.DOMAIN))
				domRangEvidenceWriter
						.write(df.format(Utilities
								.convertProbabilityToWeight(entry.getValue()
										.getRight()))
								+ " !isOfType(\""
								+ entry.getKey().replaceAll(
										Constants.ONTOLOGY_NAMESPACE,
										"DBP#ontology/")
								+ "\",ds) v !propAsstConf(P, ns, no, ccc) v "
								+ "!sameAsConf(ds, ns, f) v "
								+ "  sameAs(ds,ns) \n");

			if (identifier.equals(Constants.RANGE))
				domRangEvidenceWriter
						.write(df.format(Utilities
								.convertProbabilityToWeight(entry.getValue()
										.getRight()))
								+ " !isOfType(\""
								+ entry.getKey().replaceAll(
										Constants.ONTOLOGY_NAMESPACE,
										"DBP#ontology/")
								+ "\",do) v !propAsstConf(P, ns, no, ccc) v "
								+ "!sameAsConf(do, no, ff) v"
								+ " sameAs(do,no)  \n");

			domRangEvidenceWriter.flush();
			domRangPrefWriter.flush();
		}
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		BufferedWriter domRangPrefWriter = new BufferedWriter(new FileWriter(
				Constants.DOMAIN_RANGE_PREFERENCE_FILE));
		BufferedWriter domRangEvidenceWriter = new BufferedWriter(
				new FileWriter(Constants.DOMAIN_RANGE_EVIDENCE_FILE));

		AutomatedNodeScoringWrapper nodeScorer = new AutomatedNodeScoringWrapper();

		nodeScorer.buildDomRan(args[0]);
		nodeScorer.generateClassHierarchy(Constants.DOMAIN);

		domRangPrefWriter
				.write("\n======  DOMAIN PREFERENCE ================================\n");
		nodeScorer.rankTypes(Constants.SUB_SET_TYPES, null, domRangPrefWriter,
				domRangEvidenceWriter, Constants.DOMAIN);
		nodeScorer.generateClassHierarchy(Constants.RANGE);

		domRangPrefWriter
				.write("======  RANGE PREFERENCE ================================\n");
		nodeScorer.rankTypes(Constants.OBJ_SET_TYPES, null, domRangPrefWriter,
				domRangEvidenceWriter, Constants.RANGE);

		domRangPrefWriter.close();
		domRangEvidenceWriter.close();

		// if(Constants.BOOTSTRAP)
		// BootStrapMethod.doBootStrapping(nodeScorer);
	}

}
