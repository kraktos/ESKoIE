/**
 * 
 */
package code.dws.reverb;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import code.dws.relationMap.workflow2.GenerateTriples;
import code.dws.reverb.clustering.KMediodCluster;
import code.dws.reverb.clustering.MarkovClustering;
import code.dws.reverb.clustering.analysis.CompareClusters;
import code.dws.utils.Constants;

/**
 * In analysis, we figure out the optimal cluster size..
 * 
 * @author adutta
 * 
 */
public class ReverbPropertyReNaming {

	private static final String CLUSTER_NAMES = "src/main/resources/input/cluster.names.out";
	/**
	 * the mega collection for reverb, holding the mapping from new propery name
	 * to the list of actual properties it represents
	 */
	private static Map<String, List<String>> CLUSTERED_REVERB_PROPERTIES = new HashMap<String, List<String>>();

	/**
	 * 
	 */
	public ReverbPropertyReNaming() {
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		double optimalInflation = 0;
		String simScoreFile = null;

		if (Constants.WORKFLOW_NORMAL) {
			simScoreFile = KMediodCluster.ALL_SCORES;
			optimalInflation = getOptimalInflation();

		} else {
			simScoreFile = GenerateTriples.PAIRWISE_SCORES_FILE;
			optimalInflation = GenerateTriples.readClusters("");

			// simScoreFile =
			// "/home/adutta/git/OIE-Integration/src/main/resources/input/All5.csv";
			// optimalInflation = new ClusterClient(simScoreFile)
			// .getClusterIndex();
		}

		// use the inflation factor to regenerate the clusters
		// perform a Markov Cluster
		MarkovClustering.main(new String[] { simScoreFile,
				String.valueOf(optimalInflation) });
		//
		// // get the clusters in memory
		CLUSTERED_REVERB_PROPERTIES = MarkovClustering.getAllClusters();

		BufferedWriter writer = new BufferedWriter(
				new FileWriter(CLUSTER_NAMES));

		for (Entry<String, List<String>> e : CLUSTERED_REVERB_PROPERTIES
				.entrySet()) {
			writer.write(e.getKey() + "\t[");
			for (String val : e.getValue()) {
				writer.write(val + "\t");
			}
			writer.write("]\n");
		}

		writer.flush();
		writer.close();
	}

	/**
	 * @return the mCl
	 */
	public static Map<String, List<String>> getReNamedProperties() {
		return CLUSTERED_REVERB_PROPERTIES;
	}

	/**
	 * read the file for different inflation parameter and use it to find the
	 * inflation giving max number of clusters
	 * 
	 * @return
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("resource")
	private static double getOptimalInflation() throws FileNotFoundException {
		Scanner scan;
		String[] elem = null;
		String sCurrentLine = null;
		double inflation = 30;
		double tInfl = 0;
		int clusterSize = 0;

		double minMclIndex = Integer.MAX_VALUE;
		double tIndex = 0;

		scan = new Scanner(new File((CompareClusters.CLUSTER_INDICES)), "UTF-8");
		scan.nextLine();
		while (scan.hasNextLine()) {
			sCurrentLine = scan.nextLine();
			elem = sCurrentLine.split("\t");
			tIndex = Double.parseDouble(elem[3]);
			if (tIndex <= minMclIndex) {
				minMclIndex = tIndex;
				clusterSize = Integer.parseInt(elem[1]);
			}
		}

		scan = new Scanner(new File((CompareClusters.CLUSTER_INDICES)), "UTF-8");
		scan.nextLine();
		while (scan.hasNextLine()) {
			sCurrentLine = scan.nextLine();
			elem = sCurrentLine.split("\t");

			if (elem[1].equals(String.valueOf(clusterSize))) {
				tInfl = Double.parseDouble(elem[0]);
				inflation = (tInfl < inflation) ? tInfl : inflation;
			}
		}

		return inflation;
	}

}
