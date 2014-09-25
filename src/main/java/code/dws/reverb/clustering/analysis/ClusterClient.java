/**
 * 
 */
package code.dws.reverb.clustering.analysis;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import code.dws.reverb.clustering.MarkovClustering;

/**
 * @author adutta
 * 
 */
public class ClusterClient {
	String file = null;

	// define Logger
	public static Logger logger = Logger.getLogger(ClusterClient.class
			.getName());

	/**
	 * 
	 */
	public ClusterClient(String file) {
		this.file = file;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		System.out.println(new ClusterClient(args[0]).getClusterIndex());
	}

	/**
	 * find the best cluster setting for a given pairwise score file
	 * 
	 * @param file
	 * @return
	 */
	public double getClusterIndex() {

		double minMclIndex = Integer.MAX_VALUE;
		double bestInflation = 0;
		double tempIndex = 0;

		Map<String, List<String>> markovCluster = null;
		Map<String, List<String>> bestCluster = null;
		Map<String, Double> isoMcl = new HashMap<String, Double>();

		DecimalFormat df = new DecimalFormat("##.##");

		try {
			// iterate over varying inflation to find the sweet spot, giving the
			// best mcl index
			for (double p = 2; p < 27; p++) {

				// perform a Markov Cluster
				MarkovClustering.main(new String[] { this.file,
						String.valueOf(df.format(p)) });

				// get the clusters in memory
				markovCluster = MarkovClustering.getAllClusters();

				// find its score.
				tempIndex = CompareClusters.computeClusterIndex(markovCluster,
						isoMcl);

				// check if this is the best
				if (tempIndex < minMclIndex) {
					bestCluster = null;
					minMclIndex = tempIndex;
					bestCluster = markovCluster;
					bestInflation = p;
				}

				// create a map of cluster key and its highest isolation value
				logger.debug(markovCluster.size() + " clusters with "
						+ " Score = " + tempIndex + " for i = " + p);

				// p = p + 0.2;
			}

			logger.debug("Best score is at " + minMclIndex);
			// for (Entry<String, List<String>> e : bestCluster.entrySet()) {
			// System.out.print(e.getKey() + "\t[");
			// for (String s : e.getValue()) {
			// System.out.print(s + "\t");
			// }
			// System.out.print("]\n");
			// }
		} catch (IOException e) {
			logger.error(e.getMessage());
		}

		return bestInflation;
	}
}
