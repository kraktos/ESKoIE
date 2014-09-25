/**
 * 
 */
package code.dws.reverb.clustering.analysis;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import code.dws.reverb.clustering.KMediodCluster;
import code.dws.reverb.clustering.MarkovClustering;

/**
 * @author adutta
 * 
 */
public class CompareClusters {

	public static final String CLUSTER_INDICES = "src/main/resources/input/KCL_MCL_CL";

	public static final double SIM_SCORE_THRESHOLD = 0.01;

	/**
	 * 
	 */
	public CompareClusters() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		KMediodCluster.loadScores();
		KMediodCluster.ioRoutine();

		Map<String, List<String>> mCl = null;
		Map<String, List<String>> kmCl = null;

		Map<String, Double> isoKcl = new HashMap<String, Double>();
		Map<String, Double> isoMcl = new HashMap<String, Double>();

		BufferedWriter writer = new BufferedWriter(new FileWriter(
				CLUSTER_INDICES));

		double kclIndex = Integer.MAX_VALUE;
		double mclIndex = 0;

		DecimalFormat df = new DecimalFormat("##.##");

		writer.write("ITERATION\tCLUSTER_SIZE\tKCL_SCORE\tMCL_SCORE\n");
		for (double p = 2; p < 21;) {

			double tempIndex = 0;

			// perform a Markov Cluster
			MarkovClustering.main(new String[] { KMediodCluster.ALL_SCORES,
					String.valueOf(df.format(p)) });

			// get the clusters in memory
			mCl = MarkovClustering.getAllClusters();

			mclIndex = computeClusterIndex(mCl, isoMcl);

			// create a map of cluster key and its highest isolation value
			System.out.println("MCL Index Score = " + mclIndex);

			for (int i = 0; i < 5; i++) {

				// perform k-mediod cluster
				KMediodCluster.doKClustering(mCl.size());

				// get the cluster in memory
				kmCl = KMediodCluster.getAllClusters();

				// create a map of cluster key and its highest isolation value
				tempIndex = computeClusterIndex(kmCl, isoKcl);

				kclIndex = (tempIndex < kclIndex) ? tempIndex : kclIndex;
			}

			System.out.println("KCL Index Score = " + kclIndex);

			writer.write(df.format(p) + "\t" + mCl.size() + "\t" + kclIndex
					+ "\t" + mclIndex + "\n");

			writer.flush();
			p = p + 0.2;
		}
		writer.close();
	}

	/**
	 * get a isolation score for the whole cluster set
	 * 
	 * @param mCl
	 * @param isoMcl
	 * @return
	 * @return
	 */
	public static double computeClusterIndex(Map<String, List<String>> mCl,
			Map<String, Double> isoMcl) {

		double minCompactness = 0;
		double tempIsolation = 0;
		double maxIsolation = 0;

		List<String> arg1 = null;
		List<String> arg2 = null;

		double clusterGoodness = 0;
		// long n = (mCl.size()) * (mCl.size() - 1) / 2;
		for (Entry<String, List<String>> e1 : mCl.entrySet()) {
			
			maxIsolation = 0;

			for (Entry<String, List<String>> e2 : mCl.entrySet()) {
				if (e2.getKey().hashCode() != e1.getKey().hashCode()) {

					arg1 = e1.getValue();
					arg2 = e2.getValue();
					tempIsolation = intraClusterScore(arg1, arg2);

					// get the maximum score, i.e the strongest intra-cluster
					// pair..
					maxIsolation = (maxIsolation < tempIsolation) ? tempIsolation
							: maxIsolation;
				}
			}

			// isoMcl.put(e1.getKey(), isolation);

			// perform its own compactness
			minCompactness = getInterClusterScore(e1.getValue());

			// System.out.println(e1.getKey() + "\t" + maxIsolation + "\t"
			// + minCompactness);

			clusterGoodness = clusterGoodness + (double) minCompactness
					/ ((maxIsolation == 0) ? Math.pow(10, -1) : maxIsolation);

			// System.out.println(clusterGoodness);

		}

		clusterGoodness = (clusterGoodness == 0) ? (Math.pow(10, -8) - clusterGoodness)
				: clusterGoodness;

		return (double) 1 / clusterGoodness;

		// return (double) isolation / n;
	}

	/**
	 * take 2 lists and find the max pairwise similarity score. this essentially
	 * gives the isolation score of two clusters represented by two lists
	 * 
	 * @param arg1
	 * @param arg2
	 * @return
	 */
	private static double intraClusterScore(List<String> arg1, List<String> arg2) {
		// compare each elements from argList1 vs argList2

		Pair<String, String> pair = null;
		double tempScore = 0;
		double maxScore = 0;

		for (int list1Id = 0; list1Id < arg1.size(); list1Id++) {
			for (int list2Id = 0; list2Id < arg2.size(); list2Id++) {

				// create a pair
				pair = new ImmutablePair<String, String>(arg1.get(list1Id)
						.trim(), arg2.get(list2Id).trim());

				try {
					// retrieve the key from the collection
					tempScore = KMediodCluster.getScoreMap().get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(arg2.get(
								list2Id).trim(), arg1.get(list1Id).trim());
						tempScore = KMediodCluster.getScoreMap().get(pair);

					} catch (Exception e1) {
						tempScore = 0;
					}
				}

				// System.out.println(" temp score = " + tempScore);
				maxScore = (tempScore > maxScore) ? tempScore : maxScore;
			}
		}

		// System.out.println("max = " + maxScore);
		return maxScore;
	}

	/**
	 * get the pairwise similarity scores for each elements in a cluster
	 * 
	 * @param cluster
	 * @return
	 */
	private static double getInterClusterScore(List<String> cluster) {

		Pair<String, String> pair = null;

		double score = 1;
		double tempScore = 0;

		// System.out.println("CL size " + cluster.size());
		if (cluster.size() <= 1)
			return 0;

		for (int outer = 0; outer < cluster.size(); outer++) {
			for (int inner = outer + 1; inner < cluster.size(); inner++) {
				// create a pair
				pair = new ImmutablePair<String, String>(cluster.get(outer)
						.trim(), cluster.get(inner).trim());

				try {
					// retrieve the key from the collection
					tempScore = KMediodCluster.getScoreMap().get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(cluster.get(
								inner).trim(), cluster.get(outer).trim());
						tempScore = KMediodCluster.getScoreMap().get(pair);

					} catch (Exception e1) {
						tempScore = 0;
					}
				}

				// for sum of all pairwise scores
				// score = score + tempScore;

				// for the minimum inter cluster score
				score = (score >= tempScore) ? tempScore : score;
			}
		}
		return score;
	}
}
