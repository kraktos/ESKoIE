package code.dws.core.cluster.vector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.javaml.clustering.evaluation.SumOfSquaredErrors;

import org.apache.log4j.Logger;

public class VectorClusterWrapper {

	private static final String MEDIOD_CLUSTERS_OP = "/home/adutta/git/ESKoIE/src/main/resources/vector_Clusters/KMEDIOD_CLUSTERS.";
	// define Logger
	public static Logger logger = Logger.getLogger(KMeansClustering.class
			.getName());

	public static void main(String[] args) throws IOException {
		double score = Double.MAX_VALUE;
		double tScore = 0;
		int iOpt = 0;
		Map<String, List<String>> CLUSTERS = null;

		BufferedWriter writer = null;
		int bang;

		for (int noOfClusters = 10; noOfClusters < 82; noOfClusters++) {

			writer = new BufferedWriter(new FileWriter(MEDIOD_CLUSTERS_OP
					+ noOfClusters + ".out"));

			for (int i = 0; i < 5; i++) {

				KMeansClustering.main(new String[] { String
						.valueOf(noOfClusters) });
				tScore = new SumOfSquaredErrors()
						.score(KMeansClustering.datasets);
				logger.info("Score at " + i + " = " + tScore);
				if (tScore < score) {
					CLUSTERS = new HashMap<String, List<String>>();
					score = tScore;
					iOpt = i;
					CLUSTERS = KMeansClustering.CLUSTERS;
				}
			}
			bang = 0;
			for (Map.Entry<String, List<String>> e : CLUSTERS.entrySet()) {
				for (String elem : e.getValue()) {
					bang++;
					writer.write(elem + "\t");
				}
				writer.write("\n");
				writer.flush();
			}

			logger.info("best far " + noOfClusters + " at " + iOpt
					+ " size of elems = " + CLUSTERS.size() + " with " + bang
					+ " elemnets ");

		} // end of for outer loop
	}
}
