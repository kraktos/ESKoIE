package code.dws.core.cluster.vector;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.javaml.clustering.evaluation.SumOfSquaredErrors;

import org.apache.log4j.Logger;

public class VectorClusterWrapper {

	// define Logger
	public static Logger logger = Logger.getLogger(KMeansClustering.class
			.getName());

	public static void main(String[] args) throws IOException {
		double score = Double.MAX_VALUE;
		double tScore = 0;
		int iOpt = 0;
		Map<String, List<String>> CLUSTERS = null;

		for (int i = 0; i < 300; i++) {
			KMeansClustering.main(new String[] { "" });
			tScore = new SumOfSquaredErrors().score(KMeansClustering.datasets);
			logger.info("Score at " + i + " = " + tScore);
			if (tScore < score) {
				CLUSTERS = new HashMap<String, List<String>>();
				score = tScore;
				iOpt = i;
				CLUSTERS = KMeansClustering.CLUSTERS;
			}
		}

		logger.info("best so far at " + iOpt);
		for (Map.Entry<String, List<String>> e : CLUSTERS.entrySet()) {
			logger.info(e.getKey() + " \t" + e.getValue());
		}
	}
}
