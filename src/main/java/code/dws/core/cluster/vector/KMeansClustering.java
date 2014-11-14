/**
 * 
 */
package code.dws.core.cluster.vector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import code.dws.utils.Utilities;
import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMeans;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.Instance;
import net.sf.javaml.core.SparseInstance;
import net.sf.javaml.distance.CosineSimilarity;
import net.sf.javaml.distance.DistanceMeasure;

/**
 * This is an alternate way of clustering the OIE properties. Here we use the
 * K-Means clustering scheme
 * 
 * @author adutta
 *
 */
public class KMeansClustering {

	// define Logger
	public static Logger logger = Logger.getLogger(KMeansClustering.class
			.getName());

	private static int NO_OF_CLUSTERS = 0;
	private static int NO_OF_ITERATIONS = 0;

	private static Map<Instance, String> INSTANCE_TO_PROPERTY_MAP = new HashMap<Instance, String>();

	private static int dataSize;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Instance inst = null;
		String oieProp = null;

		long start = System.currentTimeMillis();

		VectorCluster
				.main(new String[] { "/home/adutta/git/ESKoIE/CONFIG.cfg" });

		// parameter setting
		dataSize = 10;// VectorCluster.getDataSize();
		NO_OF_CLUSTERS = (int) Math.ceil(0.3 * dataSize) + 2;
		NO_OF_ITERATIONS = 5;

		// get the data set
		Dataset data = getDataSet();
		logger.info("Added " + data.size() + " data instances");

		// get the distance measure
		DistanceMeasure dm = getDistanceMeasure();

		Clusterer km = new KMeans(NO_OF_CLUSTERS, NO_OF_ITERATIONS, dm);
		logger.info("Performing K-Means with " + NO_OF_CLUSTERS + " clusters");

		// make a recursive call

		getRecursivelyDatasets(data, km);

		Utilities.endTimer(start, "Finished clustering in ");
	}

	/**
	 * find finer data sets recursively
	 * 
	 * @param ds
	 * @param km
	 * @return
	 */
	private static void getRecursivelyDatasets(Dataset ds, Clusterer km) {
		Instance inst = null;
		String oieProp = null;
		try {
			Dataset[] datasets = km.cluster(ds);

			for (int clusterCount = 0; clusterCount < datasets.length; clusterCount++) {
				Dataset cluster = datasets[clusterCount];
				
				if (cluster.size() > 2) { // clustering possible further
					getRecursivelyDatasets(cluster, km);
				} else { // no more sub clustering..print it out
					// each data set has multiple instances,
					for (int dsCntr = 0; dsCntr < cluster.size(); dsCntr++) {
						inst = cluster.instance(dsCntr);
						oieProp = INSTANCE_TO_PROPERTY_MAP.get(inst);
						System.out.print(oieProp + "\t");
					}
				}

				System.out.println("\n");
			}

		} catch (Exception e) {
			logger.error(e.getMessage());
		}		
		return;
	}

	private static DistanceMeasure getDistanceMeasure() {
		return new CosineSimilarity();
	}

	private static Dataset getDataSet() {
		SparseInstance inst = null;
		Dataset ds = new DefaultDataset();

		for (Map.Entry<String, Map<Integer, Double>> propMatrixEntry : VectorCluster.DOMAIN_FEATURE_MATRIX
				.entrySet()) {
			inst = new SparseInstance();
			inst.setNoAttributes(VectorCluster.featureSpace.size());
			// logger.info(inst.noAttributes());
			for (Map.Entry<Integer, Double> propVector : propMatrixEntry
					.getValue().entrySet()) {
				inst.put(propVector.getKey(), propVector.getValue());
			}
			// adding one entry at time
			ds.add(inst);
			if (ds.size() == dataSize)
				return ds;
			INSTANCE_TO_PROPERTY_MAP.put(inst, propMatrixEntry.getKey());
		}

		return ds;
	}
}
