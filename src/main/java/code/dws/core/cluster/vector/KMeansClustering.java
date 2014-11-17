/**
 * 
 */
package code.dws.core.cluster.vector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.javaml.clustering.Clusterer;
import net.sf.javaml.clustering.KMedoids;
import net.sf.javaml.core.Dataset;
import net.sf.javaml.core.DefaultDataset;
import net.sf.javaml.core.Instance;
import net.sf.javaml.core.SparseInstance;
import net.sf.javaml.distance.CosineSimilarity;
import net.sf.javaml.distance.DistanceMeasure;

import org.apache.log4j.Logger;

import code.dws.utils.Utilities;

/**
 * This is an alternate way of clustering the OIE properties. Here we use the
 * K-Mediods clustering scheme
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
	public static Map<String, List<String>> CLUSTERS = new HashMap<String, List<String>>();

	private static int dataSize;

	static Dataset[] datasets;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		VectorCluster
				.main(new String[] { "/home/adutta/git/ESKoIE/CONFIG.cfg" });

		// parameter setting
		dataSize = VectorCluster.getDataSize();
		NO_OF_CLUSTERS = (int) Math.ceil(Integer.valueOf(args[0]));
		NO_OF_ITERATIONS = 80;

		// get the data set
		Dataset data = getDataSet();
		logger.debug("Added " + data.size() + " data instances");

		// get the distance measure
		DistanceMeasure dm = getDistanceMeasure(data);

		Clusterer km = new KMedoids(NO_OF_CLUSTERS, NO_OF_ITERATIONS, dm);
		logger.debug("Performing K-Means with " + NO_OF_CLUSTERS + " clusters");

		// make a recursive call
		getRecursivelyDatasets(data, km);

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
		long start = System.currentTimeMillis();
		List<String> clusterElems = null;

		int elemCntr = 0;
		try {
			datasets = km.cluster(ds);

			for (int clusterCount = 0; clusterCount < datasets.length; clusterCount++) {
				elemCntr++;

				Dataset cluster = datasets[clusterCount];
				clusterElems = new ArrayList<String>();

				for (int dsCntr = 0; dsCntr < cluster.size(); dsCntr++) {
					inst = cluster.instance(dsCntr);
					oieProp = INSTANCE_TO_PROPERTY_MAP.get(inst);

					clusterElems.add(oieProp);
				}
				// put it into collection
				CLUSTERS.put("C" + elemCntr, clusterElems);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}

		// Utilities.endTimer(start, "Finished clustering in ");
	}

	private static DistanceMeasure getDistanceMeasure(Dataset data) {
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
