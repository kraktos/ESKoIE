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
	private static Map<Instance, String> INSTANCE_TO_PROPERTY_MAP_II = new HashMap<Instance, String>();
	public static Map<String, List<String>> CLUSTERS = null;

	private static int dataSize;

	static Dataset[] datasets;
	static Dataset[] datasets2;
	static int elemCntr = 0;

	static int sum = 0;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		sum = 0;
		CLUSTERS = new HashMap<String, List<String>>();
		VectorCluster
				.main(new String[] { "/home/adutta/git/ESKoIE/CONFIG.cfg" });

		// parameter setting
		dataSize = VectorCluster.getDataSize();
		NO_OF_CLUSTERS = (int) Math.ceil(0.5 * Integer.valueOf(args[0]));
		NO_OF_ITERATIONS = 80;

		// get the data set
		Dataset data = getDataSet();
		logger.info("Added " + data.size() + " data instances");

		// get the distance measure
		DistanceMeasure dm = getDistanceMeasure();

		Clusterer km = new KMedoids(NO_OF_CLUSTERS, NO_OF_ITERATIONS, dm);
		logger.info("Performing K-Means with " + NO_OF_CLUSTERS*2 + " clusters");

		// make a recursive call
		clusterRecursively(data, km);
	}

	/**
	 * find finer data sets recursively
	 * 
	 * @param ds
	 * @param km
	 * @return
	 */
	private static void clusterRecursively(Dataset ds, Clusterer km) {
		Instance inst = null;
		String oieProp = null;
		List<String> clusterElems = null;

		try {
			datasets = km.cluster(ds);

			for (int clusterCount = 0; clusterCount < datasets.length; clusterCount++) {
				// elemCntr++;

				Dataset cluster = datasets[clusterCount];
				clusterElems = new ArrayList<String>();

				for (int dsCntr = 0; dsCntr < cluster.size(); dsCntr++) {
					inst = cluster.instance(dsCntr);
					oieProp = INSTANCE_TO_PROPERTY_MAP.get(inst);

					// add the elements clustered on Domain

					if (oieProp != null)
						clusterElems.add(oieProp);
				}

				// System.out.println("original == " + clusterElems.size());
				// group them and re cluster on Range
				reCluster(clusterElems);

				// put it into collection
				// CLUSTERS.put("C" + elemCntr, clusterElems);
			}
		} catch (Exception e) {
			e.printStackTrace();
			logger.error(e.getMessage());
		}

		// Utilities.endTimer(start, "Finished clustering in ");
	}

	private static void reCluster(List<String> clusterElems2) {

		List<String> clusterElems = null;
		Instance inst = null;
		String oieProp = null;

		// get the data set
		Dataset data = getDataSet2(clusterElems2);

		// NO_OF_CLUSTERS = (clusterElems2.size() > 15) ? (int) (0.3 *
		// clusterElems2
		// .size()) : 2;

		NO_OF_CLUSTERS = 2;

		Clusterer km = new KMedoids(NO_OF_CLUSTERS, NO_OF_ITERATIONS,
				new CosineSimilarity());

		try {
			datasets2 = km.cluster(data);

			for (int clusterCount = 0; clusterCount < datasets2.length; clusterCount++) {
				// elemCntr++;

				Dataset cluster = datasets2[clusterCount];
				clusterElems = new ArrayList<String>();

				for (int dsCntr = 0; dsCntr < cluster.size(); dsCntr++) {
					inst = cluster.instance(dsCntr);
					oieProp = INSTANCE_TO_PROPERTY_MAP_II.get(inst);

					clusterElems.add(oieProp);
				}
				// System.out.println("\t Splitted into = " +
				// clusterElems.size());
				// put it into collection
				CLUSTERS.put("C" + elemCntr++, clusterElems);

				sum = sum + clusterElems.size();
				// System.out.println("hmmm " + sum);
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	/**
	 * adding the elements grouped by Domain
	 * 
	 * @param clusterElements
	 * @return
	 */
	private static Dataset getDataSet2(List<String> clusterElements) {
		SparseInstance inst = null;
		Dataset ds = new DefaultDataset();

		for (String elem : clusterElements) {
			inst = new SparseInstance();
			inst.setNoAttributes(VectorCluster.featureSpace.size());
			Map<Integer, Double> tmap = VectorCluster.RANGE_FEATURE_MATRIX
					.get(elem);
			if (tmap == null)
				System.out.println(elem);

			for (Map.Entry<Integer, Double> propVector : tmap.entrySet()) {
				inst.put(propVector.getKey(), propVector.getValue());
			}
			// adding one entry at time
			ds.add(inst);
			if (ds.size() == dataSize)
				return ds;
			INSTANCE_TO_PROPERTY_MAP_II.put(inst, elem);
		}
		return ds;
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
