/**
 * 
 */
package code.dws.core.cluster.vector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.grouplens.lenskit.vectors.MutableSparseVector;
import org.grouplens.lenskit.vectors.SparseVector;
import org.grouplens.lenskit.vectors.similarity.CosineVectorSimilarity;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;

/**
 * Vector based clustering scheme.
 * 
 * @author adutta
 *
 */
public class VectorCluster {
	private static final Map<String, MutableSparseVector> DOMAIN_FEATURE_GLOBAL_MATRIX = new HashMap<String, MutableSparseVector>();
	private static final Map<String, MutableSparseVector> RANGE_FEATURE_GLOBAL_MATRIX = new HashMap<String, MutableSparseVector>();

	// define Logger
	public static Logger logger = Logger.getLogger(VectorCluster.class
			.getName());

	private static List<String> revbProps = null;

	public static List<String> featureSpace = new ArrayList<String>();

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		// init params
		Constants.loadConfigParameters(new String[] { "", args[0] });

		// only f+ properties
		DBWrapper.init(Constants.GET_FULLY_MAPPED_OIE_PROPS_SQL);
		revbProps = DBWrapper.getFullyMappedFacts();

		logger.info("Loaded " + revbProps.size() + " OIE properties");

		// generate feature vectors for the domain and range
		createFeatureVectors();

		// test point
		checkSim();

	}

	/**
	 * test routine to check pairwise scores
	 */
	private static void checkSim() {
		String val1 = "currently lives in";
		String val2 = "currently resides in";

		CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();

		SparseVector arg1 = DOMAIN_FEATURE_GLOBAL_MATRIX.get(val1.trim());
		SparseVector arg2 = DOMAIN_FEATURE_GLOBAL_MATRIX.get(val2.trim());

		if (arg1 != null && arg2 != null)
			logger.info("Similarity between "
					+ cosineSim.similarity(arg1, arg2));

		arg1 = RANGE_FEATURE_GLOBAL_MATRIX.get(val1.trim());
		arg2 = RANGE_FEATURE_GLOBAL_MATRIX.get(val2.trim());

		if (arg1 != null && arg2 != null)
			logger.info("Similarity between "
					+ cosineSim.similarity(arg1, arg2));

	}

	/**
	 * create a feature vector for the given OIE property
	 */
	private static void createFeatureVectors() {
		try {
			getFeatures(Constants.GET_DOMAINS, DOMAIN_FEATURE_GLOBAL_MATRIX);
			getFeatures(Constants.GET_RANGES, RANGE_FEATURE_GLOBAL_MATRIX);

		} catch (Exception e) {
			logger.error("Problem with readOIEFile");
			e.printStackTrace();
		} finally {
			DBWrapper.shutDown();
		}
	}

	/**
	 * get features individually for the domain and range
	 * 
	 * @param sql
	 * @param globalMAtrix
	 */
	private static void getFeatures(String sql,
			Map<String, MutableSparseVector> globalMAtrix) {

		// init DB
		DBWrapper.init(sql);

		List<String> features;
		Map<Long, Double> featureIdVsScore = null;

		for (String prop : revbProps) {
			featureIdVsScore = new HashMap<Long, Double>();

			features = DBWrapper.getOIEFeatures(prop);
			for (String feature : features) {
				featureIdVsScore.put(new Long(featureSpace.indexOf(feature)),
						1D);
			}
			// put in the global matrix
			globalMAtrix
					.put(prop, MutableSparseVector.create(featureIdVsScore));

		}
		DBWrapper.shutDown();
	}
}
