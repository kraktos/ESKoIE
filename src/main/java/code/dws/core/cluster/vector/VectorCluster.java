/**
 * 
 */
package code.dws.core.cluster.vector;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.grouplens.lenskit.vectors.MutableSparseVector;
import org.grouplens.lenskit.vectors.SparseVector;
import org.grouplens.lenskit.vectors.similarity.CosineVectorSimilarity;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

/**
 * Vector based clustering scheme.
 * 
 * @author adutta
 *
 */
public class VectorCluster {
	public static final Map<String, MutableSparseVector> DOMAIN_FEATURE_GLOBAL_MATRIX = new HashMap<String, MutableSparseVector>();
	public static final Map<String, MutableSparseVector> RANGE_FEATURE_GLOBAL_MATRIX = new HashMap<String, MutableSparseVector>();

	public static final Map<String, Map<Integer, Double>> DOMAIN_FEATURE_MATRIX = new HashMap<String, Map<Integer, Double>>();
	public static final Map<String, Map<Integer, Double>> RANGE_FEATURE_MATRIX = new HashMap<String, Map<Integer, Double>>();

	// define Logger
	public static Logger logger = Logger.getLogger(VectorCluster.class
			.getName());

	private static List<String> oieProps = null;

	public static List<String> featureSpace = new ArrayList<String>();

	private static boolean consoleMode = false;

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		// init params
		Constants.loadConfigParameters(new String[] { "", args[0] });

		// only f+ properties
		DBWrapper.init(Constants.GET_FULLY_MAPPED_OIE_PROPS_SQL);
		oieProps = DBWrapper.getFullyMappedFacts();

		logger.info("Loaded " + oieProps.size() + " OIE properties");

		// generate feature vectors for the domain and range
		createFeatureVectors();
		logger.info("Created domain and range feature vector");

		long start = Utilities.startTimer();

		// do the pairwise scoring for each pairs of OIE properties
		// doPairwiseScoring();
		Utilities.endTimer(start, "Completed scoring " + oieProps.size()
				+ " properties in ");
	}

	private static void doPairwiseScoring() throws IOException {
		if (consoleMode) {
			logger.info("Enter your query term. Press 'q' to quit entering");

			// query now
			BufferedReader console = new BufferedReader(new InputStreamReader(
					System.in));
			CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();

			while (true) {
				String scan = console.readLine().trim();
				if (!scan.equals("Q")) {
					getTopKItems(20, scan, cosineSim);
				} else {
					System.exit(1);
				}
			}
		} else {
			doScoring();
		}
	}

	/**
	 * read the basic cluster pattern and try to improve the scoring
	 * 
	 * @throws IOException
	 */
	private static void doScoring() throws IOException {
		String arg1 = null;
		String arg2 = null;

		double score = 0;

		BufferedWriter writerVectorSims = new BufferedWriter(new FileWriter(
				new File(Constants.OIE_DATA_PATH).getParent()
						+ "/rvb.vector.sim.csv"));

		try {
			CosineVectorSimilarity cosineSim = new CosineVectorSimilarity();

			for (int outerIdx = 0; outerIdx < oieProps.size(); outerIdx++) {

				arg1 = oieProps.get(outerIdx);

				for (int innerIdx = outerIdx + 1; innerIdx < oieProps.size(); innerIdx++) {
					arg2 = oieProps.get(innerIdx);

					score = checkSim(arg1, arg2, cosineSim);

					writerVectorSims.write(arg1 + "\t" + arg2 + "\t"
							+ Constants.formatter.format(score) + "\n");
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {

			writerVectorSims.flush();
			writerVectorSims.close();

		}
	}

	/**
	 * get the most similar items for a given phrase item
	 * 
	 * @param k
	 * @param cosineSim
	 * @return
	 */
	public static void getTopKItems(int k, String queryPhrase,
			CosineVectorSimilarity cosineSim) {

		double simScore = 0;

		SparseVector domVectArg2 = null;
		SparseVector ranVectArg2 = null;

		Map<String, Double> similarItemsMap = new LinkedHashMap<String, Double>();

		// get the query phrase vector
		SparseVector domVectArg1 = DOMAIN_FEATURE_GLOBAL_MATRIX.get(queryPhrase
				.trim());
		SparseVector ranVectArg1 = RANGE_FEATURE_GLOBAL_MATRIX.get(queryPhrase
				.trim());

		for (String phrase : oieProps) {
			// CosineVectorSimilarity cosineSim2 = new CosineVectorSimilarity();
			// get the dom/ran vectors for this phrase
			domVectArg2 = DOMAIN_FEATURE_GLOBAL_MATRIX.get(phrase.trim());
			ranVectArg2 = RANGE_FEATURE_GLOBAL_MATRIX.get(phrase.trim());

			if (domVectArg2 != null && ranVectArg2 != null
					&& domVectArg1 != null && ranVectArg1 != null)
				simScore = cosineSim.similarity(domVectArg1, domVectArg2)
						+ cosineSim.similarity(ranVectArg1, ranVectArg2);

			similarItemsMap.put(phrase, (double) simScore / 2);
		}

		similarItemsMap = sortByValue(similarItemsMap);
		int cnt = 0;
		for (Map.Entry<String, Double> e : similarItemsMap.entrySet()) {
			if (cnt != k) {
				cnt++;
				logger.info("" + e.getKey() + "\t" + e.getValue());
			}
		}
	}

	/**
	 * create a feature vector for the given OIE property
	 */
	private static void createFeatureVectors() {
		try {
			getFeatures(Constants.GET_DOMAINS, DOMAIN_FEATURE_GLOBAL_MATRIX,
					DOMAIN_FEATURE_MATRIX);
			getFeatures(Constants.GET_RANGES, RANGE_FEATURE_GLOBAL_MATRIX,
					RANGE_FEATURE_MATRIX);

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
	 * @param domainFeatureMatrix
	 */
	private static void getFeatures(String sql,
			Map<String, MutableSparseVector> globalMAtrix,
			Map<String, Map<Integer, Double>> featureMatrix) {

		Long featureID = null;
		double score = 0;

		// init DB
		DBWrapper.init(sql);

		List<String> features;
		Map<Long, Double> lnFeatureIdVsScore = null;
		Map<Integer, Double> intFeatureIdVsScore = null;

		for (String prop : oieProps) {
			lnFeatureIdVsScore = new HashMap<Long, Double>();
			intFeatureIdVsScore = new HashMap<Integer, Double>();

			features = DBWrapper.getOIEFeatures(prop);
			for (String feature : features) {
				featureID = new Long(featureSpace.indexOf(feature));

				if (!lnFeatureIdVsScore.containsKey(featureID)) {
					lnFeatureIdVsScore.put(featureID, 1D);
					intFeatureIdVsScore.put(
							new Integer(featureSpace.indexOf(feature)), 1D);
				}
			}
			// put in the global matrix
			globalMAtrix.put(prop,
					MutableSparseVector.create(lnFeatureIdVsScore));

			featureMatrix.put(prop, intFeatureIdVsScore);
		}
		DBWrapper.shutDown();
	}

	/**
	 * get the size of oie properties
	 * 
	 * @return
	 */
	public static int getDataSize() {
		return oieProps.size();
	}

	/**
	 * sort a map by value descending
	 * 
	 * @param map
	 * @param totalScore
	 * @param tripleCounter
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static Map<String, Double> sortByValue(Map map) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry<String, Double> entry = (Map.Entry) it.next();
			result.put(entry.getKey(), entry.getValue());
		}
		return result;
	}

	/**
	 * test routine to check pairwise scores
	 * 
	 * @param originalArg1
	 * @param originalArg2
	 * @param cosineSim
	 * 
	 * @param val1
	 * @param val2
	 * @param cosineSim
	 * @return
	 */
	private static double checkSim(String originalArg1, String originalArg2,
			CosineVectorSimilarity cosineSim) {
		double domScore = 0;
		double ranScore = 0;

		SparseVector arg1 = VectorCluster.DOMAIN_FEATURE_GLOBAL_MATRIX
				.get(originalArg1.trim());
		SparseVector arg2 = VectorCluster.DOMAIN_FEATURE_GLOBAL_MATRIX
				.get(originalArg2.trim());

		if (arg1 != null && arg2 != null) {
			domScore = cosineSim.similarity(arg1, arg2);
		}

		arg1 = VectorCluster.RANGE_FEATURE_GLOBAL_MATRIX.get(originalArg1
				.trim());
		arg2 = VectorCluster.RANGE_FEATURE_GLOBAL_MATRIX.get(originalArg2
				.trim());

		if (arg1 != null && arg2 != null) {
			ranScore = cosineSim.similarity(arg1, arg2);
		}

		return (double) (domScore + ranScore) / 2;

	}
}
