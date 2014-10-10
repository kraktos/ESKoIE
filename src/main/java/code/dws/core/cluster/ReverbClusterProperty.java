/**
 * 
 */
package code.dws.core.cluster;

import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.RelatednessCalculator;
import edu.cmu.lti.ws4j.impl.JiangConrath;
import edu.cmu.lti.ws4j.impl.LeacockChodorow;
import edu.cmu.lti.ws4j.impl.Lin;
import edu.cmu.lti.ws4j.impl.Path;
import edu.cmu.lti.ws4j.impl.Resnik;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.cmu.lti.ws4j.util.WS4JConfiguration;
import gnu.trove.map.hash.THashMap;
import gnu.trove.set.hash.THashSet;
import gnu.trove.set.hash.TLinkedHashSet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.experiment.PropertyGoldStandard;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;
import code.dws.wordnet.SimilatityWebService;
import code.dws.wordnet.WordNetAPI;

import com.google.common.collect.Sets;

/**
 * this class clubs together properties with similar domain, range distribution
 * 
 * @author arnab
 */
public class ReverbClusterProperty {

	/**
	 * logger
	 */
	// define Logger
	public static Logger logger = Logger.getLogger(ReverbClusterProperty.class
			.getName());

	public static int TOPK_REV_PROPS = -1;

	private static final String DELIMIT = "\t";

	/*
	 * output location for the type pairs and the properties that are common
	 */
	private static final String CLUSTERS = "CLUSTERS_TYPE";

	private static final String CLUSTERS_NAME = "src/main/resources/input/CLUSTERS_";

	private static List<String> revbProps = null;

	private static Map<String, THashSet<ImmutablePair<String, String>>> ALL_PROPS = new HashMap<String, THashSet<ImmutablePair<String, String>>>();

	/**
     * 
     */
	public ReverbClusterProperty() {
		//
	}

	private static Map<Pair<String, String>, Map<String, Double>> MAP_CLUSTER = new HashMap<Pair<String, String>, Map<String, Double>>();
	private static Map<String, Set<String>> propertBasicCluster = new ConcurrentHashMap<String, Set<String>>();
	private static THashMap<String, String> BASIC_CLUSTER_DICT = new THashMap<String, String>();

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Constants.loadConfigParameters(new String[] { "", args[0] });

		logger.info("Running " + ReverbClusterProperty.class.getName());

		// retrieve a set of OIE properties
		revbProps = PropertyGoldStandard.getReverbProperties(
				Constants.OIE_DATA_PATH, -1,
				Long.parseLong(Constants.INSTANCE_THRESHOLD));

		logger.info("Loaded " + revbProps.size() + " OIE properties");

		// Wordnet specific init block
		System.setProperty("wordnet.database.dir", Constants.WORDNET_DICTIONARY);
		WS4JConfiguration.getInstance().setMFS(true);

		doBasicPatternClustering();

		// enable scoring mechanism
		doScoring();

		// dumpPropCluster();

	}

	private static void doBasicPatternClustering() throws IOException {

		String arg1 = null;
		String arg2 = null;
		String newKey = null;
		Set<String> newValues = null;
		Set<String> props = null;
		Set<String> currentList = null;

		try {

			// iterate the list of size n, n(n-1)/2 comparison !! :D
			for (int outerIdx = 0; outerIdx < revbProps.size(); outerIdx++) {
				// init list
				props = new HashSet<String>();
				for (int innerIdx = outerIdx + 1; innerIdx < revbProps.size(); innerIdx++) {

					arg1 = revbProps.get(outerIdx);
					arg2 = revbProps.get(innerIdx);

					// apply modular clustering technique
					// first group all properties with similar patterns (was
					// student at is currently student at..so on)

					if (canBeAdded(arg1, arg2)) {
						props.add(arg2);
					}
				}
				// if (arg1.indexOf("married to") != -1)
				propertBasicCluster.put(arg1, props);
			}

			logger.info("Before pattern clustering "
					+ propertBasicCluster.size());

			// collate the clusters
			for (Map.Entry<String, Set<String>> e : propertBasicCluster
					.entrySet()) {

				currentList = e.getValue();

				newKey = e.getKey();
				for (String s : currentList) {
					if (s.length() < newKey.length())
						newKey = s;
				}
				if (!newKey.equals(e.getKey())) {
					currentList.remove(newKey);
					currentList.add(e.getKey());

					newValues = propertBasicCluster.get(newKey);
					currentList.addAll(newValues);
					propertBasicCluster.remove(e.getKey());
					propertBasicCluster.put(newKey, currentList);
				}
			}

			// at this point there is a basic pattern based cluster already in
			// place..improve more with markov cluster.
			for (Map.Entry<String, Set<String>> e : propertBasicCluster
					.entrySet()) {
				if (e.getValue().size() == 0) {
					propertBasicCluster.remove(e.getKey());
				} else {
					BASIC_CLUSTER_DICT.put(e.getKey(), e.getKey());
					for (String elem : e.getValue()) {
						BASIC_CLUSTER_DICT.put(elem, e.getKey());
					}
				}
			}
			logger.info("After pattern clustering "
					+ propertBasicCluster.size());

		} catch (Exception e) {
			logger.error(e.getMessage());
		}
	}

	private static void doScoring() throws IOException {
		String arg1 = null;
		String arg2 = null;

		long cnt = 0;

		double ovlapScore = 0;
		double wnScore = 0;
		double tokenScore = 0;

		BufferedWriter writerRevWordnetSims = new BufferedWriter(
				new FileWriter(new File(Constants.OIE_DATA_PATH).getParent()
						+ "/all.trvb.wordnet.sim.csv"));

		BufferedWriter writerRevOverlapSims = new BufferedWriter(
				new FileWriter(new File(Constants.OIE_DATA_PATH).getParent()
						+ "/all.trvb.overlap.sim.csv"));

		ILexicalDatabase db = new NictWordNet();

		RelatednessCalculator[] rcs = { new WuPalmer(db),
				new LeacockChodorow(db), new Resnik(db), new JiangConrath(db),
				new Lin(db), new Path(db) };

		// BufferedWriter writerRevDbpWordnetSims = new BufferedWriter(
		// new FileWriter(new File(Constants.REVERB_DATA_PATH).getParent()
		// + "/all.trvb.dbp.wordnet.sim.csv"));

		long start = Utilities.startTimer();

		long propSize = revbProps.size();
		long size = propSize * (propSize - 1);

		try {

			// iterate the list of size n, n(n-1)/2 comparison !! :D
			for (int outerIdx = 0; outerIdx < revbProps.size(); outerIdx++) {

				for (int innerIdx = outerIdx + 1; innerIdx < revbProps.size(); innerIdx++) {

					cnt++;

					arg1 = revbProps.get(outerIdx);
					arg2 = revbProps.get(innerIdx);

					if (!isPatternClustered(arg1, arg2)) {
						tokenScore = getTokenScore(arg1.split(" "),
								arg2.split(" "));
						wnScore = WordNetAPI.scoreWordNet(rcs, arg1.split(" "),
								arg2.split(" "));

						if (wnScore > 0) {
							if (wnScore == 1)
								writerRevWordnetSims.write(arg1 + "\t" + arg2
										+ "\t" + tokenScore + "\n");
							else
								writerRevWordnetSims.write(arg1 + "\t" + arg2
										+ "\t" + Math.max(wnScore, tokenScore)
										+ "\n");
						}
					} else {
						writerRevWordnetSims.write(arg1 + "\t" + arg2 + "\t"
								+ 1.00 + "\n");
					}
					// List<Worker> workerList = new ArrayList<Worker>();
					// for (int i = 0; i < nrOfProcessors; i++) {
					// workerList.add(new Worker(db, rcs,
					// revbProps.get(outerIdx), revbProps.get(innerIdx)));
					// innerIdx++;
					// }

					// results = eservice.invokeAll(workerList);

					// for (int i = 0; i < nrOfProcessors; i++) {
					// futuresList.add(eservice.submit(new Worker(db, rcs,
					// revbProps.get(outerIdx), revbProps
					// .get(innerIdx))));
					// }
					// int i = 0;
					// Double[] wnScore = new Double[nrOfProcessors];
					//
					// for (Future<Double> future : results) {
					// try {
					// wnScore[i++] = future.get();
					// // System.out.println(revbProps.get(outerIdx) + "\t" +
					// revbProps.get(innerIdx) + "\t"
					// // + wnScore);
					// } catch (InterruptedException e) {
					// } catch (ExecutionException e) {
					// }
					// }

					// based on number of common instance pairs for each
					// property
					// ovlapScore = getInstanceOverlapSimilarityScores(outerIdx,
					// innerIdx);
					//
					// cnt++;
					// // System.out.println(cnt + "\t"
					// // + (System.currentTimeMillis() - start));
					//
					// if (ovlapScore != 0) {
					// writerRevOverlapSims.write(revbProps.get(outerIdx)
					// + "\t" + revbProps.get(innerIdx) + "\t"
					// + ovlapScore + "\n");
					//
					// // wnScore = getWordNetSimilarityScores(outerIdx,
					// // innerIdx);
					// wnScore = WordNetAPI.scoreWordNet(rcs,
					// revbProps.get(outerIdx).split(" "),
					// revbProps.get(innerIdx).split(" "));
					// System.out.println(revbProps.get(outerIdx) + "\t"
					// + revbProps.get(innerIdx) + "\t" + wnScore);
					// if (wnScore > 0)
					// writerRevWordnetSims.write(revbProps.get(outerIdx)
					// + "\t" + revbProps.get(innerIdx) + "\t"
					// + wnScore + "\n");
					// }

					// writerRevOverlapSims.flush();
					// writerRevWordnetSims.flush();

					if (cnt > 1000 && cnt % 1000 == 0)
						Utilities.endTimer(start, 200 * ((double) cnt / size)
								+ " percent done in ");
				}

				writerRevOverlapSims.flush();
				writerRevWordnetSims.flush();
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			writerRevOverlapSims.close();
			writerRevWordnetSims.close();

		}
	}

	private static double getTokenScore(String[] split, String[] split2) {

		int num = 0;
		int denom = split.length + split2.length;
		for (String s1 : split) {
			for (String s2 : split2) {
				if (s1.equals(s2))
					num++;
			}
		}
		return (double) 2 * num / denom;
	}

	private static boolean isPatternClustered(String arg1, String arg2) {

		if (BASIC_CLUSTER_DICT.get(arg2) != null
				&& BASIC_CLUSTER_DICT.get(arg1) != null) {
			if (arg1.equals(BASIC_CLUSTER_DICT.get(arg2))
					|| BASIC_CLUSTER_DICT.get(arg1).equals(
							BASIC_CLUSTER_DICT.get(arg2))) {
				logger.info("Skipping " + arg1 + "\t" + arg2);
				return true;
			} else
				return false;
		} else
			return false;
	}

	private static boolean canBeAdded(String arg1, String arg2) {
		boolean flag1 = false;
		boolean flag2 = false;

		String regex = "\\b" + arg2 + "\\b";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(arg1);

		if (matcher.find()) {
			flag1 = true;
		}

		regex = "\\b" + arg1 + "\\b";
		pattern = Pattern.compile(regex);
		matcher = pattern.matcher(arg2);

		if (matcher.find()) {
			flag2 = true;
		}

		return flag1 || flag2;
	}

	/**
	 * get the list of Reverb properties CAn be used to get both top-k
	 * properties, or properties with atleast x number of instances
	 * 
	 * @param OIE_FILE
	 * @param TOPK_REV_PROPS
	 * @param atLeastInstancesCount
	 * @return List of properties
	 */
	public static List<String> getReverbProperties(String OIE_FILE,
			int TOPK_REV_PROPS, Long atLeastInstancesCount) {

		String line = null;
		String[] arr = null;
		long val = 0;
		int c = 0;
		List<String> ret = new ArrayList<String>();
		THashSet<ImmutablePair<String, String>> list = null;
		THashMap<String, Long> COUNT_PROPERTY_INST = new THashMap<String, Long>();

		try {
			Scanner scan = new Scanner(new File(OIE_FILE));

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split(";");
				if (COUNT_PROPERTY_INST.containsKey(arr[1])) {
					val = COUNT_PROPERTY_INST.get(arr[1]);
					val++;
				} else {
					val = 1;
				}
				COUNT_PROPERTY_INST.put(arr[1], val);

				if (ALL_PROPS.containsKey(arr[1])) {
					list = ALL_PROPS.get(arr[1]);
				} else {
					list = new TLinkedHashSet<ImmutablePair<String, String>>();
				}
				list.add(new ImmutablePair<String, String>(arr[0], arr[2]));
				ALL_PROPS.put(arr[1], list);

			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		// load the properties with atleast 500 instances each
		COUNT_PROPERTY_INST = Utilities.sortByValue(COUNT_PROPERTY_INST,
				atLeastInstancesCount);

		for (Entry<String, Long> e : COUNT_PROPERTY_INST.entrySet())
			ret.add(e.getKey());

		return ret;
	}

	/**
	 * get the list of Reverb properties
	 * 
	 * @param OIE_FILE
	 * @param TOPK_REV_PROPS
	 * @return List of properties
	 */
	public static List<String> getReverbProperties(String OIE_FILE,
			int TOPK_REV_PROPS) {

		Map<String, Long> counts = new HashMap<String, Long>();

		String line = null;
		String[] arr = null;
		long val = 0;
		int c = 0;
		List<String> ret = new ArrayList<String>();
		THashSet<ImmutablePair<String, String>> list = null;

		try {
			Scanner scan = new Scanner(new File(OIE_FILE));

			while (scan.hasNextLine()) {
				line = scan.nextLine();
				arr = line.split(";");
				if (counts.containsKey(arr[1])) {
					val = counts.get(arr[1]);
					val++;
				} else {
					val = 1;
				}
				counts.put(arr[1], val);

				if (ALL_PROPS.containsKey(arr[1])) {
					list = ALL_PROPS.get(arr[1]);
				} else {
					list = new TLinkedHashSet<ImmutablePair<String, String>>();
				}
				list.add(new ImmutablePair<String, String>(arr[0], arr[2]));
				ALL_PROPS.put(arr[1], list);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		if (TOPK_REV_PROPS != -1)
			counts = Utilities.sortByValue(counts);

		for (Entry<String, Long> e : counts.entrySet()) {
			ret.add(e.getKey());
			c++;
			if (TOPK_REV_PROPS != -1 && c == TOPK_REV_PROPS)
				return ret;
		}

		return ret;
		// try {
		// // init DB
		// DBWrapper.init(Constants.GET_DISTINCT_REVERB_PROPERTIES);
		//
		// List<String> results = DBWrapper
		// .fetchDistinctReverbProperties(TOPK_REV_PROPS);
		//
		// return (results != null) ? results : new ArrayList<String>();
		// } finally {
		// DBWrapper.shutDown();
		//
		// }

	}

	/**
	 * method to compute properties sharing reverb instances
	 * 
	 * @param id2
	 * @param id
	 * @param writerOverlap
	 * @return
	 * @return
	 * @throws Exception
	 */
	private static double getInstanceOverlapSimilarityScores(int outerIdx,
			int innerIdx) throws Exception {

		String propArg1 = revbProps.get(outerIdx);
		String propArg2 = revbProps.get(innerIdx);

		THashSet<ImmutablePair<String, String>> revbSubObj1 = ALL_PROPS
				.get(propArg1);

		THashSet<ImmutablePair<String, String>> revbSubObj2 = ALL_PROPS
				.get(propArg2);

		long min = Math.min(revbSubObj1.size(), revbSubObj2.size());

		double scoreOverlap = 0;

		// scoreOverlap = (double) CollectionUtils.intersection(revbSubObj1,
		// revbSubObj2)
		// .size() / min;

		scoreOverlap = (double) Sets.intersection(revbSubObj1, revbSubObj2)
				.size() / min;

		if (scoreOverlap > 0.002
				&& min >= Long.parseLong(Constants.INSTANCE_THRESHOLD))
			return scoreOverlap;

		return 0;
	}

	/**
	 * call the web service to compute the inter phrase similarity
	 * 
	 * @param properties
	 * @param properties
	 * @param id2
	 * @param id
	 * @return
	 * @throws Exception
	 */
	private static double getWordNetSimilarityScores(int id, int id2)
			throws Exception {

		double score = SimilatityWebService.getSimScore(revbProps.get(id),
				revbProps.get(id2));

		return score;

	}

	/**
	 * write out the clusters to a file
	 * 
	 * @throws IOException
	 */
	private static void dumpPropCluster() throws IOException {
		BufferedWriter outputWriter = null;
		try {
			outputWriter = new BufferedWriter(new FileWriter(CLUSTERS));
			for (Entry<Pair<String, String>, Map<String, Double>> e : MAP_CLUSTER
					.entrySet()) {

				for (Entry<String, Double> propEntry : e.getValue().entrySet()) {

					outputWriter.write(propEntry.getValue() + "\t"
							+ e.getKey().getLeft() + "\t"
							+ e.getKey().getRight() + "\t" + propEntry.getKey()
							+ "\n");
				}
				outputWriter.flush();
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			outputWriter.close();
		}

	}

}
