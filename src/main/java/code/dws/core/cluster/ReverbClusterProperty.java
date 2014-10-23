/**
 * 
 */
package code.dws.core.cluster;

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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.utils.Constants;
import code.dws.utils.FileUtil;
import code.dws.utils.Utilities;
import code.dws.wordnet.SimilatityWebService;

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

	private static List<Pair<String, String>> revbProps = null;

	private static Map<String, THashSet<ImmutablePair<String, String>>> ALL_PROPS = new HashMap<String, THashSet<ImmutablePair<String, String>>>();

	/**
     * 
     */
	public ReverbClusterProperty() {
		//
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		Constants.loadConfigParameters(new String[] { "", args[0] });

		logger.info("Running " + ReverbClusterProperty.class.getName());

		// retrieve a set of OIE properties
		// revbProps = PropertyGoldStandard.getReverbProperties(
		// Constants.OIE_DATA_PATH, -1,
		// Long.parseLong(Constants.INSTANCE_THRESHOLD));

		// only f+ properties
		DBWrapper.init(Constants.GET_FULLY_MAPPED_OIE_PROPS_SQL);
		// revbProps = DBWrapper.getFullyMappedFacts();

		revbProps = FileUtil.readPairsOfProperties(args[1], DELIMIT);

		logger.info("Loaded " + revbProps.size() + " OIE property pairs from "
				+ args[1]);

		// Wordnet specific init block
		// System.setProperty("wordnet.database.dir",
		// Constants.WORDNET_DICTIONARY);
		// WS4JConfiguration.getInstance().setMFS(true);

		// doBasicPatternClustering();

		// enable scoring mechanism
		doScoring();

		// dumpPropCluster();

	}

	/*
	 * A pattern based cluster
	 */

	// private static void doBasicPatternClustering() throws IOException {
	//
	// String arg1 = null;
	// String arg2 = null;
	// String newKey = null;
	// Set<String> newValues = null;
	// Set<String> props = null;
	// Set<String> currentList = null;
	//
	// try {
	//
	// // iterate the list of size n, n(n-1)/2 comparison !! :D
	// for (int outerIdx = 0; outerIdx < revbProps.size(); outerIdx++) {
	// // init list
	// props = new HashSet<String>();
	// for (int innerIdx = outerIdx + 1; innerIdx < revbProps.size();
	// innerIdx++) {
	//
	// arg1 = revbProps.get(outerIdx);
	// arg2 = revbProps.get(innerIdx);
	//
	// // apply modular clustering technique
	// // first group all properties with similar patterns (was
	// // student at is currently student at..so on)
	//
	// if (canBeAdded(arg1, arg2)) {
	// props.add(arg2);
	// }
	// }
	// // if (arg1.indexOf("married to") != -1)
	// propertBasicCluster.put(arg1, props);
	// }
	//
	// logger.info("Before pattern clustering "
	// + propertBasicCluster.size());
	//
	// // collate the clusters
	// for (Map.Entry<String, Set<String>> e : propertBasicCluster
	// .entrySet()) {
	//
	// currentList = e.getValue();
	//
	// newKey = e.getKey();
	// for (String s : currentList) {
	// if (s.length() < newKey.length())
	// newKey = s;
	// }
	// if (!newKey.equals(e.getKey())) {
	// currentList.remove(newKey);
	// currentList.add(e.getKey());
	//
	// newValues = propertBasicCluster.get(newKey);
	// currentList.addAll(newValues);
	// propertBasicCluster.remove(e.getKey());
	// propertBasicCluster.put(newKey, currentList);
	// }
	// }
	//
	// // at this point there is a basic pattern based cluster already in
	// // place..improve more with markov cluster.
	// for (Map.Entry<String, Set<String>> e : propertBasicCluster
	// .entrySet()) {
	// if (e.getValue().size() == 0) {
	// propertBasicCluster.remove(e.getKey());
	// } else {
	// BASIC_CLUSTER_DICT.put(e.getKey(), e.getKey());
	// for (String elem : e.getValue()) {
	// BASIC_CLUSTER_DICT.put(elem, e.getKey());
	// }
	// }
	// }
	// logger.info("After pattern clustering "
	// + propertBasicCluster.size());
	//
	// } catch (Exception e) {
	// logger.error(e.getMessage());
	// }
	// }

	/**
	 * read the basic cluster pattern and try to improve the scoring
	 * 
	 * @throws IOException
	 */
	private static void doScoring() throws IOException {
		String arg1 = null;
		String arg2 = null;
		PairDto resultPair = null;

		long cnt = 0;
		long starTime = 0;
		long cntr = 0;

		BufferedWriter writerRevWordnetSims = new BufferedWriter(
				new FileWriter(new File(Constants.OIE_DATA_PATH).getParent()
						+ "/all.trvb.wordnet.sim."
						+ Constants.SIMILARITY_FACTOR + ".csv"));

		long start = Utilities.startTimer();

		int cores = Runtime.getRuntime().availableProcessors();
		cores = (cores > Constants.THREAD_MAX_POOL_SIZE) ? cores
				: Constants.THREAD_MAX_POOL_SIZE;

		ExecutorService executorPool = Executors.newFixedThreadPool(cores);
		ExecutorCompletionService<PairDto> completionService = new ExecutorCompletionService<PairDto>(
				executorPool);

		// init task list
		List<Future<PairDto>> taskList = new ArrayList<Future<PairDto>>();

		try {

			for (Pair<String, String> pair : revbProps) {
				cnt++;

				arg1 = pair.getLeft();
				arg2 = pair.getRight();

				// add to the pool of tasks
				taskList.add(completionService.submit(new Worker(arg1, arg2)));

			}
			// shutdown pool thread
			executorPool.shutdown();

			logger.info("Pushed " + taskList.size() + " tasks to the pool ");
			starTime = System.currentTimeMillis();

			while (!executorPool.isTerminated()) {
				try {
					cntr++;
					Future<PairDto> futureTask = completionService.poll(
							Constants.TIMEOUT_MINS, TimeUnit.MINUTES);

					resultPair = futureTask.get();

					// write it out
					if (resultPair.getScore() > 0)
						writerRevWordnetSims.write(resultPair.getArg1()
								+ "\t"
								+ resultPair.getArg2()
								+ "\t"
								+ Constants.formatter.format(resultPair
										.getScore()) + "\n");

					if (cntr % 1000 == 0 && cntr > 1000) {
						Utilities.endTimer(start, 100
								* ((double) cntr / taskList.size())
								+ " percent done in ");
						writerRevWordnetSims.flush();
					}

				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			writerRevWordnetSims.close();
		}
	}

	// private static double getTokenScore(String[] split, String[] split2) {
	//
	// int num = 0;
	// int denom = split.length + split2.length;
	// for (String s1 : split) {
	// for (String s2 : split2) {
	// if (s1.equals(s2))
	// num++;
	// }
	// }
	// return (double) 2 * num / denom;
	// }

	// private static boolean isPatternClustered(String arg1, String arg2) {
	//
	// if (BASIC_CLUSTER_DICT.get(arg2) != null
	// && BASIC_CLUSTER_DICT.get(arg1) != null) {
	// if (arg1.equals(BASIC_CLUSTER_DICT.get(arg2))
	// || BASIC_CLUSTER_DICT.get(arg1).equals(
	// BASIC_CLUSTER_DICT.get(arg2))) {
	// logger.info("Skipping " + arg1 + "\t" + arg2);
	// return true;
	// } else
	// return false;
	// } else
	// return false;
	// }

	// private static boolean canBeAdded(String arg1, String arg2) {
	// boolean flag1 = false;
	// boolean flag2 = false;
	//
	// String regex = "\\b" + arg2 + "\\b";
	// Pattern pattern = Pattern.compile(regex);
	// Matcher matcher = pattern.matcher(arg1);
	//
	// if (matcher.find()) {
	// flag1 = true;
	// }
	//
	// regex = "\\b" + arg1 + "\\b";
	// pattern = Pattern.compile(regex);
	// matcher = pattern.matcher(arg2);
	//
	// if (matcher.find()) {
	// flag2 = true;
	// }
	//
	// return flag1 || flag2;
	// }

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
		List<String> ret = new ArrayList<String>();
		THashSet<ImmutablePair<String, String>> list = null;
		THashMap<String, Long> COUNT_PROPERTY_INST = new THashMap<String, Long>();

		try {
			@SuppressWarnings("resource")
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
			@SuppressWarnings("resource")
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
	// private static double getInstanceOverlapSimilarityScores(int outerIdx,
	// int innerIdx) throws Exception {
	//
	// String propArg1 = revbProps.get(outerIdx);
	// String propArg2 = revbProps.get(innerIdx);
	//
	// THashSet<ImmutablePair<String, String>> revbSubObj1 = ALL_PROPS
	// .get(propArg1);
	//
	// THashSet<ImmutablePair<String, String>> revbSubObj2 = ALL_PROPS
	// .get(propArg2);
	//
	// long min = Math.min(revbSubObj1.size(), revbSubObj2.size());
	//
	// double scoreOverlap = 0;
	//
	// // scoreOverlap = (double) CollectionUtils.intersection(revbSubObj1,
	// // revbSubObj2)
	// // .size() / min;
	//
	// scoreOverlap = (double) Sets.intersection(revbSubObj1, revbSubObj2)
	// .size() / min;
	//
	// if (scoreOverlap > 0.002
	// && min >= Long.parseLong(Constants.INSTANCE_THRESHOLD))
	// return scoreOverlap;
	//
	// return 0;
	// }

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
	// private static double getWordNetSimilarityScores(int id, int id2)
	// throws Exception {
	//
	// double score = SimilatityWebService.getSimScore(revbProps.get(id),
	// revbProps.get(id2));
	//
	// return score;
	//
	// }

	/**
	 * write out the clusters to a file
	 * 
	 * @throws IOException
	 */
	// private static void dumpPropCluster() throws IOException {
	// BufferedWriter outputWriter = null;
	// try {
	// outputWriter = new BufferedWriter(new FileWriter(CLUSTERS));
	// for (Entry<Pair<String, String>, Map<String, Double>> e : MAP_CLUSTER
	// .entrySet()) {
	//
	// for (Entry<String, Double> propEntry : e.getValue().entrySet()) {
	//
	// outputWriter.write(propEntry.getValue() + "\t"
	// + e.getKey().getLeft() + "\t"
	// + e.getKey().getRight() + "\t" + propEntry.getKey()
	// + "\n");
	// }
	// outputWriter.flush();
	// }
	//
	// } catch (IOException e) {
	// e.printStackTrace();
	// } finally {
	// outputWriter.close();
	// }
	//
	// }

}
