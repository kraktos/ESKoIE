/**
 * 
 */
package code.dws.reverb.clustering;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import code.dws.reverb.ReverbClusterProperty;
import code.dws.reverb.clustering.analysis.CompareClusters;
import code.dws.utils.Constants;

/**
 * define K seed of random properties and start clusterting. Idea is a place a
 * property nearest to one of the K seed properties
 * 
 * @author adutta
 * 
 */
public class KMediodCluster {

	private static final String WORDNET_SCORES = "src/main/resources/input/CLUSTERS_WORDNET_";
	private static final String JACCARD_SCORES = "src/main/resources/input/CLUSTERS_OVERLAP_";

	private static final int TOPK_REVERB_PROPERTIES = 500;

	public static final String ALL_SCORES = "src/main/resources/input/COMBINED_SCORE.tsv";

	// private static int SEED_CLUSTERS = (int) (0.2 * TOPK_REVERB_PROPERTIES);

	private static List<String> reverbProperties = null;
	private static List<String> seedReverbProperties = null;

	private static Map<Pair<String, String>, Double> SCORE_MAP = new HashMap<Pair<String, String>, Double>();

	// cluster placeholder
	private static Map<String, List<String>> K_CLUSTER_MAP = new HashMap<String, List<String>>();
	private static Map<String, List<String>> K_CLUSTER_MAP2 = new HashMap<String, List<String>>();

	/**
	 * @throws FileNotFoundException
	 * 
	 */
	public static void loadScores() throws FileNotFoundException {
		// feed seedc count and generate K-random cluster points
		// seedReverbProperties = generateKRandomSeed();

		// load the scores in memeory
		loadScores(WORDNET_SCORES + TOPK_REVERB_PROPERTIES, "sameAsPropWNConf");
		loadScores(JACCARD_SCORES + TOPK_REVERB_PROPERTIES, "sameAsPropJacConf");
	}

	/**
	 * @return the k_CLUSTER_MAP
	 */
	public static Map<String, List<String>> getAllClusters() {
		int cnt = 1;
		for (Entry<String, List<String>> e : K_CLUSTER_MAP.entrySet()) {
			K_CLUSTER_MAP2.put("C" + cnt++, e.getValue());
		}
		return K_CLUSTER_MAP2;
	}

	/**
	 * @return the reverbProperties
	 */
	public static List<String> getReverbProperties() {
		return reverbProperties;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		int SEED_CLUSTERS = 0;
		if (args.length > 0)
			SEED_CLUSTERS = Integer.parseInt(args[0]);

		// load the two+ pair-wise scoring files
		loadScores();

		// combine the scores to a single normalized value
		ioRoutine();
		System.out.println("Dumped combined scores to " + ALL_SCORES);

		// perform clustering with the K-seed properties
		doKClustering(SEED_CLUSTERS);

		// write out the clusters
		// printCluster();
	}

	/**
	 * a sub routine to dump the values of the pair wise scores to a file.
	 * required for clustering
	 * 
	 * @throws IOException
	 */
	public static void ioRoutine() throws IOException {
		BufferedWriter ioWriter = new BufferedWriter(new FileWriter(ALL_SCORES));
		for (Entry<Pair<String, String>, Double> e : SCORE_MAP.entrySet()) {
			if (e.getValue() > CompareClusters.SIM_SCORE_THRESHOLD)
				ioWriter.write(e.getKey().getLeft() + "\t"
						+ e.getKey().getRight() + "\t"
						+ Constants.formatter.format(e.getValue()) + "\n");
		}

		ioWriter.flush();
		ioWriter.close();
	}

	/**
	 * @return the sCORE_MAP
	 */
	public static Map<Pair<String, String>, Double> getScoreMap() {

		if (SCORE_MAP.size() == 0)
			return MarkovClustering.PAIR_SCORE_MAP;
		return SCORE_MAP;
	}

	/**
	 * print out all the clusters
	 */
	private static void printCluster() {

		int k = 1;

		for (Entry<String, List<String>> e : K_CLUSTER_MAP.entrySet()) {
			System.out.println(" \n************  Cluster " + k++
					+ " **************** ");
			System.out.println("\t" + e.getKey());
			for (String val : e.getValue()) {
				System.out.println("\t" + val);
			}
		}
	}

	/**
	 * load in memory all the scores, Wordnet similarity and jaccard ones
	 * 
	 * @param file
	 * @param arg
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("resource")
	private static void loadScores(String file, String arg)
			throws FileNotFoundException {

		String sCurrentLine;
		double score;

		Scanner scan;
		scan = new Scanner(new File(file), "UTF-8");

		Pair<String, String> pair = null;

		while (scan.hasNextLine()) {
			sCurrentLine = scan.nextLine();
			sCurrentLine = sCurrentLine.replaceAll("" + arg + "\\(", "")
					.replaceAll("\\)", "").replaceAll("\"", "");

			pair = new ImmutablePair<String, String>(
					sCurrentLine.split(", ")[0].trim(),
					sCurrentLine.split(", ")[1].trim());

			if (SCORE_MAP.containsKey(pair)) {
				score = (double) (SCORE_MAP.get(pair) + Double
						.valueOf(sCurrentLine.split(", ")[2])) / 2;

			} else {
				score = Double.valueOf(sCurrentLine.split(", ")[2]);
			}

			SCORE_MAP.put(pair, score);

			// System.out.println(sCurrentLine);
		}
	}

	/**
	 * load in memory all the scores, Wordnet similarity and jaccard ones
	 * 
	 * @param file
	 * @param arg
	 * @param delimit
	 * @throws FileNotFoundException
	 */
	@SuppressWarnings("resource")
	public static void loadScores(String file, String arg, String delimit)
			throws FileNotFoundException {

		String sCurrentLine;
		double score;

		Scanner scan;
		scan = new Scanner(new File(file), "UTF-8");

		Pair<String, String> pair = null;

		while (scan.hasNextLine()) {
			sCurrentLine = scan.nextLine();
			sCurrentLine = sCurrentLine.replaceAll("" + arg + "\\(", "")
					.replaceAll("\\)", "").replaceAll("\"", "");

			pair = new ImmutablePair<String, String>(
					sCurrentLine.split(delimit)[0].trim(),
					sCurrentLine.split(delimit)[1].trim());

			if (SCORE_MAP.containsKey(pair)) {
				score = (double) (SCORE_MAP.get(pair) + Double
						.valueOf(sCurrentLine.split(delimit)[2])) / 2;

			} else {
				score = Double.valueOf(sCurrentLine.split(delimit)[2]);
			}

			SCORE_MAP.put(pair, score);
		}
	}

	/**
	 * actual clustering algo
	 * 
	 * @param seedReverbProperties
	 */
	public static void doKClustering(int kSeeds) {

		double bestScore;
		String bestSeedProp = null;

		double score = 0;

		// feed seedc count and generate K-random cluster points
		seedReverbProperties = generateKRandomSeed(kSeeds);

		// iterate the full list of properties
		for (String reverbProp : reverbProperties) {
			bestScore = 0;
			// compare with each of the seed points
			for (String seedProp : seedReverbProperties) {
				if (!reverbProp.equals(seedProp)) {

					try {
						score = SCORE_MAP
								.get(new ImmutablePair<String, String>(
										reverbProp.trim(), seedProp.trim()));
					} catch (NullPointerException e) {
						try {
							score = SCORE_MAP
									.get(new ImmutablePair<String, String>(
											seedProp.trim(), reverbProp.trim()));
						} catch (Exception e1) {
							System.out.println("problem with " + seedProp
									+ ", " + reverbProp);
						}
					}

					// filter the top score
					if (bestScore <= score) {
						bestScore = score;
						bestSeedProp = seedProp;
					}
				}
			}

			// at this point, one prop is checked against all seed
			// properties..so
			// we can place this in the cluster collection
			putInCluster(bestSeedProp, reverbProp);
		}

		System.out.println("Loaded " + K_CLUSTER_MAP.size()
				+ " mediod clusters...");

	}

	/**
	 * once a property is checked with its closest seed property, place it in
	 * that cluster
	 * 
	 * @param keySeedProp
	 * @param valueProperty
	 */
	private static void putInCluster(String keySeedProp, String valueProperty) {

		List<String> exixtingClusterMemebers = K_CLUSTER_MAP.get(keySeedProp);
		try {
			exixtingClusterMemebers.add(valueProperty);
		} catch (Exception e) {
			System.out.println("Problem while adding for the cluster "
					+ keySeedProp);
		}

		K_CLUSTER_MAP.put(keySeedProp, exixtingClusterMemebers);

	}

	/**
	 * populae full list of properties
	 * 
	 * @param seedCluster
	 * 
	 * @param seed2
	 * @return
	 */
	private static List<String> generateKRandomSeed(int seedCluster) {

		ReverbClusterProperty.TOPK_REV_PROPS = TOPK_REVERB_PROPERTIES;
		// call DBand retrieve a set of TOPK properties
		reverbProperties = ReverbClusterProperty.getReverbProperties(
				Constants.REVERB_DATA_PATH,
				ReverbClusterProperty.TOPK_REV_PROPS);

		List<String> temp = getRandomProps((seedCluster < TOPK_REVERB_PROPERTIES) ? seedCluster
				: TOPK_REVERB_PROPERTIES);

		K_CLUSTER_MAP.clear();
		for (String p : temp) {
			K_CLUSTER_MAP.put(p, new ArrayList<String>());
		}
		return temp;

	}

	/**
	 * random K properties
	 * 
	 * @param revbProps
	 * @param seedK
	 * @return
	 */
	private static List<String> getRandomProps(int seedK) {

		List<String> temp = new LinkedList<String>(reverbProperties);
		Collections.shuffle(temp);
		List<String> seeedList = temp.subList(0, seedK);
		// for (String k : seeedList) {
		// reverbProperties.remove(k);
		// }
		return seeedList;
	}

}
