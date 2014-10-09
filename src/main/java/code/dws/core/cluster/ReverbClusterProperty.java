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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.experiment.PropertyGoldStandard;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;
import code.dws.wordnet.SimilatityWebService;

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

		// enable scoring mechanism
		doScoring(revbProps);

		// dumpPropCluster();

	}

	private static void doScoring(List<String> properties) throws IOException {

		long cnt = 0;
		double ovlapScore = 0;
		double wnScore = 0;

		BufferedWriter writerRevWordnetSims = new BufferedWriter(
				new FileWriter(new File(Constants.OIE_DATA_PATH).getParent()
						+ "/all.trvb.wordnet.sim.csv"));

		BufferedWriter writerRevOverlapSims = new BufferedWriter(
				new FileWriter(new File(Constants.OIE_DATA_PATH).getParent()
						+ "/all.trvb.overlap.sim.csv"));

		// BufferedWriter writerRevDbpWordnetSims = new BufferedWriter(
		// new FileWriter(new File(Constants.REVERB_DATA_PATH).getParent()
		// + "/all.trvb.dbp.wordnet.sim.csv"));

		long s = properties.size();

		try {
			System.setProperty("wordnet.database.dir",
					"/home/adutta/WordNet-3.0/dict/");
			WS4JConfiguration.getInstance().setMFS(true);

			ILexicalDatabase db = new NictWordNet();

			RelatednessCalculator[] rcs = { new WuPalmer(db),
					new LeacockChodorow(db), new Resnik(db),
					new JiangConrath(db), new Lin(db), new Path(db) };

			long start = System.currentTimeMillis();
			// iterate the list of size n, n(n-1)/2 comparison !! :D
			for (int outerIdx = 0; outerIdx < properties.size(); outerIdx++) {
				for (int innerIdx = outerIdx + 1; innerIdx < properties.size(); innerIdx++) {
					cnt++;

					

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
					// revbProps.get(outerIdx).split(" "), revbProps
					// .get(innerIdx).split(" "));
					//
					// if (wnScore > 0)
					// writerRevWordnetSims.write(revbProps.get(outerIdx)
					// + "\t" + revbProps.get(innerIdx) + "\t"
					// + wnScore + "\n");
					// }
					//
					writerRevOverlapSims.flush();
					writerRevWordnetSims.flush();
				}

				long s2 = s * (s - 1);
				logger.info("Completed " + 200 * ((double) cnt / s2) + " %");

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

	/**
	 * get the list of Reverb properties
	 * 
	 * CAn be used to get both top-k properties, or properties with atleast x
	 * number of instances
	 * 
	 * @param OIE_FILE
	 * @param TOPK_REV_PROPS
	 * @param atLeastInstancesCount
	 * 
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
	 * 
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
	 * 
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
	 * 
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
