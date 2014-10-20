/**
 * 
 */
package code.dws.core.cluster.analysis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.utils.Constants;

/**
 * @author adutta
 * 
 */
public class CompareClusters {

	// define Logger
	public static Logger logger = Logger.getLogger(CompareClusters.class
			.getName());

	/**
	 * cluster collection
	 */
	static Map<String, List<String>> CLUSTER = new HashMap<String, List<String>>();

	private static Map<Pair<String, String>, Double> SCORE_MAP = new HashMap<Pair<String, String>, Double>();

	public static String CLUSTER_INDICES = null;

	// public static final double SIM_SCORE_THRESHOLD = 0.01;

	/**
	 * 
	 */
	public CompareClusters() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		Constants.loadConfigParameters(new String[] { "", args[0] });

		CLUSTER_INDICES = "src/main/resources/KCL_MCL_CL."
				+ Constants.SIMILARITY_FACTOR;

		// load the pairwise scores of all relevant properties
		loadScores("src/main/resources/all.trvb.wordnet.sim."
				+ Constants.SIMILARITY_FACTOR + ".csv", "\t");

		logger.info("Loaded " + SCORE_MAP.size() + " pairs");
		scanAndWriteClusterScores();
	}

	private static void scanAndWriteClusterScores() throws IOException {
		double mclIndex = 0;
		int inf = 0;

		Path filePath = Paths.get("src/main/resources");

		BufferedWriter writer = new BufferedWriter(new FileWriter(
				CLUSTER_INDICES));

		writer.write("ITERATION\tCLUSTER_SIZE\tMCL_SCORE\n");

		// VISIT THE FOLDER LOCATION AND ITERATE THROUGH ALL SUB FOLDERS FOR
		// THE EXACT OUTPUT FILE
		final List<Path> files = new ArrayList<Path>();
		FileVisitor<Path> fv = new SimpleFileVisitor<Path>() {
			@Override
			public FileVisitResult visitFile(Path file,
					BasicFileAttributes attrs) throws IOException {
				if (file.toString()
						.startsWith("src/main/resources/rev.cluster")
						&& file.toString().endsWith(".out"))
					files.add(file);
				return FileVisitResult.CONTINUE;
			}
		};

		try {
			// gets the only relevant output files
			Files.walkFileTree(filePath, fv);

			// iterate the files
			for (Path path : files) {
				CLUSTER = new HashMap<String, List<String>>();
				logger.info("Currently in location " + path + " .... ");

				inf = Integer.parseInt((path.toString()
						.replaceAll("[^\\d]", "")));

				readMarkovClusters(path.toString());
				mclIndex = computeClusterIndex(CLUSTER);

				// create a map of cluster key and its highest isolation value
				logger.info("MCL Index Score = " + mclIndex);
				writer.write(inf + "\t" + CLUSTER.size() + "\t" + mclIndex
						+ "\n");
				writer.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		writer.close();
	}

	private static void loadScores(String file, String delimit)
			throws FileNotFoundException {

		String sCurrentLine;
		double score;

		Scanner scan;
		scan = new Scanner(new File(file), "UTF-8");

		Pair<String, String> pair = null;

		while (scan.hasNextLine()) {
			sCurrentLine = scan.nextLine();

			pair = new ImmutablePair<String, String>(
					sCurrentLine.split(delimit)[0].trim(),
					sCurrentLine.split(delimit)[1].trim());

			score = Double.valueOf(sCurrentLine.split(delimit)[2]);

			SCORE_MAP.put(pair, score);

		}
	}

	private static void readMarkovClusters(String output) throws IOException {

		Scanner scan;
		scan = new Scanner(new File((output)), "UTF-8");
		int cnt = 1;

		List<String> list = null;

		String sCurrentLine = null;
		String[] elem = null;

		while (scan.hasNextLine()) {
			list = new ArrayList<String>();
			sCurrentLine = scan.nextLine();
			elem = sCurrentLine.split("\t");
			for (String s : elem)
				list.add(s);

			CLUSTER.put("C" + cnt++, list);

		}
	}

	/**
	 * get a isolation score for the whole cluster set
	 * 
	 * @param mCl
	 * @param isoMcl
	 * @return
	 * @return
	 */
	public static double computeClusterIndex(Map<String, List<String>> mCl) {

		double minCompactness = 0;
		double tempIsolation = 0;
		double maxIsolation = 0;

		List<String> arg1 = null;
		List<String> arg2 = null;

		double clusterGoodness = 0;
		// long n = (mCl.size()) * (mCl.size() - 1) / 2;
		for (Entry<String, List<String>> e1 : mCl.entrySet()) {

			maxIsolation = 0;

			for (Entry<String, List<String>> e2 : mCl.entrySet()) {
				if (e2.getKey().hashCode() != e1.getKey().hashCode()) {

					arg1 = e1.getValue();
					arg2 = e2.getValue();
					tempIsolation = intraClusterScore(arg1, arg2);

					// get the maximum score, i.e the strongest intra-cluster
					// pair..
					maxIsolation = (maxIsolation < tempIsolation) ? tempIsolation
							: maxIsolation;
				}
			}

			// perform its own compactness
			minCompactness = getInterClusterScore(e1.getValue());

			clusterGoodness = clusterGoodness + (double) minCompactness
					/ ((maxIsolation == 0) ? Math.pow(10, -1) : maxIsolation);

		}

		clusterGoodness = (clusterGoodness == 0) ? (Math.pow(10, -8) - clusterGoodness)
				: clusterGoodness;

		return (double) 1 / clusterGoodness;

	}

	/**
	 * take 2 lists and find the max pairwise similarity score. this essentially
	 * gives the isolation score of two clusters represented by two lists
	 * 
	 * @param arg1
	 * @param arg2
	 * @return
	 */
	private static double intraClusterScore(List<String> arg1, List<String> arg2) {
		// compare each elements from argList1 vs argList2

		Pair<String, String> pair = null;
		double tempScore = 0;
		double maxScore = 0;

		for (int list1Id = 0; list1Id < arg1.size(); list1Id++) {
			for (int list2Id = 0; list2Id < arg2.size(); list2Id++) {

				// create a pair
				pair = new ImmutablePair<String, String>(arg1.get(list1Id)
						.trim(), arg2.get(list2Id).trim());

				try {
					// retrieve the key from the collection
					tempScore = SCORE_MAP.get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(arg2.get(
								list2Id).trim(), arg1.get(list1Id).trim());
						tempScore = SCORE_MAP.get(pair);

					} catch (Exception e1) {
						tempScore = 0;
					}
				}

				// System.out.println(" temp score = " + tempScore);
				maxScore = (tempScore > maxScore) ? tempScore : maxScore;
			}
		}

		// System.out.println("max = " + maxScore);
		return maxScore;
	}

	/**
	 * get the pairwise similarity scores for each elements in a cluster
	 * 
	 * @param cluster
	 * @return
	 */
	private static double getInterClusterScore(List<String> cluster) {

		Pair<String, String> pair = null;

		double score = 1;
		double tempScore = 0;

		// System.out.println("CL size " + cluster.size());
		if (cluster.size() <= 1)
			return 0;

		for (int outer = 0; outer < cluster.size(); outer++) {
			for (int inner = outer + 1; inner < cluster.size(); inner++) {
				// create a pair
				pair = new ImmutablePair<String, String>(cluster.get(outer)
						.trim(), cluster.get(inner).trim());

				try {
					// retrieve the key from the collection
					tempScore = SCORE_MAP.get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(cluster.get(
								inner).trim(), cluster.get(outer).trim());
						tempScore = SCORE_MAP.get(pair);

					} catch (Exception e1) {
						tempScore = 0;
					}
				}

				// for sum of all pairwise scores
				// score = score + tempScore;

				// for the minimum inter cluster score
				score = (score >= tempScore) ? tempScore : score;
			}
		}
		return score;
	}
}
