/**
 * 
 */
package code.dws.reverb.clustering;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * define a transition matrix of all the connected nodes with edge weights
 * perform a markov chain simulation of the state jumps till convergence
 * 
 * @author adutta
 * 
 */
public class MarkovClustering {

	/**
	 * cluster collection
	 */
	static Map<String, List<String>> CLUSTER = new HashMap<String, List<String>>();

	/**
	 * mcl output file
	 */
	private static final String OUTPUT = "src/main/resources/input/mcl.output";
	// private static final String OUTPUT =
	// "/home/adutta/Work/mcl/mcl-14-137/output/";

	private static final String OUTPUT_TEMP = "src/main/resources/input/mcl.output.temp";
	// private static final String OUTPUT_TEMP =
	// "/home/adutta/Work/mcl/mcl-14-137/output/";

	public static Map<Pair<String, String>, Double> PAIR_SCORE_MAP = new HashMap<Pair<String, String>, Double>();

	private static int cnt = 1;
	private static int lastSize = 0;

	/**
	 * 
	 */
	public MarkovClustering() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @return the cLUSTER
	 */
	public static Map<String, List<String>> getAllClusters() {
		return CLUSTER;
	}

	static DecimalFormat df = new DecimalFormat("##.##");

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		String inflation = null;
		String pairScoreFile = null;

		if (args.length == 2) {
			pairScoreFile = args[0];
			inflation = args[1];
		} else {
			pairScoreFile = KMediodCluster.ALL_SCORES;
			inflation = args[1];
		}

		cnt = 1;
		loadAllPairwiseScores(pairScoreFile);

		// make mcl call to perform clustering
		systemRoutine(inflation, pairScoreFile, OUTPUT);

		CLUSTER.clear();
		// read the output to load in memory
		readMarkovClusters(inflation, OUTPUT);// + "mcl." + inflation +
												// ".output");

	}

	@SuppressWarnings("resource")
	private static void readMarkovClusters(String inflation, String output)
			throws IOException {

		Scanner scan;
		scan = new Scanner(new File((output)), "UTF-8");

		List<String> list = null;

		String sCurrentLine = null;
		String[] elem = null;

		double infl = Double.parseDouble(inflation);
		while (scan.hasNextLine()) {
			list = new ArrayList<String>();
			sCurrentLine = scan.nextLine();
			elem = sCurrentLine.split("\t");
			for (String s : elem)
				list.add(s);

			if (list.size() > 10 && lastSize != 5) {
				lastSize++;
				reCluster(list, cnt, inflation);
			} else {
				lastSize = 0;
				CLUSTER.put("C" + cnt++, list);
			}

			// for cluster sizes larger than threshold, re cluster them

		}
		//
		// System.out
		// .println("\nLoaded " + CLUSTER.size() + " markov clusters...");
	}

	private static void reCluster(List<String> list, int cnt, String inflation)
			throws IOException {
		double val = 0;
		Pair<String, String> pair = null;

		BufferedWriter writer = new BufferedWriter(new FileWriter(
				"/home/adutta/git/OIE-Integration/COMBINED_SCORE_TEMP.tsv"));

		for (int i = 0; i < list.size(); i++) {
			for (int j = i+1; j < list.size(); j++) {
				pair = new ImmutablePair<String, String>(list.get(i).trim(),
						list.get(j).trim());

				try {
					val = PAIR_SCORE_MAP.get(pair);
				} catch (Exception e) {
					try {
						pair = new ImmutablePair<String, String>(list.get(j)
								.trim(), list.get(i).trim());
						val = PAIR_SCORE_MAP.get(pair);
					} catch (Exception e1) {
						val = 0;
					}
				}

				writer.write(pair.getLeft() + "\t" + pair.getRight() + "\t"
						+ val + "\n");
			}

			writer.flush();
		}
		writer.close();

		// make mcl call to perform clustering
		systemRoutine(inflation,
				"/home/adutta/git/OIE-Integration/COMBINED_SCORE_TEMP.tsv",
				OUTPUT_TEMP);// + "mcl.output.temp");

		// read the output to load in memory
		readMarkovClusters(inflation, OUTPUT_TEMP);// + "mcl.output.temp");

	}

	private static void systemRoutine(String inflation, String scoreFile,
			String output) {
		Runtime r = Runtime.getRuntime();

		try {

			Process p = r.exec("/home/adutta/Work/mcl/mcl-14-137/bin/mcl "
					+ scoreFile + " --abc -I " + inflation + " -o " + output);

			BufferedReader bufferedreader = new BufferedReader(
					new InputStreamReader(new BufferedInputStream(
							p.getInputStream())));

			try {
				if (p.waitFor() != 0)
					System.err.println("exit value = " + p.exitValue());

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				bufferedreader.close();
			}

		} catch (Exception e) {
			System.err.println(e.getMessage());
		}

	}

	/**
	 * load the pairwise values in memory
	 * 
	 * @param allScoreFile
	 */
	public static void loadAllPairwiseScores(String allScoreFile) {
		String elem = null;
		String[] elems = null;

		Scanner scan;
		try {
			scan = new Scanner(new File((allScoreFile)), "UTF-8");

			Pair<String, String> pair = null;
			while (scan.hasNextLine()) {
				elem = scan.nextLine();
				elems = elem.split("\t");
				pair = new ImmutablePair<String, String>(elems[0].trim(),
						elems[1].trim());
				PAIR_SCORE_MAP.put(pair, Double.valueOf(elems[2]));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			// System.out.println(map.size());
		}
	}
}
