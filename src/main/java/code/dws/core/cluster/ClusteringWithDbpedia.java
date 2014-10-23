/**
 * 
 */
package code.dws.core.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;
import code.dws.wordnet.SimilatityWebService;

import com.hp.hpl.jena.query.QuerySolution;

/**
 * This is another way of finding new triples, Here, we do not try to find what
 * DBPedia proeprty a cluster might map to, but feed the set of DBpedia property
 * along with Reverb and cluster all together.
 * 
 * 
 * @author adutta
 * 
 */
public class ClusteringWithDbpedia {

	private static final String QUERY = "select distinct ?val where {?val <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/2002/07/owl#ObjectProperty>} ";

	/**
	 * logger
	 */
	// define Logger
	public static Logger logger = Logger.getLogger(ClusteringWithDbpedia.class
			.getName());

	static BufferedWriter writerDbpProps = null;

	static int k = -1; // ReverbClusterProperty.TOPK_REV_PROPS;

	/**
	 * initialize writers.
	 */
	private static void init() {
		try {

			writerDbpProps = new BufferedWriter(new FileWriter(new File(
					Constants.OIE_DATA_PATH).getParent()
					+ "/dbp."
					+ k
					+ ".objects.csv"));

		} catch (IOException e) {
			logger.error(e.getMessage());
		}

	}

	/**
	 * 
	 */
	public ClusteringWithDbpedia() {

	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		int cnt = 0;
		double score = 0;
		String arg1 = null;
		String arg2 = null;
		String targ1 = null;
		String targ2 = null;

		Constants.loadConfigParameters(new String[] { "", args[0] });

		init();

		List<String> dbpProps = null;

		// call to retrieve DBPedia owl object property
		dbpProps = loadDbpediaProperties(k);

		logger.info("Loaded " + dbpProps.size() + " DBpedia properties");

		logger.info("Writing sim scores to "
				+ new File(Constants.OIE_DATA_PATH).getParent()
				+ "/all.trvb.dbp." + k + ".pairwise.sim.csv");

		long start = Utilities.startTimer();
		long propSize = dbpProps.size() * (dbpProps.size() - 1);

		Collection<Pair<String, String>> identifier = null;
		List<Double> scores = null;

		int cores = Runtime.getRuntime().availableProcessors();

		try {
			for (int outer = 0; outer < dbpProps.size(); outer++) {

				// get the first operand
				targ1 = dbpProps.get(outer);
				arg1 = Utilities.splitAtCapitals(targ1);

				for (int inner = outer + 1; inner < dbpProps.size(); inner++) {

					ExecutorService executorPool = Executors
							.newFixedThreadPool(cores);
					ExecutorCompletionService<Double> completionService = new ExecutorCompletionService<Double>(
							executorPool);

					cnt++;
					// targ2 = dbpProps.get(inner);
					// arg2 = Utilities.splitAtCapitals(targ2);
					identifier = new ArrayList<Pair<String, String>>();

					List<Future<Double>> taskList = new ArrayList<Future<Double>>();
					// score = SimilatityWebService.getSimScore(arg1, arg2);

					// writerDbpProps.write(targ1 + "\t" + targ2 + "\t"
					// + Constants.formatter.format(score) + "\n");
					// writerDbpProps.flush();

					for (int c = 0; c < cores; c++) {
						inner = inner + c;

						if (inner < dbpProps.size()) {
							targ2 = dbpProps.get(inner);
							arg2 = Utilities.splitAtCapitals(dbpProps
									.get(inner));

							Future<Double> fut = completionService
									.submit(new Worker(arg1, arg2));
							taskList.add(fut);

							identifier.add(new ImmutablePair<String, String>(
									arg1, arg2));
						}
					}

					inner++;

					executorPool.shutdown();
					scores = new ArrayList<Double>();
					try {
						for (int k = 0; k < cores; k++) {

							Future<Double> future = completionService.take();
							double sc = future.get();

//							System.out.println(sc);
							scores.add(sc);
						}
						int i = 0;
						for (Pair<String, String> pair : identifier) {
							writerDbpProps.write(pair.getLeft()
									+ "\t"
									+ pair.getRight()
									+ "\t"
									+ Constants.formatter.format(scores
											.get(i++)) + "\n");
						}
						writerDbpProps.flush();

					} catch (InterruptedException e) {
						logger.error(e.getMessage());
					}

					// while (!executorPool.isTerminated()) {
					// try {
					// Future<Double> future = completionService.poll(2,
					// TimeUnit.MINUTES);
					// System.out.println(future.get());
					// scores = new ArrayList<Double>();
					// for (int k = 0; k < cores; k++) {
					// scores.add(completionService.take().get());
					// }
					//
					// int i = 0;
					// for (Pair<String, String> pair : identifier) {
					// writerDbpProps.write(pair.getLeft()
					// + "\t"
					// + pair.getRight()
					// + "\t"
					// + Constants.formatter.format(scores
					// .get(i++)) + "\n");
					// }
					// writerDbpProps.flush();
					//
					// } catch (InterruptedException e) {
					// logger.error(e.getMessage());
					// }
					// }

					// try {
					// scores = new ArrayList<Double>();
					// for (int k = 0; k < cores; k++) {
					// scores.add(completionService.take().get());
					// }
					//
					// int i = 0;
					// for (Pair<String, String> pair : identifier) {
					// writerDbpProps.write(pair.getLeft()
					// + "\t"
					// + pair.getRight()
					// + "\t"
					// + Constants.formatter.format(scores
					// .get(i++)) + "\n");
					// }
					// writerDbpProps.flush();
					// } catch (ExecutionException e) {
					// logger.error(e.getMessage());
					// }
//					if (cnt % 500 == 0 && cnt > 500)
						Utilities.endTimer(start, 200
								* ((double) cnt / propSize)
								+ " percent done in ");

				}
			}

			// executorPool.shutdown();
			// while (!executorPool.isTerminated()) {
			// }

		} catch (Exception e) {
			logger.error(e.getMessage());

		} finally {
			writerDbpProps.close();
		}

	}

	/**
	 * load DBP properties from SPARQL endpoint, -1 means all properties
	 * 
	 * @param topKDBPediaProperties
	 * 
	 * @return
	 */
	public static List<String> loadDbpediaProperties(long topKDBPediaProperties) {

		String prop = null;
		String cnt = "0";
		int c = 0;

		List<String> retS = new ArrayList<String>();

		Map<String, Long> props = new HashMap<String, Long>();

		List<QuerySolution> count = null;

		List<QuerySolution> dbpObjProps = SPARQLEndPointQueryAPI
				.queryDBPediaEndPoint(QUERY);

		for (QuerySolution querySol : dbpObjProps) {
			prop = querySol.get("val").toString();

			if ((prop.indexOf(Constants.DBPEDIA_PREDICATE_NS) != -1)
					&& (prop.indexOf("wikiPageWikiLink") == -1)
					&& (prop.indexOf("wikiPageExternalLink") == -1)
					&& (prop.indexOf("wikiPageRedirects") == -1)
					&& (prop.indexOf("thumbnail") == -1)
					&& (prop.indexOf("wikiPageDisambiguates") == -1)
					&& (prop.indexOf("wikiPageInterLanguageLink") == -1)) {

				count = SPARQLEndPointQueryAPI
						.queryDBPediaEndPoint("select (count(*)  as ?val)  where {?a <"
								+ prop + "> ?c} ");

				for (QuerySolution sol : count) {
					cnt = sol.get("val").toString();
				}
				cnt = cnt.substring(0, cnt.indexOf("^"));
				props.put(prop.replaceAll(Constants.DBPEDIA_PREDICATE_NS, ""),
						Long.parseLong(cnt));
			}
		}

		// sort only when interested in top-k, else makes no sense
		if (topKDBPediaProperties != -1)
			props = Utilities.sortByValue(props);

		for (Entry<String, Long> e : props.entrySet()) {
			retS.add(e.getKey());

			c++;
			if (c == topKDBPediaProperties)
				return retS;
		}

		return retS;
	}
}
