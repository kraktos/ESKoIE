/**
 * 
 */
package code.dws.core.cluster;

import java.io.BufferedWriter;
import java.io.File;
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

		String arg1 = null;
		String arg2 = null;
		String targ1 = null;
		String targ2 = null;
		long starTime = 0;
		long cntr = 0;
		PairDto resultPair = null;

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

		int cores = Runtime.getRuntime().availableProcessors();
		cores = (cores > Constants.THREAD_MAX_POOL_SIZE) ? cores
				: Constants.THREAD_MAX_POOL_SIZE;

		int SIZE = dbpProps.size();

		ExecutorService executorPool = Executors.newFixedThreadPool(cores);
		ExecutorCompletionService<PairDto> completionService = new ExecutorCompletionService<PairDto>(
				executorPool);

		// init http connection pool
		SimilatityWebService.init();

		List<Future<PairDto>> taskList = new ArrayList<Future<PairDto>>();

		try {
			for (int outer = 0; outer < SIZE; outer++) {

				// get the first operand
				targ1 = dbpProps.get(outer);
				arg1 = Utilities.splitAtCapitals(targ1);

				for (int inner = outer + 1; inner < SIZE; inner++) {

					targ2 = dbpProps.get(inner);
					arg2 = Utilities.splitAtCapitals(dbpProps.get(inner));

					// submit task to a thread
					taskList.add(completionService
							.submit(new Worker(arg1, arg2)));

				}
			}

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
					writerDbpProps.write(resultPair.getArg1() + "\t"
							+ resultPair.getArg2() + "\t"
							+ Constants.formatter.format(resultPair.getScore())
							+ "\n");
					writerDbpProps.flush();

					if (cntr % 1000 == 0 && cntr > 1000)
						Utilities.endTimer(start, 100
								* ((double) cntr / taskList.size())
								+ " percent done in ");

				} catch (InterruptedException e) {
					logger.error(e.getMessage());
				}
			}

			Utilities.endTimer(starTime, "Finished in ");
		} catch (Exception e) {
			logger.error(e.getMessage());

		} finally {
			writerDbpProps.close();

			// init http connection pool
			SimilatityWebService.closeDown();
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
