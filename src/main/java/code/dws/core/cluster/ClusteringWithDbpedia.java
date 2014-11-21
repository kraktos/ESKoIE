/**
 * 
 */
package code.dws.core.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

/**
 * This is another way of finding new triples, Here, we do not try to find what DBPedia proeprty a cluster might map to,
 * but feed the set of DBpedia property along with Reverb and cluster all together.
 * 
 * @author adutta
 */
public class ClusteringWithDbpedia
{

    public static String QUERY = null;

    /**
     * logger
     */
    // define Logger
    public static Logger logger = Logger.getLogger(ClusteringWithDbpedia.class.getName());

    static BufferedWriter writerDbpProps = null;

    static int k = -1; // ReverbClusterProperty.TOPK_REV_PROPS;

    /**
     * initialize writers.
     */
    private static void init()
    {
        try {
            QUERY = Constants.QUERY_DATATYPE;

            writerDbpProps =
                new BufferedWriter(new FileWriter(new File(Constants.OIE_DATA_PATH).getParent() + "/dbp." + k
                    + ".props.csv"));

        } catch (IOException e) {
            logger.error(e.getMessage());
        }

    }

    /**
	 * 
	 */
    public ClusteringWithDbpedia()
    {

    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException
    {

        String arg1 = null;
        String arg2 = null;
        String oArg1 = null;
        String oArg2 = null;
        long starTime = 0;
        long cntr = 0;
        PairDto resultPair = null;

        Constants.loadConfigParameters(new String[] {"", args[0]});

        init();

        List<String> dbpProps = null;

        // call to retrieve DBPedia owl object property
        dbpProps = SPARQLEndPointQueryAPI.loadDbpediaProperties(k, ClusteringWithDbpedia.QUERY);

        logger.info("Loaded " + dbpProps.size() + " DBpedia properties");

        logger.info("Writing sim scores to " + writerDbpProps);

        long start = Utilities.startTimer();

        int cores = Runtime.getRuntime().availableProcessors();
        cores = (cores > Constants.THREAD_MAX_POOL_SIZE) ? cores : Constants.THREAD_MAX_POOL_SIZE;

        int SIZE = dbpProps.size();

        ExecutorService executorPool = Executors.newFixedThreadPool(cores);
        ExecutorCompletionService<PairDto> completionService = new ExecutorCompletionService<PairDto>(executorPool);

        // init http connection pool
        SimilatityWebService.init();

        List<Future<PairDto>> taskList = new ArrayList<Future<PairDto>>();

        try {
            for (int outer = 0; outer < SIZE; outer++) {

                // get the first operand
                oArg1 = dbpProps.get(outer);
                if (QUERY.equals(Constants.QUERY_OBJECTTYPE))
                    arg1 = Utilities.splitAtCapitals(oArg1);
                else
                    arg1 = Utilities.splitAtCapitalsExt(oArg1);

                for (int inner = outer + 1; inner < SIZE; inner++) {

                    oArg2 = dbpProps.get(inner);
                    if (QUERY.equals(Constants.QUERY_OBJECTTYPE))
                        arg2 = Utilities.splitAtCapitals(oArg2);
                    else
                        arg2 = Utilities.splitAtCapitalsExt(oArg2);

                    // submit task to a thread
                    taskList.add(completionService.submit(new Worker(arg1, arg2, oArg1, oArg2)));

                }
            }

            executorPool.shutdown();

            logger.info("Pushed " + taskList.size() + " tasks to the pool ");
            starTime = System.currentTimeMillis();

            while (!executorPool.isTerminated()) {
                try {
                    cntr++;
                    Future<PairDto> futureTask = completionService.poll(Constants.TIMEOUT_MINS, TimeUnit.MINUTES);

                    resultPair = futureTask.get();

                    // write it out
                    writerDbpProps.write(resultPair.getArg1() + "\t" + resultPair.getArg2() + "\t"
                        + Constants.formatter.format(resultPair.getScore()) + "\n");
                    writerDbpProps.flush();

                    if (cntr % 1000 == 0 && cntr > 1000)
                        Utilities.endTimer(start, 100 * ((double) cntr / taskList.size()) + " percent done in ");

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
}
