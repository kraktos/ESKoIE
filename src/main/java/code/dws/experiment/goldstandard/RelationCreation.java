/**
 * 
 */
package code.dws.experiment.goldstandard;

import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Logger;

import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;

/**
 * Distant supervision based
 * 
 * @author adutta
 */
public class RelationCreation
{

    // define Logger
    public static Logger logger = Logger.getLogger(RelationCreation.class.getName());

    /**
	 * 
	 */
    public RelationCreation()
    {

    }

    public static void main(String[] args)
    {
        String randomKBProp = null;

        List<String> dbpProps = null;
        Pair<String, String> randomKBInst = null;

        // number of KB properties interested in, -1 is all
        long topKKBProperties = -1;

        // get the KB object properties
        // feed based
        dbpProps = SPARQLEndPointQueryAPI.loadDbpediaProperties(topKKBProperties, Constants.QUERY_OBJECTTYPE);

        Random randomizer = new Random();
        Random offsetGen = new Random();

        long i = 0;
        // iterate the KB properties and find a prop instance prop(sub, obj), randomly
        // repeat for a long time
        while (i++ != 1000000) {
            randomKBProp = dbpProps.get(randomizer.nextInt(dbpProps.size()));

            int randomNum = offsetGen.nextInt((39999 - 1) + 1) + 1;

            // find a random instance with this random KB property
            randomKBInst = SPARQLEndPointQueryAPI.getRandomInstance(randomKBProp, randomNum);

            if (randomKBInst != null)
                logger.info(randomKBInst.getLeft() + "\t" + randomKBProp + "\t" + randomKBInst.getRight());
        }

        // get the ReVerb facts

        // get raw triples

        // scan and match
    }
}
