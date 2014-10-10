/**
 * 
 */
package code.dws.core.cluster;

import java.util.concurrent.Callable;

import code.dws.wordnet.WordNetAPI;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.ws4j.RelatednessCalculator;

/**
 * @author arnab
 */
public class Worker implements Callable<Double>
{

    private String arg1;

    private String arg2;

    private Double score;

    private ILexicalDatabase db;

    private RelatednessCalculator[] rcs;

    /**
     * @param db
     * @param rcs
     * @param string2
     * @param string
     */
    public Worker(ILexicalDatabase db, RelatednessCalculator[] rcs, String arg1, String arg2)
    {
        this.db = db;
        this.rcs = rcs;
        this.arg1 = arg1;
        this.arg2 = arg2;

    }

    @Override
    public Double call() throws Exception
    {
        this.score = WordNetAPI.scoreWordNet(this.rcs, this.arg1.split(" "), this.arg2.split(" "));

        return this.score;
    }
}
