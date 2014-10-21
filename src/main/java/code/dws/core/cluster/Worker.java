/**
 * 
 */
package code.dws.core.cluster;

import java.io.BufferedWriter;
import java.util.concurrent.Callable;

import code.dws.wordnet.SimilatityWebService;
import code.dws.wordnet.WordNetAPI;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.ws4j.RelatednessCalculator;

/**
 * @author arnab
 */
public class Worker implements Callable<Double> {

	private String arg1;

	private String arg2;

	public Worker(String arg1, String arg2) {
		this.arg1 = arg1;
		this.arg2 = arg2;
	}

	@Override
	public Double call() throws Exception {

		return SimilatityWebService.getSimScore(arg1, arg2);
	}
}
