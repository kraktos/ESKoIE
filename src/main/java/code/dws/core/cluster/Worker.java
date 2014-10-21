/**
 * 
 */
package code.dws.core.cluster;

import java.util.concurrent.Callable;

import code.dws.wordnet.SimilatityWebService;

/**
 * @author arnab
 */
public class Worker implements Callable<Double> {

	private String arg1;

	private String arg2;

	private Double score;

	public Worker(String arg1, String arg2) {
		this.arg1 = arg1;
		this.arg2 = arg2;
	}

	@Override
	public Double call() throws Exception {

		this.score = SimilatityWebService.getSimScore(arg1, arg2);
		return this.score;
	}
}
