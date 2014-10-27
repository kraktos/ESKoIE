/**
 * 
 */
package code.dws.core.cluster;

import java.util.concurrent.Callable;

import code.dws.utils.Utilities;
import code.dws.wordnet.SimilatityWebService;

/**
 * @author arnab
 */
public class Worker implements Callable<PairDto> {

	private String arg1;

	private String arg2;

	public Worker(String arg1, String arg2) {
		this.arg1 = arg1;
		this.arg2 = arg2;
	}

	@Override
	public PairDto call() throws Exception {

		double score = SimilatityWebService.getSimScore(
				Utilities.splitAtCapitals(this.arg1),
				Utilities.splitAtCapitals(this.arg2));
		return new PairDto(this.arg1, this.arg2, score);
	}
}
