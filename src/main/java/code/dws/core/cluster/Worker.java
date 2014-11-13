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

	private String originalArg1;
	private String originalArg2;

	public Worker(String arg1, String arg2) {
		this.originalArg1 = arg1;
		this.originalArg2 = arg2;
	}

	public Worker(String arg1, String arg2, String originalArg1,
			String originalArg2) {
		super();
		this.arg1 = arg1;
		this.arg2 = arg2;
		this.originalArg1 = originalArg1;
		this.originalArg2 = originalArg2;
	}

	@Override
	public PairDto call() throws Exception {

		double score = SimilatityWebService.getSimScore(
				Utilities.splitAtCapitals(this.arg1),
				Utilities.splitAtCapitals(this.arg2));
		return new PairDto(this.originalArg1, this.originalArg2, score);
	}
}
