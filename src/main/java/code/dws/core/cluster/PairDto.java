/**
 * 
 */
package code.dws.core.cluster;

/**
 * @author adutta
 *
 */
public class PairDto {
	private String arg1;

	private String arg2;

	private Double score;

	public PairDto(String arg1, String arg2, Double score) {
		super();
		this.arg1 = arg1;
		this.arg2 = arg2;
		this.score = score;
	}

	public String getArg1() {
		return arg1;
	}

	public String getArg2() {
		return arg2;
	}

	public Double getScore() {
		return score;
	}

	@Override
	public String toString() {
		return "PairDto [arg1=" + arg1 + ", arg2=" + arg2 + ", score=" + score
				+ "]";
	}
	
	

}
