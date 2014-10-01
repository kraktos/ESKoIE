/**
 * 
 */
package code.dws.experiment.evaluation;

/**
 * @author adutta
 * 
 */
public class FactDao {

	private String sub;
	private String relation;
	private String obj;

	/**
	 * 
	 */
	public FactDao() {

	}

	/**
	 * @param sub
	 * @param relation
	 * @param obj
	 */
	public FactDao(String sub, String relation, String obj) {
		this.sub = sub;
		this.relation = relation;
		this.obj = obj;
	}

	/**
	 * @return the sub
	 */
	public String getSub() {
		return sub;
	}

	/**
	 * @return the relation
	 */
	public String getRelation() {
		return relation;
	}

	/**
	 * @return the obj
	 */
	public String getObj() {
		return obj;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((obj == null) ? 0 : obj.hashCode());
		result = prime * result
				+ ((relation == null) ? 0 : relation.hashCode());
		result = prime * result + ((sub == null) ? 0 : sub.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FactDao other = (FactDao) obj;
		if (this.obj == null) {
			if (other.obj != null)
				return false;
		} else if (!this.obj.equals(other.obj))
			return false;
		if (relation == null) {
			if (other.relation != null)
				return false;
		} else if (!relation.equals(other.relation))
			return false;
		if (sub == null) {
			if (other.sub != null)
				return false;
		} else if (!sub.equals(other.sub))
			return false;
		return true;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(sub).append(", ").append(relation).append(", ")
				.append(obj);
		return builder.toString();
	}

}
