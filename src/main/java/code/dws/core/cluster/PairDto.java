/**
 * 
 */
package code.dws.core.cluster;

/**
 * @author adutta
 */
public class PairDto
{
    private String arg1;

    private String rel;

    private String arg2;

    private String kbArg1;

    private String kbArg2;

    private Double score;

    /**
     * @param arg1
     * @param rel
     * @param arg2
     * @param kbArg1
     * @param kbArg2
     */
    public PairDto(String arg1, String rel, String arg2, String kbArg1, String kbArg2)
    {
        this.arg1 = arg1;
        this.rel = rel;
        this.arg2 = arg2;
        this.kbArg1 = kbArg1;
        this.kbArg2 = kbArg2;
    }

    /**
     * @param arg1
     * @param rel
     * @param arg2
     */
    public PairDto(String arg1, String rel, String arg2)
    {
        this.arg1 = arg1;
        this.rel = rel;
        this.arg2 = arg2;
    }

    public PairDto(String arg1, String arg2, Double score)
    {
        super();
        this.arg1 = arg1;
        this.arg2 = arg2;
        this.score = score;
    }

    /**
     * @return the kbArg1
     */
    public String getKbArg1()
    {
        return kbArg1;
    }

    /**
     * @return the kbArg2
     */
    public String getKbArg2()
    {
        return kbArg2;
    }

    public String getArg1()
    {
        return arg1;
    }

    public String getArg2()
    {
        return arg2;
    }

    public Double getScore()
    {
        return score;
    }

    /**
     * @return the rel
     */
    public String getRel()
    {
        return rel;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("PairDto [");
        if (arg1 != null) {
            builder.append("arg1=");
            builder.append(arg1);
            builder.append(", ");
        }
        if (rel != null) {
            builder.append("rel=");
            builder.append(rel);
            builder.append(", ");
        }
        if (arg2 != null) {
            builder.append("arg2=");
            builder.append(arg2);
            builder.append(", ");
        }
        if (kbArg1 != null) {
            builder.append("kbArg1=");
            builder.append(kbArg1);
            builder.append(", ");
        }
        if (kbArg2 != null) {
            builder.append("kbArg2=");
            builder.append(kbArg2);
            builder.append(", ");
        }
        if (score != null) {
            builder.append("score=");
            builder.append(score);
        }
        builder.append("]");
        return builder.toString();
    }

}
