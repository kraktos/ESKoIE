package code.dws.experiment.goldstandard;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.sf.javaml.distance.fastdtw.timeseries.PAA;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import code.dws.core.cluster.PairDto;
import code.dws.utils.Constants;

public class StreamProcessor
{
    // define Logger
    public static Logger logger = Logger.getLogger(StreamProcessor.class.getName());

    private static List<PairDto> ALL_OIE = new ArrayList<PairDto>();

    /**
     * read the changing file to fetch OIe data set
     */

    public StreamProcessor()
    {

    }

    public static void main(String[] args) throws IOException
    {
        Constants.loadConfigParameters(new String[] {"", args[0]});

        String[] arr = null;

        List<String> list = null;

        logger.info("Loaded OIE facts...");
        loadOIEInMemory();
        logger.info("Loaded " + ALL_OIE.size() + " lines of OIE facts");

        // load a file repeatedly to process the snapshot of samples
        while (true) {
            list = FileUtils.readLines(new File(KBSeeder.SEED_KB), "UTF-8");

            for (String line : list) {
                arr = line.split("\t");
                scanOIEFile(arr[0], arr[1], arr[2]);
            }
        }
    }

    /**
     * load the oie file base in memory
     */
    private static void loadOIEInMemory()
    {
        String[] arr = null;
        try {
            List<String> oieTriples = FileUtils.readLines(new File(Constants.OIE_DATA_PATH), "UTF-8");

            for (String oieTriple : oieTriples) {
                arr = oieTriple.split(";");
                ALL_OIE.add(new PairDto(arr[0], arr[1], arr[2]));
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    /**
     * routine to search the in memory OIE file for a match
     * 
     * @param sub
     * @param kbRel
     * @param obj
     * @return
     */
    private static boolean scanOIEFile(String sub, String kbRel, String obj)
    {
        if (existsInList(sub, kbRel, obj))
            return true;
        return false;
    }

    private static boolean existsInList(String sub, String kbRel, String obj)
    {
        boolean flag1;
        boolean flag2;
        boolean flag3;

        for (PairDto oieTriple : ALL_OIE) {
            flag1 = oieTriple.getArg1().toLowerCase().equals(sub.toLowerCase());
            flag3 = oieTriple.getArg2().toLowerCase().equalsIgnoreCase(obj.toLowerCase());
            if (flag1 || flag3)
                System.out.println(oieTriple.getRel() + "\t" + kbRel);

        }

        return false;
    }
};
