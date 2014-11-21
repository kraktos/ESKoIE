package code.dws.core.cluster;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import code.dws.dbConnectivity.DBWrapper;
import code.dws.query.SPARQLEndPointQueryAPI;
import code.dws.utils.Constants;

public class PairSplitter
{

    // define Logger
    public static Logger logger = Logger.getLogger(PairSplitter.class.getName());

    private static List<String> revbProps = null;

    public static void main(String[] args)
    {

        Constants.loadConfigParameters(new String[] {"", args[0]});

        int cnt = 1;
        int node = 5;
        String arg1 = null;
        String arg2 = null;

        BufferedWriter pairNode1 = null;

        // only f+ properties
        DBWrapper.init(Constants.GET_FULLY_MAPPED_OIE_PROPS_SQL);
        revbProps = DBWrapper.getFullyMappedFacts();

        logger.info("Loaded " + revbProps.size() + " OIE properties");

        List<String> dbpProps = null;

        // call to retrieve DBPedia owl object property
        dbpProps = SPARQLEndPointQueryAPI.loadDbpediaProperties(-1, Constants.QUERY_OBJECTTYPE);

        logger.info("Loaded " + dbpProps.size() + " DBpedia properties");

        // revbProps.addAll(dbpProps);
        try {

            pairNode1 =
                new BufferedWriter(new FileWriter(new File(Constants.OIE_DATA_PATH).getParent() + "/pairNodeAll.csv"));

            for (int outerIdx = 0; outerIdx < revbProps.size(); outerIdx++) {

                arg1 = revbProps.get(outerIdx);

                for (int innerIdx = 0; innerIdx < dbpProps.size(); innerIdx++) {

                    arg2 = dbpProps.get(innerIdx);

                    pairNode1.write(arg1 + "\t" + arg2 + "\n");

                    cnt++;
                }

                pairNode1.flush();
            }

        } catch (IOException e) {

        } finally {
            try {
                if (pairNode1 != null)
                    pairNode1.close();

                // pairNode2.close();
                // pairNode3.close();
                //
                // pairNode4.close();
                // pairNode5.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }

    }
}
