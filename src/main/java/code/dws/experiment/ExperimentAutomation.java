/**
 * 
 */

package code.dws.experiment;

import java.io.IOException;

import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import code.dws.core.AutomatedNodeScoringWrapper;
import code.dws.markovLogic.EvidenceBuilder;
import code.dws.markovLogic.YagoDbpediaMapping;
import code.dws.utils.Constants;

/**
 * INCEPTION POINT !!
 * 
 * @author Arnab Dutta
 */
public class ExperimentAutomation {

	/**
	 * logger
	 */
	public final static Logger logger = LoggerFactory
			.getLogger(Constants.class);

	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {

		logger.info("\n\n =========" + args[0] + "============= \n ");

		Constants.loadConfigParameters(args);
		Constants.BOOTSTRAP = false;
		runAll();
	}

	/**
	 * @param prop
	 * @throws IOException
	 * @throws OWLOntologyCreationException
	 * @throws Exception
	 */
	private static void runAll() throws IOException,
			OWLOntologyCreationException, Exception {

		// initiate yago info
		if (Constants.INCLUDE_YAGO_TYPES)
			YagoDbpediaMapping.main(new String[] { "" });

		EvidenceBuilder.main(new String[] { Constants.PREDICATE });

		AutomatedNodeScoringWrapper.main(new String[] { Constants.PREDICATE });
	}
}
