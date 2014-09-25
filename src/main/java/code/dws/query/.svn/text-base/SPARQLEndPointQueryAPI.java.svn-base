/**
 * 
 */

package code.dws.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import code.dws.ontology.GenericConverter;
import code.dws.utils.Constants;
import code.dws.utils.Utilities;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

/**
 * @author Arnab Dutta
 */
public class SPARQLEndPointQueryAPI
{

    Logger logger = Logger.getLogger(SPARQLEndPointQueryAPI.class.getName());

    // map of class and its subclasses
    private static Map<String, Set<String>> classAndSubClassesMap = new HashMap<String, Set<String>>();

    public static void queryDBPedia(final String QUERY)
    {

        Logger logger = Logger.getLogger(SPARQLEndPointQueryAPI.class.getName());

        String sparqlQueryString1 = QUERY;

        Query query = QueryFactory.create(sparqlQueryString1);
        QueryExecution qexec = QueryExecutionFactory.sparqlService(
                Constants.DBPEDIA_SPARQL_ENDPOINT, query);

        // get the result set
        ResultSet results = qexec.execSelect();

        @SuppressWarnings("unchecked")
        List<QuerySolution> listResults = ResultSetFormatter.toList(results);

        @SuppressWarnings("unchecked")
        List<String> listVarnames = results.getResultVars();

        for (QuerySolution querySol : listResults) {
            for (int indx = 0; indx < listVarnames.size();) {
                String key = querySol.get(listVarnames.get(indx++)).toString();
                String value = querySol.get(listVarnames.get(indx++)).toString();
                logger.info(key + "  " + value);
            }
        }

        qexec.close();
    }

    /**
     * overloaded method to accept different query endpoint runtime
     * 
     * @param QUERY
     * @param endpoint
     * @return
     */
    public static List<QuerySolution> queryDBPediaEndPoint(final String QUERY, String endpoint)
    {
        if (endpoint == null) {
            endpoint = Constants.DBPEDIA_SPARQL_ENDPOINT;
        } else {
            Constants.DBPEDIA_SPARQL_ENDPOINT = endpoint;
        }
        return queryDBPediaEndPoint(QUERY);
    }

    public static List<QuerySolution> queryDBPediaEndPoint(final String QUERY)
    {
        List<QuerySolution> listResults = null;

        QueryExecution qexec;

        Query query = QueryFactory.create(QUERY);
        qexec = QueryExecutionFactory.sparqlService(
                Constants.DBPEDIA_SPARQL_ENDPOINT, query);
        try {
            // get the result set
            ResultSet results = qexec.execSelect();
            listResults = ResultSetFormatter.toList(results);

        } catch (Exception e) {
            System.out.println(query);
            e.printStackTrace();

        } finally
        {
            qexec.close();
        }

        return listResults;
    }

    /**
     * get type of a given instance
     * 
     * @param inst instance
     * @return list of its type
     */
    public static List<String> getInstanceTypes(String inst) {
        List<String> result = new ArrayList<String>();
        String sparqlQuery = null;

        try {
            sparqlQuery = "select ?val where{ <http://dbpedia.org/resource/" + inst
                    + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?val}";

            // fetch the result set
            List<QuerySolution> list = queryDBPediaEndPoint(sparqlQuery);

            for (QuerySolution querySol : list) {
                // Get the next result row
                // QuerySolution querySol = results.next();
                if (querySol.get("val").toString().indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1) {
                    if (!result.contains(Utilities.cleanDBpediaURI(querySol.get("val").toString())))
                        result.add(Utilities.cleanDBpediaURI(querySol.get("val").toString()));
                }
            }
        } catch (Exception e) {
            // e.printStackTrace();
            // GenericConverter.logger.info("problem with " + sparqlQuery + " "
            // + e.getMessage());
        }
        return result;
    }

    /**
     * get type of a given instance
     * 
     * @param inst instance
     * @return list of its type
     */
    public static List<String> getInstanceTypesAll(String inst) {
        List<String> result = new ArrayList<String>();
        String sparqlQuery = null;

        try {
            ResultSet results = null;
            sparqlQuery = "select ?val where{ <" + inst
                    + "> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?val} ";

            // fetch the result set
            List<QuerySolution> listResults = queryDBPediaEndPoint(sparqlQuery);

            for (QuerySolution querySol : listResults) {
                if (querySol.get("val").toString().indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1)
                    result.add(querySol.get("val").toString());
            }
        } catch (Exception e) {
            GenericConverter.logger.info("problem with " + sparqlQuery + " " + e.getMessage());
        }
        return result;
    }

    /**
     * This method finds the highest type in hierarchy given a list of such
     * types
     * 
     * @param types List of types
     */
    public static void createClassVsSubclassMap(List<String> types) {

        Set<String> setSubClasses = null;

        Iterator<String> typesIter = types.iterator();

        String subClassValue = null;
        String sparqlQuery = null;
        String key = null;

        // iterate over the types
        while (typesIter.hasNext()) {

            key = typesIter.next();
            // Set to contain the unique list of subclasses
            setSubClasses = new HashSet<String>();

            sparqlQuery = "SELECT ?val WHERE {?val <http://www.w3.org/2000/01/rdf-schema#subClassOf> <"
                    + "http://dbpedia.org/ontology/" + key + "> .} ";

            // fetch the result set
            List<QuerySolution> listResults = queryDBPediaEndPoint(sparqlQuery);

            for (QuerySolution querySol : listResults) {
                subClassValue = querySol.get("val").toString();
                if (subClassValue.indexOf(Constants.DBPEDIA_CONCEPT_NS) != -1)
                {
                    subClassValue = subClassValue.replaceAll(Constants.DBPEDIA_CONCEPT_NS, "");
                    // add the sub classes to a set
                    setSubClasses.add(subClassValue);
                }
            }

            // store in a collection
            classAndSubClassesMap.put(key, setSubClasses);
        }
    }

    /**
     * iterates the map and removes the types which occurs as subclass in some
     * other class
     * 
     * @param types
     * @return
     */
    private static List<String> removeChildClasses(List<String> types) {

        for (Map.Entry<String, Set<String>> e : classAndSubClassesMap.entrySet()) {
            for (String val : e.getValue()) {
                if (types.contains(val)) {
                    types.remove(val);
                }
            }
        }

        return types;
    }

    private static List<String> removeSuperClasses(List<String> types) {

        List<String> subClass = new ArrayList<String>();

        for (String type : types) {
            for (Map.Entry<String, Set<String>> e : classAndSubClassesMap.entrySet()) {
                if (e.getValue().contains(type)) {
                    classAndSubClassesMap.put(e.getKey(), new HashSet<String>(Arrays.asList("NA")));
                }
            }
        }

        for (Map.Entry<String, Set<String>> e : classAndSubClassesMap.entrySet()) {

            if (types.contains(e.getKey())) {
                if (!e.getValue().contains("NA") || e.getValue().size() == 0)
                    subClass.add(e.getKey());
            }
        }
        return subClass;
    }

    /**
     * Tet main class
     * 
     * @param args
     */
    public static void main(String[] args) {

        String test = "Peter_Pan";
        List<String> types = getInstanceTypes(test);
        System.out.println(types);

        // types = getHighestType(types);
        types = getLowestType(types);

        System.out.println(types);

    }

    /**
     * @param types
     * @return
     */
    public static List<String> getHighestType(List<String> types) {
        createClassVsSubclassMap(types);
        types = removeChildClasses(types);
        return types;
    }

    /**
     * @param types
     * @return
     */
    public static List<String> getLowestType(List<String> types) {
        List<String> specificType = null;
        createClassVsSubclassMap(types);
        specificType = removeSuperClasses(types);
        return specificType;
    }

} // end class

