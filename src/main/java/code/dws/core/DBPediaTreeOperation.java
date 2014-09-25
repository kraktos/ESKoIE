/**
 * 
 */

package code.dws.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import code.dws.dao.GenericTreeNode;
import code.dws.utils.Constants;

import com.clarkparsia.pellet.owlapiv3.PelletReasoner;
import com.clarkparsia.pellet.owlapiv3.PelletReasonerFactory;

/**
 * Creates a tree structure out of the DBPedia onology
 * 
 * @author Arnab Dutta
 */
public class DBPediaTreeOperation {

	private OWLClass THING = null;
	private OWLClass UNTYPED = null;
	private OWLDataFactory factory;

	private PelletReasoner reasoner;
	private OWLOntology ontology;
	private OWLOntologyManager manager;

	List<Pair<String, Double>> domPairs;
	List<Pair<String, Double>> ranPairs;

	// first create the top most concept "T"
	GenericTreeNode TREE = null;

	GenericTreeNode UNTYPEDNODE = null;

	// A collection of all the scored up nodes
	static Map<String, GenericTreeNode> COLLECTION_NODES = new HashMap<String, GenericTreeNode>();

	String identifier = null;

	boolean flag = false;

	// int nodeDepthCounter = 0;

	/**
	 * @return the identifier
	 */
	public String getIdentifier() {
		return identifier;
	}

	/**
	 * @param identifier
	 *            the identifier to set
	 */
	public void setIdentifier(String identifier) {
		this.identifier = identifier;
	}

	/**
	 * parameterized constructor
	 * 
	 * @param path
	 */
	public DBPediaTreeOperation(String path) {

		manager = OWLManager.createOWLOntologyManager();
		File ontologyFile = new File(path);

		try {
			ontology = manager.loadOntologyFromOntologyDocument(ontologyFile);
			PelletReasonerFactory reasonerFactory = new PelletReasonerFactory();
			reasoner = reasonerFactory.createReasoner(ontology);
			factory = this.manager.getOWLDataFactory();
			System.out.println("Loaded " + ontology.getOntologyID());

			this.UNTYPED = factory.getOWLClass(IRI.create("UNTYPED"));

			this.THING = factory.getOWLThing();

			// initialize tree with the top concept "Thing"
			this.TREE = new GenericTreeNode(this.THING);
			this.TREE.setChildren(new ArrayList<GenericTreeNode>());

		} catch (OWLOntologyCreationException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	/**
	 * STAND ALONE TEST POINT
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		DBPediaTreeOperation dbTOp = new DBPediaTreeOperation(
				Constants.DBPEDIA_TBOX);

		// run the learner to score the nodes
		dbTOp.buildDomRan(new String[] { "bookwriter", "1" });
	}

	/**
	 * @return the uNTYPEDNODE
	 */
	public GenericTreeNode getUNTYPEDNODE() {
		return this.UNTYPEDNODE;
	}

	/**
	 * @param uNTYPEDNODE
	 *            the uNTYPEDNODE to set
	 */
	public void setUNTYPEDNODE(GenericTreeNode uNTYPEDNODE) {
		this.UNTYPEDNODE = uNTYPEDNODE;
	}

	/**
	 * returns the score of a node, ie a given type
	 * 
	 * @param type
	 *            DBPedia type
	 * @return score
	 */
	public Double getNodeScore(String type) {
		if (COLLECTION_NODES.containsKey(type))
			return COLLECTION_NODES.get(type).getNodeDownScore();

		return -1D;
	}

	/**
	 * @param dbPediaTypes
	 *            types of the entity, used for querying
	 * @param fullSetOfTpes
	 * @return a ranked list of DBpedia types for that entity
	 * @throws IOException
	 */

	public Map<String, Pair<Double, Double>> getNodeScore(
			List<String> dbPediaTypes, Set<String> fullSetOfTpes) {

		double totalScore = 0;
		Map<String, Double> rankedNodesMap = new HashMap<String, Double>();

		GenericTreeNode node = null;

		// find scores of each node in the tree
		for (Map.Entry<String, GenericTreeNode> entry : COLLECTION_NODES
				.entrySet()) {

			node = entry.getValue();

			// sum up the original scores
			totalScore = totalScore + node.getNodeValue();

			// put the nodes with computed scores in a collection
			rankedNodesMap.put(node.getNodeName().getIRI().toURI().toString()
					.replaceAll(",", "_"), node.getNodeDownScore());
		}

		// with the total score, we should also add the score of the UNTYPED
		// node.
		totalScore = totalScore + this.UNTYPEDNODE.getNodeValue();
		rankedNodesMap.put("UNTYPED", this.UNTYPEDNODE.getNodeValue());

		return sortByValue(rankedNodesMap, totalScore);
	}

	/**
	 * sort a map by value descending
	 * 
	 * @param map
	 * @param totalScore
	 * @param tripleCounter
	 * @return
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	static Map<String, Pair<Double, Double>> sortByValue(Map map,
			double totalScore) {
		List list = new LinkedList(map.entrySet());
		Collections.sort(list, new Comparator() {
			public int compare(Object o2, Object o1) {
				return ((Comparable) ((Map.Entry) (o1)).getValue())
						.compareTo(((Map.Entry) (o2)).getValue());
			}
		});

		Map result = new LinkedHashMap();
		for (Iterator it = list.iterator(); it.hasNext();) {
			Map.Entry entry = (Map.Entry) it.next();
			result.put(entry.getKey(), new ImmutablePair<Double, Double>(
					(Double) entry.getValue(), (Double) entry.getValue()
							/ totalScore));
		}
		return result;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public void buildDomRan(String[] args) throws IOException {

		System.out.println("Learning domain/range values for " + args[0]);

		// initiate the dom range un supervised learning technique
		this.computeDomRan(args);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public void generateClassHierarchy(String args) throws IOException {

		this.setIdentifier(args);

		System.out.println("Generating tree of concepts...");
		this.createFullTreeStructure();

		// once done with tree creation, we can start manipulating with the
		// values.
		System.out.println("starting score propagation from leaves...");
		this.doNodeScoring();

		// // resetting the tree
		// this.TREE = new GenericTreeNode(this.THING);
		// this.TREE.setChildren(new ArrayList<GenericTreeNode>());
	}

	/**
	 * method to learn the domain and range from the data set from the OIE
	 * 
	 * @param args
	 * @throws IOException
	 */
	public void computeDomRan(final String[] args) throws IOException {

		DistantSupervised distSup = new DistantSupervised(args);

		if (!Constants.BOOTSTRAP)
			distSup.learnDomRanFromFullData();

		if (Constants.BOOTSTRAP)
			distSup.learnDomRanFromFilteredOutputFiles();

		// get the domain range values for the given predicate

		domPairs = distSup.getTopDomainClass(0);
		ranPairs = distSup.getTopRangeClass(0);

		// domPairs = dummyTestSet();
		// this.generateClassHierarchy(Constants.DOMAIN);

	}

	/**
	 * method to construct the tree structure
	 */
	private void createFullTreeStructure() {

		List<OWLClass> listTopLevelClasses = getLonelyTopConcepts();

		double nodeValue = findDomainRangeValueByName(this.UNTYPED.getIRI()
				.toURI().toString()
				.replaceAll(Constants.ONTOLOGY_NAMESPACE, ""));

		// create an untyped node. this is independent of any class hierarchy
		this.UNTYPEDNODE = new GenericTreeNode(UNTYPED, nodeValue);
		this.UNTYPEDNODE.setNodeDownScore(nodeValue);
		this.UNTYPEDNODE.setNodeUpScore(nodeValue);

		TREE.addChild(this.UNTYPEDNODE);

		populateChildren(TREE, listTopLevelClasses);

		// with the initial tree constructed, iteratively construct the full
		// tree
		recurAndAddChildren(TREE);

		// fancy printing only
		// preOrderTreeTraversal(TREE);
	}

	/**
	 * this method starts from the leaf nodes and computes the value of its
	 * parent. And iteratively this value is propagated up the tree.
	 */
	private void doNodeScoring() {

		// key idea is to traverse the tree in post-order since in that case we
		// can start from the leaves and move up
		propagateValuesUp(TREE);

		// once down we again push the up-scores down
		propagateValuesDown();

	}

	/**
	 * here we start from top node, and keep updating the down values till the
	 * leaves. Basically it is guided by level-order traversal
	 */
	public void propagateValuesDown() {
		Queue<GenericTreeNode> queue = new LinkedList<GenericTreeNode>();
		queue.add(TREE);

		while (!queue.isEmpty()) {
			GenericTreeNode node = queue.remove();

			for (GenericTreeNode child : node.getChildren()) {

				if (!THING.equals(child.getNodeName())) {
					if (isDirectSubClsOfThing(child.getNodeName())) {
						child.setNodeDownScore(child.getNodeUpScore());
					} else {

						child.setNodeDownScore(getParentsDownScore(node)
								+ downScore(child));
					}
				}

				// print the final info after complete scoring of all nodes
				// if (child.getNodeValue() > 0)
				// System.out.println(child.printNode());

				COLLECTION_NODES.put(child.getNodeName().getIRI().toURI()
						.toString(), child);
				queue.add(child);
			}
		}
	}

	/**
	 * search a particular node in the tree
	 * 
	 * @param tree
	 * @param searchnode
	 * @return
	 */
	public GenericTreeNode searchForNode(GenericTreeNode tree,
			OWLClass searchnode) {

		GenericTreeNode result = null;
		if (tree.getNodeName().equals(searchnode)) {
			result = tree;
		}

		for (GenericTreeNode child : tree.getChildren()) {
			result = searchForNode(child, searchnode);
			if (result != null)
				System.out.println(" GOT SOMETHNG.." + result);
		}

		return result;
	}

	/*
	 * get the down score of your parent
	 */
	private double getParentsDownScore(GenericTreeNode node) {

		// find your parent node first
		GenericTreeNode matchedNode = node;

		try {
			if (matchedNode != null)
				return matchedNode.getNodeDownScore();
			else
				return 0;
		} catch (Exception e) {
			System.out.println("Exception with " + node.getNodeName() + " \t "
					+ e.getMessage());
			return 0;
		}
	}

	/**
	 * computes the value from the leaves up to the top level. This is
	 * essentially a post-order traversal
	 * 
	 * @param node
	 */
	public void propagateValuesUp(GenericTreeNode node) {
		if (node != null) {
			for (GenericTreeNode child : node.getChildren()) {
				propagateValuesUp(child);

				// do the calculation of the scores here
				if (child.getNumberOfChildren() == 0) {
					child.setNodeUpScore(child.getNodeValue());
				} else {
					child.setNodeUpScore(upScore(child));
				}
			}
		}
	}

	/**
	 * score for downward propagation
	 * 
	 * @param node
	 * @return
	 */
	private double downScore(GenericTreeNode node) {
		return (1 - Constants.PROPGTN_FACTOR) * node.getNodeUpScore();
	}

	/**
	 * score for upward propagation
	 * 
	 * @param node
	 * @return
	 */
	private double upScore(GenericTreeNode node) {
		double sumChildScores = 0;
		for (GenericTreeNode n : node.getChildren()) {
			sumChildScores = sumChildScores + n.getNodeUpScore();
		}
		return node.getNodeValue() + Constants.PROPGTN_FACTOR * sumChildScores;
	}

	/**
	 * core method to recursively create the full tree structure
	 * 
	 * @param node
	 */
	private void recurAndAddChildren(GenericTreeNode node) {
		List<OWLClass> subConcepts;
		// start off with the collection of top level concepts
		for (GenericTreeNode childNode : node.getChildren()) {

			// for each node, find its subclasses
			// get all its subclasses
			subConcepts = new ArrayList<OWLClass>(reasoner.getSubClasses(
					childNode.getNodeName(), true).getFlattened());

			// populate the node with these child nodes
			populateChildren(childNode, subConcepts);

			// repeat for this node
			recurAndAddChildren(childNode);
		}
	}

	/**
	 * given a node and a list of its children, this method, creates a bunch of
	 * child nodes
	 * 
	 * @param node
	 * @param listChildClasses
	 */
	private void populateChildren(GenericTreeNode node,
			List<OWLClass> listChildClasses) {

		double nodeValue;

		// iterate through the list of domains and create a tree.
		// this loop creates only the head node and its direct sub classes
		for (OWLClass childClass : listChildClasses) {

			if (childClass.getIRI().toURI().toString()
					.indexOf(Constants.ONTOLOGY_NAMESPACE) != -1) {

				nodeValue = findDomainRangeValueByName(childClass.getIRI()
						.toURI().toString()
						.replaceAll(Constants.ONTOLOGY_NAMESPACE, ""));

				// System.out.println("Adding node " +
				// childClass.getIRI().toURI().toString() + " ("
				// + nodeValue + ")");

				node.addChild(new GenericTreeNode(childClass, nodeValue));
			}
		}

	}

	/**
	 * returns a list of top level concepts directly subclasses of owl:Thing
	 * 
	 * @return
	 */
	private List<OWLClass> getLonelyTopConcepts() {
		List<OWLClass> classNames = new ArrayList<OWLClass>();

		// get all the classes in the ontology
		Set<OWLClass> concepts = this.ontology.getClassesInSignature();

		for (OWLClass dbPediaClass : concepts) {
			String className = dbPediaClass.getIRI().toURI().toString();

			if (isDirectSubClsOfThing(dbPediaClass)
					&& className.indexOf(Constants.ONTOLOGY_NAMESPACE) != -1) {
				classNames.add(dbPediaClass);
			}
		}
		return classNames;
	}

	/**
	 * method to check if it is a direct subclass of owl:Thing
	 * 
	 * @param owlClass
	 * @return
	 */
	private boolean isDirectSubClsOfThing(OWLClass owlClass) {
		Set<OWLClass> superConcepts = reasoner.getSuperClasses(owlClass, true)
				.getFlattened();
		if (superConcepts.size() == 1 && superConcepts.contains(THING))
			return true;
		return false;
	}

	/**
	 * method to find the instance count of the domain class for the given
	 * property.
	 * 
	 * @param topClass
	 * @return
	 */
	private double findDomainRangeValueByName(String topClass) {

		if (this.getIdentifier().equals(Constants.DOMAIN)) {
			for (Pair<String, Double> pairDomValue : domPairs) {
				if (pairDomValue.getLeft().equals(topClass)) {
					return pairDomValue.getRight();
				}
			}
		} else {
			for (Pair<String, Double> pairRanValue : ranPairs) {
				if (pairRanValue.getLeft().equals(topClass)) {
					return pairRanValue.getRight();
				}
			}
		}
		return 0;
	}

	/**
	 * this is not just post order traversal but it computes the up score in the
	 * process
	 * 
	 * @param node
	 */
	public void preOrderTreeTraversal(GenericTreeNode node) {
		if (node != null) {
			for (GenericTreeNode child : node.getChildren()) {
				System.out.println(child.printNode());
				preOrderTreeTraversal(child);
			}
		}
	}

}
