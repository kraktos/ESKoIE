/**
 * 
 */

package code.dws.dao;

import java.util.ArrayList;
import java.util.List;

import org.semanticweb.owlapi.model.OWLClass;

/**
 * Data object representing a tree structure where every node is a DBPedia class
 * with a set of attributes,
 * 
 * @author Arnab Dutta
 */
public class GenericTreeNode {

    /**
     * name of the node, the DBPedia class
     */
    private OWLClass nodeName;

    /**
     * value of the node denoting instances for this class
     */
    private double nodeValue;

    /**
     * re computed value for the nodes coming from the children
     */
    private double nodeUpScore;

    /**
     * re computed value for the nodes coming from the parent
     */
    private double nodeDownScore;

    /**
     * collection of child nodes
     */
    private List<GenericTreeNode> children;

    /**
     * depth at which this node is placed in the tree
     */
    private int nodeLevel;

    /**
     * @param nodeName
     */
    public GenericTreeNode(OWLClass nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * @param nodeName
     * @param nodeValue
     */
    public GenericTreeNode(OWLClass nodeName, double nodeValue) {
        this.nodeName = nodeName;
        this.nodeValue = nodeValue;
        this.children = new ArrayList<GenericTreeNode>();
    }

    /**
     * @return the nodeName
     */
    public OWLClass getNodeName() {
        return this.nodeName;
    }

    /**
     * @return the nodeValue
     */
    public double getNodeValue() {
        return this.nodeValue;
    }

    /**
     * @return the nodePseudoValueFromChild
     */
    public double getNodeUpScore() {
        return this.nodeUpScore;
    }

    /**
     * @return the nodePseudoValueFromParent
     */
    public double getNodeDownScore() {
        return this.nodeDownScore;
    }

    /**
     * @return the nodeLevel
     */
    public int getNodeLevel() {
        return this.nodeLevel;
    }

    /**
     * @param nodeName the nodeName to set
     */
    public void setNodeName(OWLClass nodeName) {
        this.nodeName = nodeName;
    }

    /**
     * @param nodeValue the nodeValue to set
     */
    public void setNodeValue(double nodeValue) {
        this.nodeValue = nodeValue;
    }

    /**
     * @param nodePseudoValueFromChild the nodePseudoValueFromChild to set
     */
    public void setNodeUpScore(double nodeUpScore) {
        this.nodeUpScore = nodeUpScore;
    }

    /**
     * @param nodePseudoValueFromParent the nodePseudoValueFromParent to set
     */
    public void setNodeDownScore(double nodeDownScore) {
        this.nodeDownScore = nodeDownScore;
    }

    /**
     * @param children the children to set
     */
    public void setChildren(List<GenericTreeNode> children) {
        this.children = children;
    }

    /**
     * @param nodeLevel the nodeLevel to set
     */
    public void setNodeLevel(int nodeLevel) {
        this.nodeLevel = nodeLevel;
    }

    /**
     * @return the children
     */
    public List<GenericTreeNode> getChildren() {
        if (this.children != null)
            return this.children;
        else
            return new ArrayList<GenericTreeNode>();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("GenericTreeNode [");
        if (nodeName != null) {
            builder.append("nodeName=");
            builder.append(this.nodeName);
            builder.append(", ");
        }
        if (nodeValue >= 0) {
            builder.append("nodeValue=");
            builder.append(this.nodeValue);
            builder.append(", ");
        }
        if (nodeUpScore >= 0) {
            builder.append("nodeUpValue=");
            builder.append(this.nodeUpScore);
            builder.append(", ");
        }
        if (nodeDownScore >= 0) {
            builder.append("nodeDownValue=");
            builder.append(this.nodeDownScore);
        }
        // if (children != null) {
        // builder.append("children=");
        // for (GenericTreeNode childNodes : this.children) {
        // builder.append(childNodes.getNodeName());
        // builder.append(", ");
        // builder.append(childNodes.getNodeValue());
        // builder.append("\n");
        // }
        // }
        builder.append("]");
        return builder.toString();
    }

    /**
     * prints a particular node value
     * 
     * @param node
     * @return
     */
    public String printNode() {
        StringBuilder builder = new StringBuilder();
        builder.append(this.nodeName.toString());
        builder.append("[");
        builder.append("Original = " + this.nodeValue);
        builder.append(", ");
        builder.append("UP Score = " + this.nodeUpScore);
        builder.append(", ");

        builder.append("DOWN Score = " + this.nodeDownScore);
        builder.append(", ");

        builder.append("Children = " + getNumberOfChildren());
        builder.append("]");

        return builder.toString();
    }

    /**
     * prints node and it children
     * 
     * @param node
     */
    public void display(GenericTreeNode node) {
        System.out.println(node.getNodeName().toString() + "  \t"
                + node.getNodeValue());

        for (GenericTreeNode child : node.getChildren()) {
            System.out.println(">>>\t" + child.getNodeName().toString() + "  \t"
                    + child.getNodeValue());
            display(child);
        }
    }

    public void addChild(GenericTreeNode child) {
        this.children.add(child);
    }

    /**
     * removes a specific node by the node.
     * 
     * @param node
     * @throws IndexOutOfBoundsException
     */
    public void removeChild(GenericTreeNode node) throws IndexOutOfBoundsException {
        int index = this.children.indexOf(node);
        if (index != -1)
            this.children.remove(index);
    }

    /**
     * returns the number of children of this node
     * 
     * @return
     */
    public int getNumberOfChildren() {
        return getChildren().size();
    }

}
