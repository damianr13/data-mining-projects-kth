package se.kth.jabeja;

import org.apache.log4j.Logger;
import se.kth.jabeja.annealing.AbstractAnnealing;
import se.kth.jabeja.config.Config;
import se.kth.jabeja.config.NodeSelectionPolicy;
import se.kth.jabeja.events.ColorEventListener;
import se.kth.jabeja.io.FileIO;
import se.kth.jabeja.rand.RandNoGenerator;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Jabeja implements ColorEventListener {
  final static Logger logger = Logger.getLogger(Jabeja.class);
  private final Config config;
  private final HashMap<Integer/*id*/, Node/*neighbors*/> entireGraph;
  private final List<Integer> nodeIds;
  private int numberOfSwaps;
  private int round;
  private boolean resultFileCreated = false;
  private AbstractAnnealing annealing;

  //-------------------------------------------------------------------
  public Jabeja(HashMap<Integer, Node> graph, Config config) {
    this.entireGraph = graph;
    this.nodeIds = new ArrayList(entireGraph.keySet());
    this.round = 0;
    this.numberOfSwaps = 0;
    this.config = config;
    this.annealing = AbstractAnnealing.fromConfig(config);
  }


  //-------------------------------------------------------------------
  public void startJabeja() throws IOException {
    for (round = 0; round < config.getRounds(); round++) {
      for (int id : entireGraph.keySet()) {
        sampleAndSwap(id);
      }

      //one cycle for all nodes have completed.
      //reduce the temperature
      saCoolDown();
      report();

      if (config.getResetAnnealing() != 0 && round % config.getResetAnnealing() == 0) {
        annealing.reset();
      }
    }
  }

  /**
   * Simulated analealing cooling function
   */
  private void saCoolDown(){
    annealing.update();
  }

  /**
   * Sample and swap algorith at node p
   * @param nodeId
   */
  private void sampleAndSwap(int nodeId) {
    Node partner;
    Node nodep = entireGraph.get(nodeId);

    Integer[] potentialNeighbourPartners = new Integer[0];
    Integer[] potentialRandomPartners = new Integer[0];
    if (config.getNodeSelectionPolicy() == NodeSelectionPolicy.HYBRID
            || config.getNodeSelectionPolicy() == NodeSelectionPolicy.LOCAL) {
      // swap with random neighbors
      potentialNeighbourPartners = getNeighbors(nodep);
    }

    if (config.getNodeSelectionPolicy() == NodeSelectionPolicy.HYBRID
            || config.getNodeSelectionPolicy() == NodeSelectionPolicy.RANDOM) {
      // if local policy fails then randomly sample the entire graph
      potentialRandomPartners = getSample(nodeId);
    }

    partner = findPartner(nodeId, potentialNeighbourPartners);
    if (partner == null) {
      partner = findPartner(nodeId, potentialRandomPartners);
    }

    if (partner == null) {
      return ;
    }

    nodep.registerColorListener(this);
    partner.registerColorListener(this);

    int sourceColor = nodep.getColor();
    nodep.setColor(partner.getColor());
    partner.setColor(sourceColor);
    numberOfSwaps++;
  }

  public Node findPartner(int nodeId, Integer[] nodes){
    Node nodep = entireGraph.get(nodeId);

    Node bestPartner = null;
    double highestBenefit = 0;

    for (int candidateId: nodes) {
      Node candidate = entireGraph.get(candidateId);
      if (candidate.getColor() == nodep.getColor()) {
        // same color --> no exchange can be performed, skip
        continue;
      }

      // exchange their colors only if this exchange decreases their energy...
      // --> increases the number of neighbors with a similar color to that of the node

      int targetDegree = getDegree(nodep, nodep.getColor()); // how many neighbors of the node have color == color of node
      int candidateDegree = getDegree(candidate, candidate.getColor()); // how many neighbors of the candidate have color == color of candidate

      int potentialTargetDegree = getDegree(nodep, candidate.getColor()); // how many neighbors of the node have color == color of candidate
      int potentialCandidateDegree = getDegree(candidate, nodep.getColor()); // how many neighbors of the candidate have color == color of node

      double oldSameColorDegree = Math.pow(targetDegree, config.getAlpha())
              + Math.pow(candidateDegree, config.getAlpha());
      double potentialSameColorDegree = Math.pow(potentialTargetDegree, config.getAlpha())
              + Math.pow(potentialCandidateDegree, config.getAlpha());

      // finds best partner... (largest decrease in energy)
      if (annealing.shouldAcceptSolution(oldSameColorDegree, potentialSameColorDegree)
              && potentialSameColorDegree > highestBenefit) {
        bestPartner = candidate;
        highestBenefit = potentialSameColorDegree;
      }
    }

    return bestPartner;
  }

  /**
   * The the degreee on the node based on color
   * @param node
   * @param colorId
   * @return how many neighbors of the node have color == colorId
   */
  private int getDegree(Node node, int colorId){
    if (node.getNeighbourColorCount(colorId) != Node.UNKNOWN_COLOR_COUNT) {
      return node.getNeighbourColorCount(colorId);
    }

    int degree = 0;
    for(int neighborId : node.getNeighbours()){
      Node neighbor = entireGraph.get(neighborId);
      if(neighbor.getColor() == colorId){
        degree++;
      }
    }

    node.learnColorMap(colorId, degree);
    return degree;
  }

  /**
   * Returns a uniformly random sample of the graph
   * @param currentNodeId
   * @return Returns a uniformly random sample of the graph
   */
  private Integer[] getSample(int currentNodeId) {
    int count = config.getUniformRandomSampleSize();
    int rndId;
    int size = entireGraph.size();
    ArrayList<Integer> rndIds = new ArrayList<Integer>();

    while (true) {
      rndId = nodeIds.get(RandNoGenerator.nextInt(size));
      if (rndId != currentNodeId && !rndIds.contains(rndId)) {
        rndIds.add(rndId);
        count--;
      }

      if (count == 0)
        break;
    }

    Integer[] ids = new Integer[rndIds.size()];
    return rndIds.toArray(ids);
  }

  /**
   * Get random neighbors. The number of random neighbors is controlled using
   * -closeByNeighbors command line argument which can be obtained from the config
   * using {@link Config#getRandomNeighborSampleSize()}
   * @param node
   * @return
   */
  private Integer[] getNeighbors(Node node) {
    ArrayList<Integer> list = node.getNeighbours();
    int count = config.getRandomNeighborSampleSize();
    int rndId;
    int index;
    int size = list.size();
    ArrayList<Integer> rndIds = new ArrayList<Integer>();

    if (size <= count)
      rndIds.addAll(list);
    else {
      while (true) {
        index = RandNoGenerator.nextInt(size);
        rndId = list.get(index);
        if (!rndIds.contains(rndId)) {
          rndIds.add(rndId);
          count--;
        }

        if (count == 0)
          break;
      }
    }

    Integer[] arr = new Integer[rndIds.size()];
    return rndIds.toArray(arr);
  }


  /**
   * Generate a report which is stored in a file in the output dir.
   *
   * @throws IOException
   */
  private void report() throws IOException {
    int grayLinks = 0;
    int migrations = 0; // number of nodes that have changed the initial color
    int size = entireGraph.size();

    for (int i : entireGraph.keySet()) {
      Node node = entireGraph.get(i);
      int nodeColor = node.getColor();
      ArrayList<Integer> nodeNeighbours = node.getNeighbours();

      if (nodeColor != node.getInitColor()) {
        migrations++;
      }

      if (nodeNeighbours != null) {
        for (int n : nodeNeighbours) {
          Node p = entireGraph.get(n);
          int pColor = p.getColor();

          if (nodeColor != pColor)
            grayLinks++;
        }
      }
    }

    int edgeCut = grayLinks / 2;

    logger.info("round: " + round +
            ", edge cut:" + edgeCut +
            ", swaps: " + numberOfSwaps +
            ", migrations: " + migrations);

    saveToFile(edgeCut, migrations);
  }

  private void saveToFile(int edgeCuts, int migrations) throws IOException {
    String delimiter = "\t\t";
    String outputFilePath;

    //output file name
    File inputFile = new File(config.getGraphFilePath());
    outputFilePath = config.getOutputDir() +
            File.separator +
            inputFile.getName() + "_" +
            "NS" + "_" + config.getNodeSelectionPolicy() + "_" +
            "GICP" + "_" + config.getGraphInitialColorPolicy() + "_" +
            "T" + "_" + config.getTemperature() + "_" +
            "D" + "_" + config.getDelta() + "_" +
            "RNSS" + "_" + config.getRandomNeighborSampleSize() + "_" +
            "URSS" + "_" + config.getUniformRandomSampleSize() + "_" +
            "A" + "_" + config.getAlpha() + "_" +
            annealing.key() + "_" +
            "R" + "_" + config.getRounds() + ".txt";

    if (!resultFileCreated) {
      File outputDir = new File(config.getOutputDir());
      if (!outputDir.exists()) {
        if (!outputDir.mkdir()) {
          throw new IOException("Unable to create the output directory");
        }
      }
      // create folder and result file with header
      String header = "# Migration is number of nodes that have changed color.";
      header += "\n\nRound" + delimiter + "Edge-Cut" + delimiter + "Swaps" + delimiter + "Migrations" + delimiter + "Skipped" + "\n";
      FileIO.write(header, outputFilePath);
      resultFileCreated = true;
    }

    FileIO.append(round + delimiter + (edgeCuts) + delimiter + numberOfSwaps + delimiter + migrations + "\n", outputFilePath);
  }

  @Override
  public void notifyChangedColor(int notifiedNodeId, int oldColor, int newColor) {
    entireGraph.get(notifiedNodeId).notifyNeighbourColorChanged(oldColor, newColor);
  }
}
