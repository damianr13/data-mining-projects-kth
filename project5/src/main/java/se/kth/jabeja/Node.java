package se.kth.jabeja;

import se.kth.jabeja.events.ColorEventListener;

import java.util.*;

public class Node {

	public static final int UNKNOWN_COLOR_COUNT = -1;

	private int id;
	private int color;
	private int initColor;
	private HashSet<Integer> neighbours;

	private Map<Integer, Integer> colorToCount;
	private Set<ColorEventListener> colorEventListenerList;

	public Node(int id, int color) {
		this.id = id;
		this.color = color;
		this.initColor = color;
		this.neighbours = new HashSet<>();

		colorToCount = new HashMap<>();
		colorEventListenerList = new HashSet<>();
	}

	public void registerColorListener(ColorEventListener listener) {
		this.colorEventListenerList.add(listener);
	}

	public void setColor(int color) {
		colorEventListenerList.forEach(l ->
			this.neighbours.forEach(n -> l.notifyChangedColor(n, this.color, color)));

		this.color = color;
	}

	public void setNeighbours(ArrayList<Integer> neighbours) {
		for (int id : neighbours)
			this.neighbours.add(id);
	}

	public void learnColorMap(int color, int count) {
		colorToCount.put(color, count);
	}

	public int getNeighbourColorCount(int color) {
		return colorToCount.getOrDefault(color, UNKNOWN_COLOR_COUNT);
	}

	public void notifyNeighbourColorChanged(int oldColor, int newColor) {
		if (colorToCount.containsKey(oldColor)) {
			colorToCount.compute(oldColor, (k, v) -> v - 1);
		}

		if (colorToCount.containsKey(newColor)) {
			colorToCount.compute(newColor, (k, v) -> v + 1);
		}
	}

	public boolean hasNeighbour(int nodeId) {
		return neighbours.contains(nodeId);
	}
	
	public int getId() {
		return this.id;
	}
	public int getColor() {
		return this.color;
	}
	public int getDegree() {
		return this.neighbours.size();
	}
	public int getInitColor() {
		return this.initColor;
	}
	public ArrayList<Integer> getNeighbours() {
		return new ArrayList<>(this.neighbours);
	}
	@Override
	public String toString() {
		return "id: " + id + ", color: " + color + ", neighbours: " + neighbours + "\n";
	}
}

//	Execution time: 466072
//	Execution time: 1571729