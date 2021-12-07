package se.kth.jabeja.events;

public interface ColorEventListener {
    void notifyChangedColor(int id, int oldColor, int newColor);
}
