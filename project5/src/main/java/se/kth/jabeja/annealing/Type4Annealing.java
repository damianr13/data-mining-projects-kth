package se.kth.jabeja.annealing;

import se.kth.jabeja.rand.RandNoGenerator;

class Type4Annealing extends AbstractAnnealing {

    public Type4Annealing(double t, double delta) {
        super(Math.min(1, t), delta);
    }

    @Override
    public void update() {
        T = Math.max(0, T - delta);
    }

    @Override
    public boolean shouldAcceptSolution(double currentValue, double potentialValue) {
        return potentialValue + T > currentValue;
    }

    @Override
    public String key() {
        return "TYPE4";
    }
}
