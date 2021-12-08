package se.kth.jabeja.annealing;

import se.kth.jabeja.rand.RandNoGenerator;

class Type2Annealing extends AbstractAnnealing {

    public Type2Annealing(double t, double delta) {
        super(Math.min(t, 1), delta);
    }

    public void update() {
        T = Math.max(minValue(), T * delta);
    }

    @Override
    public boolean shouldAcceptSolution(double currentValue, double potentialValue) {
        double acceptanceThreshold = RandNoGenerator.nextDouble();
        double acceptanceProbability = potentialValue > currentValue ?
                1 : Math.exp((potentialValue - currentValue) / currentTemperature());
        return acceptanceProbability > acceptanceThreshold;
    }

    protected double minValue() {
        return 1e-10;
    }

    @Override
    public String key() {
        return "TYPE2";
    }
}
