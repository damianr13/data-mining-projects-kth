package se.kth.jabeja.annealing;

class Type3Annealing extends AbstractAnnealing {

    public Type3Annealing(double t, double delta) {
        super(t, delta);
    }

    @Override
    public void update() {
        // NONE
    }

    @Override
    public boolean shouldAcceptSolution(double currentValue, double potentialValue) {
        return potentialValue >= currentValue;
    }

    @Override
    public String key() {
        return "TYPE3";
    }
}
