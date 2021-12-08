package se.kth.jabeja.annealing;

/**
 * Custom annealing method
 * It starts with an acceptable difference for which a swap is accepted. This acceptable difference drops over the
 * passage of rounds, eventually reaching 0 and settling 0. The step of decreasing is defined by the parameter delta.
 */
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
