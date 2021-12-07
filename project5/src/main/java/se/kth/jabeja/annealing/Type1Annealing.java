package se.kth.jabeja.annealing;

class Type1Annealing extends AbstractAnnealing{

    public Type1Annealing(double t, double delta) {
        super(t, delta);
    }

    public void update() {
        T = Math.max(1, T - delta);
    }

    @Override
    public boolean shouldAcceptSolution(double currentValue, double potentialValue) {
        return potentialValue * currentTemperature() > currentValue;
    }

    @Override
    public String key() {
        return "TYPE1";
    }
}
