package se.kth.jabeja.annealing;

import se.kth.jabeja.config.Config;

public abstract class AbstractAnnealing {

    protected double T, delta;
    protected final double initialT;

    public AbstractAnnealing(double t, double delta) {
        this.T = t;
        this.initialT = t;
        this.delta = delta;
    }

    public void reset() {
        T = initialT;
    }

    public abstract void update();

    public abstract boolean shouldAcceptSolution(double currentValue, double potentialValue);

    public abstract String key();

    protected double currentTemperature() {
        return T;
    }

    public static AbstractAnnealing fromConfig(Config config) {
        switch (config.getAnnealingType()) {
            case "type1":
                return new Type1Annealing(config.getTemperature(), config.getDelta());
            case "type2":
                return new Type2Annealing(config.getTemperature(), config.getDelta());
            case "type3":
                return new Type3Annealing(config.getTemperature(), config.getDelta());
            case "type4":
                return new Type4Annealing(config.getTemperature(), config.getDelta());
            default:
                throw new IllegalArgumentException("Unknown annealing type: " + config.getAnnealingType());
        }
    }
}
