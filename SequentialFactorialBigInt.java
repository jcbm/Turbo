import TurboFramework.FactorialTask;
import TurboFramework.Util.TimeMeasurer;

import java.math.BigInteger;

public class SequentialFactorialBigInt {

    public static void main(String[] args) {
        int value = 20;
        TimeMeasurer timeMeasurer = new TimeMeasurer();
        timeMeasurer.startMeasurement();
        factorial(BigInteger.valueOf(value));
        try {
            timeMeasurer.stopMeasurement();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static BigInteger factorial(BigInteger n) {
        BigInteger factorial = BigInteger.valueOf(1);

        for (int i = 1; i <= n.intValue(); i++) {
            factorial = factorial.multiply(BigInteger.valueOf(i));
        }

        return factorial;
    }
}
