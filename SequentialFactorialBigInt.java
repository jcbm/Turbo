import TurboFramework.FactorialTask;
import TurboFramework.Util.TimeMeasurer;

import java.math.BigInteger;

public class SequentialFactorialBigInt {

    public static void main(String[] args) {
        BigInteger value = BigInteger.valueOf(100);
        TimeMeasurer timeMeasurer = new TimeMeasurer();
        timeMeasurer.startMeasurement();
        BigInteger sum = BigInteger.ZERO;
        for (int i = 0; i < 10000; i++) {
           sum = sum.add(factorial((value)));
        }
        try {
            System.out.println(sum);
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
