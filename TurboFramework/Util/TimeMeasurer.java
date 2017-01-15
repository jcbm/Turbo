package TurboFramework.Util;

public class TimeMeasurer {
    private long startTime;
    private boolean startMeasurementSet;

    public TimeMeasurer() {

    }

   public void startMeasurement() {
        startTime = System.nanoTime();
        startMeasurementSet = true;
    }

  public  void stopMeasurement() throws Exception {
        if (!startMeasurementSet) throw new Exception("Cannot stop measurement before it is started");
        long elapsedTime = System.nanoTime() - startTime;
      double elapsedTimeInseconds = (double)elapsedTime / 1000000000.0;
        System.out.println("Time for task execution " + elapsedTimeInseconds + " seconds");
        startMeasurementSet = false;

    }

}
