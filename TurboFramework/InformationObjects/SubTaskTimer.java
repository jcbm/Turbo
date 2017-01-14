package TurboFramework.InformationObjects;

import java.util.ArrayList;

/**
 * Created by JC Denton on 11-01-2017.
 */

/*
This class is created when the subtasks are created
 */
public class SubTaskTimer {
    private ArrayList<Long> times = new ArrayList<>();
    private int splitsize;
    private boolean notFinalized = true;

    public SubTaskTimer(int splitsize) {

        this.splitsize = splitsize;
    }

    public boolean addTimeAndCheckIfFinalTime (long time) {
   times.add(time);
       boolean finalTime = times.size() == splitsize;
        return finalTime;
    }

   public int getAverageTime() {
       int totalTime = 0;
       int averageTime;
       for (long time : times) {
          totalTime += time;
       }
       averageTime = totalTime / splitsize;
       return averageTime;
   }
}
