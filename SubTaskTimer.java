import java.util.ArrayList;

/**
 * Created by JC Denton on 11-01-2017.
 */

/*
This class is created when the subtasks are created
 */
public class SubTaskTimer {
    private ArrayList<Integer> times = new ArrayList<>();
    private int splitsize;

    public SubTaskTimer(int splitsize) {

        this.splitsize = splitsize;
    }

    public boolean addTimeAndCheckIfFinalTime (int time) {
   times.add(time);
        return times.size() == splitsize;
    }

   public int getAverageTime() {
       int totalTime = 0;
       int averageTime;
       for (int time : times) {
          totalTime += time;
       }
       averageTime = totalTime / splitsize;
       return averageTime;
   }
}
