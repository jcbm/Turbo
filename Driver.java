import java.util.ArrayList;

/**
 * Created by JC Denton on 11-01-2017.
 */
public class Driver {

    public static void main(String[] args) {
        int numberOfWorkers = Integer.parseInt(args[0]);
        int numberOfReducers = Integer.parseInt(args[1]);
        SchedulingStrategy schedulingStrategy = new BasicScheduler();
        String localHost = "localhost";
int schedulerPort = 8080;
        int workerStartport = 4000;
        ArrayList<Worker> workers = new ArrayList<>();
        SchedulerInfo schedulerInfo = new SchedulerInfo(localHost, schedulerPort);
        for (int i = 0; i < numberOfWorkers; i++) {
            Worker worker = new Worker(localHost, workerStartport + i, String.valueOf(i), schedulerInfo);
    workers.add(worker);
        }
int reducerStartPort = 6000;
        ArrayList<Reducer> reducers = new ArrayList<>();
        for (int j = 0; j < numberOfWorkers; j++) {
            Reducer reducer = new Reducer(localHost, workerStartport + i, String.valueOf(i), schedulerInfo);
            reducers.add(reducer);
        }
Scheduler scheduler = new Scheduler()
    }
    }
}
