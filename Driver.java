import java.util.ArrayList;

/**
 * Created by JC Denton on 11-01-2017.
 */
public class Driver {

    public static void main(String[] args) {
        int numberOfWorkers = 3;// Integer.parseInt(args[0]);
        int numberOfReducers = 1; // Integer.parseInt(args[1]);
        SchedulingStrategy schedulingStrategy = new BasicScheduler();
        String localHost = "localhost";
        int schedulerPort = 8080;
        int workerStartport = 4000;
        int splitSize = 3;
        ArrayList<Worker> workers = new ArrayList<>();
        ArrayList<WorkerInfo> workerInfos = new ArrayList<>();
        int heartBeatFrequency = 5;
        SchedulerInfo schedulerInfo = new SchedulerInfo(localHost, schedulerPort, heartBeatFrequency);
        for (int i = 0; i < numberOfWorkers; i++) {
            int port = workerStartport + i;
            String guid = String.valueOf(i);
            Worker worker = new Worker(localHost, port, guid, schedulerInfo);
            WorkerInfo workerInfo = new WorkerInfo(localHost, port, guid);
            workerInfos.add(workerInfo);
           new Thread(worker).start();
            //workers.add(worker);
        }
        int reducerStartPort = 6000;
        ArrayList<ReducerInfo> reducerInfos = new ArrayList<>();
        for (int j = 0; j < numberOfReducers; j++) {
            int reducerPort = reducerStartPort + j;
            String reducerID = String.valueOf(1000 + j);
            Reducer reducer = new Reducer(localHost, reducerPort, reducerID, schedulerInfo);
            ReducerInfo reducerInfo = new ReducerInfo(localHost, reducerPort, reducerID);
            reducerInfos.add(reducerInfo);
            new Thread(reducer).start();
        }
        try {
            Scheduler scheduler = new Scheduler(workerInfos, reducerInfos, schedulingStrategy, heartBeatFrequency, schedulerPort);
            scheduler.addTask(new TestTask(splitSize));
        new Thread(scheduler).start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

