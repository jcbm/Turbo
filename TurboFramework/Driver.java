package TurboFramework;

import TurboFramework.InformationObjects.ReducerInfo;
import TurboFramework.InformationObjects.SchedulerInfo;
import TurboFramework.InformationObjects.WorkerInfo;
import TurboFramework.Interfaces.SchedulingStrategy;
import TurboFramework.Nodes.Reducer;
import TurboFramework.Nodes.Scheduler;
import TurboFramework.Nodes.Worker;
import TurboFramework.Schedulers.BasicScheduler;
import TurboFramework.Util.TimeMeasurer;

import java.util.ArrayList;

public class Driver {

    public static void main(String[] args) {
        int numberOfWorkers = 10; // Number of workers to setup network with
        int numberOfReducers =  3; // Number of reducers to setup network
        SchedulingStrategy schedulingStrategy = new BasicScheduler();
        String localHost = "localhost";
        int schedulerPort = 8080;
        int workerStartport = 42000; // First port of the port range used for workers
        int reducerStartPort = 52000; // First port of the port range used for reducers
        int splitSize = 10; // Number of subtasks to create for data
        ArrayList<WorkerInfo> workerInfos = new ArrayList<>();
        int heartbeatFrequencyInMinutes = 5;
        SchedulerInfo schedulerInfo = new SchedulerInfo(localHost, schedulerPort, heartbeatFrequencyInMinutes);
        for (int i = 0; i < numberOfWorkers; i++) {
            int port = workerStartport + i;
            String id = String.valueOf(i);
            Worker worker = new Worker(localHost, port, id, schedulerInfo);
            WorkerInfo workerInfo = new WorkerInfo(localHost, port, id);
            workerInfos.add(workerInfo);
           new Thread(worker).start();
        }

        ArrayList<ReducerInfo> reducerInfos = new ArrayList<>();
        for (int j = 0; j < numberOfReducers; j++) {
            int reducerPort = reducerStartPort + j;
            String reducerID = String.valueOf(1000 + j);
            Reducer reducer = new Reducer(localHost, reducerPort, reducerID, schedulerInfo);
            reducer.activateDebug();
            ReducerInfo reducerInfo = new ReducerInfo(localHost, reducerPort, reducerID);
            reducerInfos.add(reducerInfo);
            new Thread(reducer).start();
        }
        try {
            Scheduler scheduler = new Scheduler(workerInfos, reducerInfos, schedulingStrategy, heartbeatFrequencyInMinutes, schedulerPort);
           // scheduler.activateDebug();
            //scheduler.addTask(new TestTask("Test task 1", splitSize)); // A simple task
         //  scheduler.addTask(new FactorialTask("Test task 2", splitSize)); // Factorial based on integers - overflows
            scheduler.addTask(new FactorialTaskBigInt("Factorial", splitSize)); // Factorial based on BigInteger





            TimeMeasurer timeMeasurer = new TimeMeasurer();
            scheduler.setTimeMeasurer(timeMeasurer);
            timeMeasurer.startMeasurement();
            new Thread(scheduler).start();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

