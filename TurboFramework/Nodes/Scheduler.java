package TurboFramework.Nodes;

import TurboFramework.Enums.NodeType;
import TurboFramework.InformationObjects.*;
import TurboFramework.Interfaces.Function;
import TurboFramework.Interfaces.SchedulingStrategy;
import TurboFramework.Interfaces.Task;
import TurboFramework.Messages.*;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class Scheduler implements Runnable {
    private final List<WorkerInfo> workers;
    private final SchedulingStrategy schedulingStrategy;
    private final int heartBeatFrequencyInMinutes;
    private final int port;

    // available reducers to chose from
    private HashMap<String, ReducerInfo> reducers = new HashMap<>(); //
    // TaskId to Reducerid - used to keep the reducer for a subtask when a worker fails. Must be updated when a reducer crashes. A task is associated with one reducer.
    private HashMap<String, String> taskToReducer = new HashMap<>();
    // For reducer recovery - ReducerID : List of task IDs. A reducer may be associated with many tasks.
    private HashMap<String, List<String>> tasksThatReducerIsResponsibleFor = new HashMap<>();


    // All id's of workers that are not working on anything currently
    private List<String> availableWorkers = new ArrayList<>();
    private HashMap<String, WorkerInfo> allWorkers = new HashMap<>();  // WorkerId --> WorkerInfo
    private HashMap<String, List<String>> workerResponsibleForTask = new HashMap<>(); // Look up the workers who have active tasks for

    private List<Task> tasks = new ArrayList<>();
    private HashMap<String, String> subTaskIDToTaskId = new HashMap<>();
    private HashMap<String, SubTaskTimer> timesForTasks = new HashMap<>();
    private HashMap<String, Task> processedTasks = new HashMap<>();
    // TaskId : Result
    private HashMap<String, Object> results = new HashMap<>();
    private ConcurrentSkipListMap<String, DateNodeTypePair> heartBeatHashMap = new ConcurrentSkipListMap<>(); // treeMap is ordered


    //Monitors:
    private Object tasksForSending = new Object();
    boolean debug;

    public Scheduler(List<WorkerInfo> workers, List<ReducerInfo> reducers, SchedulingStrategy schedulingStrategy, int heartBeatFrequencyInMinutes, int port) throws Exception {
        this.workers = workers;
        this.schedulingStrategy = schedulingStrategy;
        this.heartBeatFrequencyInMinutes = heartBeatFrequencyInMinutes;
        this.port = port;
        if (reducers.isEmpty()) {
            throw new Exception("No reducers assigned!");
        }
        if (workers.isEmpty()) {
            throw new Exception("No workers assigned!");
        }
        if (reducers.isEmpty()) {
            throw new Exception("No reducers assigned!");
        }
        if (reducers.size() > workers.size()) {
            // fixme: I dont think this is a problem anyway
            // throw new Exception("More reducers than workers have been provided!");
        }

//        this.workers = workers; // all workers on startup

        for (ReducerInfo reducer : reducers) {
            this.reducers.put(reducer.getID(), reducer);
        }
        for (WorkerInfo worker : workers) {

            availableWorkers.add(worker.getGUID());
            allWorkers.put(worker.getGUID(), worker);
        }
    }
    /*
    Method to add task from driver
     */

    public void addTask(Task task) {
        tasks.add(task);
    }

    public void activateDebug() {
        debug = true;
    }

    /**
     * method to start everything. Tempting to do in the constructor, but you can't use a constructor for that.
     */

    public void run() {


// start task distributor thread
        new Thread(new EagerTaskDistributor()).start();
// TODO:        new reactiveScheduler().run();
        // start fault detection
        new Thread(new FailureDetector()).start();
// start task loader
//TODO:
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new NetWorkListener(socket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void updateHeartbeats(Message message) {
        String sender = message.getSender();
        // just in case I screwed up somewhere and a heartbeat is sent after the last task has been completed
        if (!allWorkers.get(sender).isInactive()) {
            NodeType nodeType = (NodeType) message.getData();
            Date timeReceived = new Date();
            DateNodeTypePair dateAndNodeType = new DateNodeTypePair(timeReceived, nodeType);
            heartBeatHashMap.put(sender, dateAndNodeType);
        }
    }

    // will not be called if a worker is detected as crashed when task sending is attempted
    private void setWorkerNodeAsActive(String id) {
        DateNodeTypePair dateAndNodeType = new DateNodeTypePair(new Date(), NodeType.WORKER);
        heartBeatHashMap.put(id, dateAndNodeType); //
    }

    private void setWorkerNodeAsInactive(String id) {
        heartBeatHashMap.remove(id);
    }

    /**
     * Active scheduling strategy - Just sends all tasks, even though workers may be busy
     */
    public class EagerTaskDistributor implements Runnable {

        @Override
        public void run() {
            while (true) {
                //check that there are Tasks to distribute
                if (tasks.isEmpty()) {
                    try {
                        //FIXME
                        synchronized (tasksForSending) {
                            tasksForSending.wait();
                        }
                    } catch (InterruptedException e) {
                    }
                }
                writeToLog("There are " + tasks.size() + " available tasks (EagertaskDistributor)");
                // object creation should be avoided though and here we create
                // Note that framework cannot realize any type of the data - it must use the generic collection class, and it is up to the Map/Reduce methods to perform the necessary casts
                // An iterator is used here so that a task can be removed when it has been processed
                for (Iterator<Task> iter = tasks.listIterator(); iter.hasNext(); ) {
                    Task task = iter.next();
                    writeToLog("Splitting " + task.getName());
                    Collection data = task.getData();
                    // if there are more workers than elements in the data, split into single elements. Else, split after number of workers. A task can provide it's own split size, and throw the default value away in the split() method.
                    int splitSize = 0;
                    ArrayList<SubTaskMessageStatePair> subjobs = createSubjobs(task, task.split(data, splitSize));
                    writeToLog(task.getName() + " has been split into " + subjobs.size() + " subtasks");
                    sendTask(subjobs);
                    iter.remove();
                }
                //   parentToChildrenTaskMap.put(parentJobId, subTasks);
            }
        }
    }

    /**
     * Method creates two subtask objects - one for sending and one for internal state keeping. The latter only contains the collection that
     * is sent to a node, in case the node crashes, and the ID of the parent.
     *
     * @param task
     * @param data
     * @return
     */

    public ArrayList<SubTaskMessageStatePair> createSubjobs(Task task, Collection<Collection> data) {

        SubtaskMessage newSubTask;
        SubTaskData subTaskForState;
        String taskName = task.getName();
        String parentTaskID = java.util.UUID.randomUUID().toString();
        processedTasks.put(parentTaskID, task); // A little early to call it a processed task, but here we give it an ID
        Function mapFunction = task.getMapFunction();
        Function reduceFunction = task.getReduceFunction();
        ReducerInfo reducer = getNewReducer(parentTaskID);
        int splitSize = task.getSplitSize(); // todo: may have fucked something up here: old value = data.size();
        timesForTasks.put(parentTaskID, new SubTaskTimer(splitSize));
        ArrayList<SubTaskMessageStatePair> subTaskMessageStatePairs = new ArrayList<>();
        for (Collection collection : data) { // note: Collection of collections
            // Create the message
            String subTaskId = java.util.UUID.randomUUID().toString();
            newSubTask = new SubtaskMessage(subTaskId);
            newSubTask.setMap(mapFunction);
            newSubTask.setReduce(reduceFunction);
            newSubTask.setData(collection);
            newSubTask.setReducer(reducer);
            newSubTask.setName(taskName);
            newSubTask.setParentId(parentTaskID);
            newSubTask.setSplitSize(splitSize);
            subTaskIDToTaskId.put(subTaskId, parentTaskID);
            // create the state Object
            subTaskForState = new SubTaskData(subTaskId, collection, parentTaskID);
            SubTaskMessageStatePair subTaskMessageStatePair = new SubTaskMessageStatePair(newSubTask, subTaskForState);
            subTaskMessageStatePairs.add(subTaskMessageStatePair);
        }
        return subTaskMessageStatePairs;
    }

    /*
     A hashMap is used to remember which subtasks have been sent to a reducer - All subtasks belonging to one task must be sent to the same reducer - so this must be used if a subtask is rerouted to a new
      An alternative approach would be to save the reducer in the subtaskdata object in the workerinfo, but it would be hard to update in case of a reducer crash
     */
    public ReducerInfo getNewReducer(String taskID) {
        String reducerUsedforThisTask = taskToReducer.get(taskID);
        if (reducerUsedforThisTask != null) {
            return reducers.get(reducerUsedforThisTask);
        } else {
            // pick random
            Random generator = new Random();
            Object[] values = reducers.values().toArray();
            ReducerInfo randomReducer = (ReducerInfo) values[generator.nextInt(values.length)];
            // store that this reducer is now responsible for this task
            taskToReducer.put(taskID, randomReducer.getID());
            return randomReducer;
        }
    }

    /*
    Reactive scheduler -
    distributes tasks in response to:
     1) a worker becoming available, if there's any available tasks
     2) On input of a new task, if there are any workers available
     */
    public class reactiveScheduler implements Runnable {

        @Override
        public void run() {
            while (true) {
                // Notify when worker is ready
                if (tasks.isEmpty() || allWorkers.isEmpty()) {
                    try {
                        tasksForSending.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // the scheduler should consider only those nodes that are not busy
                    // schedulingStrategy.schedule(tasks, availableNodes);

                }
            }
        }
    }

    // called by scheduling strategy
    private void sendTask(ArrayList<SubTaskMessageStatePair> subJobs) {
        for (SubTaskMessageStatePair messageAndState : subJobs) {
            SubtaskMessage subtaskMessage = messageAndState.getMessage();
            SubTaskData subTaskData = messageAndState.getState();
            WorkerInfo worker = getWorker();
            writeToLog("Attempting to send subtask " + subtaskMessage.getId() + " to " + worker.getGUID());
            Message message = new Message(MessageType.NEWTASK, subtaskMessage, null);
            sendInformation(worker, message);
            String parentID = subtaskMessage.getParentId();
            // Save which worker is responsible for subtasks of this parent
            List<String> responsibleWorkersForParentTasks = workerResponsibleForTask.get(parentID);
            if (responsibleWorkersForParentTasks == null) {
                responsibleWorkersForParentTasks = new ArrayList<>();
                workerResponsibleForTask.put(parentID, responsibleWorkersForParentTasks);
            }

            String workerid = worker.getGUID();
            if (!responsibleWorkersForParentTasks.contains(workerid)) {
                // Don't add the same worker multiple times to the list - alternatively, maybe use a set
                responsibleWorkersForParentTasks.add(worker.getGUID());
            }
            // only register as one of the nodes tasks if actually succeeds in sending
            worker.addActiveTask(subTaskData);
            setWorkerNodeAsActive(worker.getGUID());
            String reducerId = subtaskMessage.getReducer().getID();
// Store that the reducer is responsible for this given task
            List<String> tasksAtReducer = tasksThatReducerIsResponsibleFor.get(reducerId);
            if (tasksAtReducer == null) {
                tasksAtReducer = new ArrayList<>();
                tasksThatReducerIsResponsibleFor.put(reducerId, tasksAtReducer);
            }
            tasksAtReducer.add(parentID);
        }

    }

    // This is the scheduling algorithm. It sucks currently.
    public WorkerInfo getWorker() {
        // pick first avilable worker if anybody is available
        WorkerInfo workerInfo;

        if (!availableWorkers.isEmpty()) {
            String worker = availableWorkers.get(0);
            workerInfo = allWorkers.get(worker);
            availableWorkers.remove(0);
        } else {
            // send to random already busy worker
            Random generator = new Random();
            Object[] values = allWorkers.values().toArray();
            workerInfo = (WorkerInfo) values[generator.nextInt(values.length)];

        }
        return workerInfo;
    }

    /**
     * A thread that functions as the server and recieves information from the worker and reducer nodes
     * Information is recieved when:
     */
    public class NetWorkListener implements Runnable {

        private Socket socket;

        public NetWorkListener(Socket socket) {

            this.socket = socket;
        }


        @Override
        public void run() {
            Message message = null;
            try {
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                message = (Message) inputStream.readObject();

                inputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            MessageType type = message.getType();
            if (type == MessageType.FINISHEDTASK) {
                finishTask(message);

            } else if (type == MessageType.HEARTBEAT) {
                updateHeartbeats(message);
            } else if (type == MessageType.RESULT) {
                Result result = (Result) message.getData();
                String parentTask = result.getTaskID(); // parent task - now we need to know who worked on this task => get children of task, look up their responsibleWorker
                List<String> idsOfWorkersResponsibleForTask = workerResponsibleForTask.get(parentTask);
                for (String id : idsOfWorkersResponsibleForTask) {
                    WorkerInfo responsibleWorker = allWorkers.get(id);
                    responsibleWorker.finalizeTask(parentTask);
                }

                writeToLog("recieved result " + result.getResult() + " from task " + result.getTaskID());
                results.put(parentTask, result.getResult());

            }
        }

        private void finishTask(Message message) {
            String sender = message.getSender();
            String subtaskID = message.getTask();
            Long completionTime = (Long) message.getData();
            WorkerInfo worker = allWorkers.get(sender);
            // Save time in WorkerInfo
            String parentID = subTaskIDToTaskId.get(subtaskID);
            worker.inactivateTask(parentID, subtaskID, completionTime);
            SubTaskTimer timesForTask = timesForTasks.get(parentID);
            if (timesForTask != null) {
                boolean taskCompletedWorkerWise = timesForTask.addTimeAndCheckIfFinalTime(completionTime);
                // has all workers completed their subtask execution
                if (taskCompletedWorkerWise) {
                    int averageTime = timesForTask.getAverageTime();
                    timesForTasks.remove(parentID);
                    List<String> idsOfWorkersResponsibleForTask = workerResponsibleForTask.get(parentID);
                    for (String id : idsOfWorkersResponsibleForTask) {
                        WorkerInfo responsibleWorker = allWorkers.get(id);
                        responsibleWorker.completeAndEvaluateTask(parentID, averageTime);
                    }
                }
            }
            if (worker.isInactive()) { // worker is inactive if it has no tasks to finish
                writeToLog("Worker " + sender + " is available again");
                // now available  for selection again
                availableWorkers.add(sender);
                // dont check for heartbeat
                setWorkerNodeAsInactive(sender);
            }

        }
    }

    /*
    Failure detection builds on heart beats.
     */

    class FailureDetector implements Runnable {
        private final long ONE_MINUTE_IN_MILLIS = 60000;

        public void run() {
            while (true) {
                // iterate over all active nodes, both workers and reducers
                Date now = new Date();
                Iterator it = heartBeatHashMap.entrySet().iterator();
                // Only active notes are iterated over as they are added when they become active and remove when they become inactive - just add after sending a task
                Calendar cal = Calendar.getInstance();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();
                    DateNodeTypePair dateAndNodeType = (DateNodeTypePair) pair.getValue();
                    Date lastTimeOfHeartBeat = dateAndNodeType.getLastHeartBeat();
                    cal.setTime(lastTimeOfHeartBeat);
                    // have x minutes gone by since we last heard from this - check that Date.now is not before the last time of heartbeat + x minutes (If now is 12.00 and time limit is 5 minutes, then last heartBeat would have to be at 11.55 it would be before now)
                    long timeInMilis = cal.getTimeInMillis();
                    Date lastHeartBeatPlusTimeLimit = new Date(timeInMilis + (heartBeatFrequencyInMinutes * ONE_MINUTE_IN_MILLIS));

                    String nodeId = (String) pair.getKey();

                    // Last heartbeat has expired - we assume the node is dead
                    if (now.after(lastHeartBeatPlusTimeLimit)) {
                        if (dateAndNodeType.getNodeType() == NodeType.WORKER) {
                            // node has crashed - this handles worker
                            writeToLog("Crash occurred - worker " + nodeId);
                            removeNodeAndResendActiveTasks(nodeId);
                            it.remove();
                        } else {
                            writeToLog("Crash occurred - reducer " + nodeId);
                            // dont assign this reducer to anything anymore
                            reducers.remove(nodeId);
                            List<String> taskIDs = tasksThatReducerIsResponsibleFor.get(nodeId);
                            // for every task, we need to tell the workers responsible for a subtask that it should use a new reducer

                            HashSet<String> workersToNotify = new HashSet<>();
                            for (String taskID : taskIDs) {
                                List<String> workersForTask = workerResponsibleForTask.get(taskID);
                                for (String workerForTask : workersForTask) {
                                    // Using a set here as a worker may be associated with several tasks, but we only want to notify it once and then it will take care of all tasks associated with the reducer
                                    workersToNotify.add(workerForTask);
                                }
                            }

                            ReducerInfo newReducer = replaceTaskReducerMappingsAndAssignSameReducerToAll(taskIDs);
                            ReducerSwitchMessage reducerSwitchMessage = new ReducerSwitchMessage(nodeId, newReducer);//get a new reducer and inform workers
                            Message message = new Message(MessageType.REDUCERFAILED, reducerSwitchMessage, null); // id is irrelevant here
                            for (String worker : workersToNotify) {
                                WorkerInfo workerInfo = allWorkers.get(worker);
                                sendInformation(workerInfo, message);
                            }
                        }
                    }
                }
            }
        }


        /*
        Generic method for establishing a socket to the worker and sending a message
         */

        private ReducerInfo replaceTaskReducerMappingsAndAssignSameReducerToAll(List<String> taskIDs) {
            ReducerInfo reducer = null;
            boolean first = true;
            for (String taskid : taskIDs) {
                // remove pre-existing mapping to crashed reducer
                taskToReducer.remove(taskid);
                if (first) {
                    // getNewReducer will put the new mapping in taskToReducer
                    // we only retrieve a new reducer once as all tasks
                    reducer = getNewReducer(taskid);
                    first = false;
                } else {
                    taskToReducer.put(taskid, reducer.getID());
                }
            }
            return reducer;
        }


    }


    public void sendInformation(WorkerInfo workerInfo, Message message) {
        String ipAddress = workerInfo.getIPAddress();
        int port = workerInfo.getPort();
        try {
            Socket socket = new Socket(ipAddress, port);
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
            objectOutputStream.writeObject(message);
            objectOutputStream.close();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void removeNodeAndResendActiveTasks(String nodeId) {
        // Get the info for the failed node
        WorkerInfo failedNode = allWorkers.get(nodeId);
        // worker can no longer be selected for work
        availableWorkers.remove(nodeId);
        allWorkers.remove(nodeId);
        // get the worker's active tasks - they have to go to the same reducer as originally destined
        ArrayList<SubTaskMessageStatePair> subTaskMessageStatePairs = generateNewTasks(failedNode);
        sendTask(subTaskMessageStatePairs);

    }


    private ArrayList<SubTaskMessageStatePair> generateNewTasks(WorkerInfo worker) {
// combine the data in every TaskDataObject from the WorkerInfo with the original tasks
        // for every task and for every subtask associated with the task
        ArrayList<SubTaskMessageStatePair> messages = new ArrayList<>();
        HashMap<String, HashMap<String, SubTaskData>> lostTasks = worker.getActiveTasks();
        Iterator it = lostTasks.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String parentTask = (String) pair.getKey();
            HashMap<String, SubTaskData> subtasksForParent = (HashMap) pair.getValue();
            Iterator subtaskIterator = subtasksForParent.entrySet().iterator();
            Task originalTask = processedTasks.get(parentTask);
            while (subtaskIterator.hasNext()) {
                Map.Entry subTaskIdSubtaskPair = (Map.Entry) subtaskIterator.next();
                String subTaskId = (String) subTaskIdSubtaskPair.getKey();
                SubTaskData subTaskData = (SubTaskData) subTaskIdSubtaskPair.getValue();

                SubtaskMessage newTask = new SubtaskMessage(subTaskId);
                newTask.setMap(originalTask.getMapFunction());
                newTask.setReduce(originalTask.getReduceFunction());
                newTask.setData((Collection) subTaskData.getData());
                newTask.setReducer(getNewReducer(parentTask));
                newTask.setName(originalTask.getName());
                newTask.setParentId(parentTask);
                newTask.setSplitSize(originalTask.getSplitSize());
                // note that the old task can simply be reused - it's just put into another workers workerinfo object
                SubTaskMessageStatePair subTaskMessageStatePair = new SubTaskMessageStatePair(newTask, subTaskData);
                messages.add(subTaskMessageStatePair);
            }
        }
        return messages;
    }


    private void writeToLog(String information) {
        System.out.println("Scheduler: " + information);
    }
}

