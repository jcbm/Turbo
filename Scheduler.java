import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class Scheduler {
    private final SchedulingStrategy schedulingStrategy;
    private final int heartBeatFrequencyInMinutes;

    //TODO: Handle case where a reducer crashes, so it is replaced with another. A worker then crashes

    private List<ReducerInfo> reducers; //
    // private List<String> healthyWorkers = new ArrayList<>();
    private HashMap<String, WorkerInfo> healthyWorkers = new HashMap<>();  // WorkerId --> WorkerInfo
    private HashMap<String, ArrayList<Task>> parentToChildrenTaskMap = new HashMap<>(); //
    public List<Task> tasks = new ArrayList<>();
    private HashMap<String, Object> results;
    private HashMap<String, SubTaskTimer> timesForTasks = new HashMap<>();
    private TreeMap<String, Date> heartBeatHashMap = new TreeMap<>(); // treeMap is ordered
    private HashMap<String, List<String>> workerResponsibleForTask = new HashMap<>(); // Look up the workers who have active tasks for
    private HashMap<String, List<SubTaskData>> parentToChildren = new HashMap<>(); // Look up the workers who have active tasks for
    private HashSet<String> availableNodes = new HashSet<>();
    private HashMap<String, List<String>> tasksThatReducerIsResponsibleFor = new HashMap<>();

    public Scheduler(List<WorkerInfo> workers, List<ReducerInfo> reducers, SchedulingStrategy schedulingStrategy, int heartBeatFrequencyInMinutes) throws Exception {
        this.schedulingStrategy = schedulingStrategy;
        this.heartBeatFrequencyInMinutes = heartBeatFrequencyInMinutes;
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
            throw new Exception("More reducers than workers have been provided!");
        }

//        this.workers = workers; // all workers on startup
        this.reducers = reducers;

        for (WorkerInfo worker : workers) {
            // we assume that all provided workers are healthy initially
            // healthyWorkers.add(worker.getGUID());
            healthyWorkers.put(worker.getGUID(), worker);
        }
    }
    /*
    Method to add task from driver
     */

    public void addTask(Task task) {
       tasks.add(task);
    }

    /**
     * method to start everything. Tempting to do in the constructor, but you can't use a constructor for that.
     */

    public void run() {


// start task distributor thread
        new EagerTaskDistributor().run();
// TODO:        new reactiveScheduler().run();
        // start fault detection
        new FailureDetector().run();
// start task loader
//TODO:
        try {
            ServerSocket serverSocket = new ServerSocket();
            while (true) {
                Socket socket = serverSocket.accept();
                new NetWorkListener(socket).run();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void updateHeartbeats(String id, Date recievedWhen) {
        Date date = new Date();
        //new HeartBeatInfo(Date, true);
        // just in case I screwed up somewhere and a heartbeat is sent after the last task has been completed
        if (!healthyWorkers.get(id).isInactive()) {
            heartBeatHashMap.put(id, date);
        }
    }

    // will not be called if a worker is detected as crash when task sending is attempted
    private void setWorkerNodeAsActive(String id) {
        heartBeatHashMap.put(id, new Date()); //
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
                        wait();
                    } catch (InterruptedException e) {
                    }
                }
                // object creation should be avoided though and here we create
                // TODO: note that we run sequntially through all tasks - we may also gain a speedup by parallelization
                // Note that framework cannot realize any type of the data - it must use the generic collection class, and it is up to the Map/Reduce methods to perform the necessary casts
                for (Task task : tasks) {
                    Collection data = task.getData();
                    List[] subjobs; // split size is provided in data
                    // if there are more workers than elements in the data, split into single elements. Else, split after number of workers. A task can provide it's own split size, and throw the default value away in the split() method.
                    int splitSize = data.size() < healthyWorkers.size() ? data.size() : healthyWorkers.size();
                    subjobs = createSubjobs(task, task.split(data, splitSize));
                    List<TaskMessage> taskMessages = subjobs[0];
                    List subjobInfo = subjobs[1];

                    sendTask(subjobs);

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

    public List[] createSubjobs(Task task, Collection<Collection> data) {

        TaskMessage newTask = null;
        SubTaskData subTaskForState = null;
        ArrayList<TaskMessage> tasks = new ArrayList<>();
        ArrayList<SubTaskData> tasksForState = new ArrayList<>();

        String taskName = task.getName();
        String parentJobid = java.util.UUID.randomUUID().toString();
        String subTaskId = java.util.UUID.randomUUID().toString();
        Function mapFunction = task.getMapFunction();
        Function reduceFunction = task.getReduceFunction();
        ReducerInfo reducer = getNewReducer();
        int splitSize = data.size();
        timesForTasks.put(parentJobid, new SubTaskTimer(splitSize));
        for (Collection collection : data) { // note: Collection of collections
            // Create the message
            newTask = new TaskMessage(subTaskId);
            newTask.setMap(mapFunction);
            newTask.setReduce(reduceFunction);
            newTask.setData(collection);
            newTask.setReducer(reducer);
            newTask.setName(taskName);
            newTask.setParentId(parentJobid);

            // create the state Object
            subTaskForState = new SubTaskData(subTaskId, collection, parentJobid);
            tasks.add(newTask);
            tasksForState.add(subTaskForState);

        }
        List[] list = new List[]{tasks, tasksForState};
        return list;
    }

    public ReducerInfo getNewReducer() {

        return reducers.get(1); //TODO get a new reducer in each call - optimize to get one that isn't assigned to other things
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
                if (tasks.isEmpty() || healthyWorkers.isEmpty()) {
                    try {
                        this.wait();
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
    private void sendTask(List[] subJobs) {
        List<TaskMessage> subJobMessages = subJobs[0];
        List<SubTaskData> subJobData = subJobs[1];
        try {
            for (int i = 0; i < subJobMessages.size(); i++) {
                TaskMessage taskMessage = subJobMessages.get(i);
                SubTaskData subTaskData = subJobData.get(i);
                Iterator it = healthyWorkers.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry pair = (Map.Entry) it.next();
                    WorkerInfo worker = (WorkerInfo) pair.getValue();

                    String address = worker.getAddress();
                    int port = worker.getPort();
                    Socket socket = new Socket(address, port);
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(taskMessage);
                    // only register as one of the nodes tasks if actually succeeds in sending
                    worker.addActiveTask(subTaskData);
                    setWorkerNodeAsActive(worker.getGUID());
                    String reducerId = taskMessage.getReducer().getGuid();
                    // List<Object> tasksAtReducer = HashMapHelper.safeGetHashMapCollection(tasksThatReducerIsResponsibleFor, reducerId);
                    List<String> tasksAtReducer = tasksThatReducerIsResponsibleFor.get(reducerId);
                    if (tasksAtReducer == null) {
                        tasksAtReducer = new ArrayList<>();
                    }
                    tasksAtReducer.add(taskMessage.getParentId());
                    tasksThatReducerIsResponsibleFor.put(taskMessage.getParentId(), tasksAtReducer);
                }
            }
        } catch (Exception e) { // TODO: Change to better exception type
            System.out.println("Scheduler.sendTask() failed. Could not send task ");
            // save all receivers GUIDs of a task to a HashMap so that we can move the info from the

        }
    }

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
                    String sender = message.getSender();
                    String task = message.getTask();
                    Integer completionTime = (Integer) message.getData();
                    WorkerInfo worker = healthyWorkers.get(sender);
// Save time in WorkerInfo
String parentID = null; //TODO - have a way to translate subtaskid to taskid
worker.inactivateTask(parentID, task, completionTime);
                    SubTaskTimer timesForTask = timesForTasks.get(parentID);
                    // will never be null
                  boolean taskCompletedWorkerWise = timesForTask.addTimeAndCheckIfFinalTime(completionTime);
                    if (taskCompletedWorkerWise) {
                        int averageTime = timesForTask.getAverageTime();
                        List<String> idsOfWorkersResponsibleForTask = workerResponsibleForTask.get(parentID);
                        for (String id : idsOfWorkersResponsibleForTask) {
                            WorkerInfo responsibleWorker = healthyWorkers.get(id);
                            responsibleWorker.completeAndEvaluateTask(parentID, averageTime);
                        }
                    }

                    if (worker.isInactive()) { // worker is inactive if it has no tasks to finish
                        setWorkerNodeAsInactive(sender);
                    }
                } else if (type == MessageType.HEARTBEAT) {
                    String sender = message.getSender();
                    Date timeRecieved = new Date();
                    updateHeartbeats(sender, timeRecieved);
                } else if (type == MessageType.RESULT) {
                    Result result = (Result) message.getData();
                    String parentTask = result.getTaskID(); // parent task - now we need to know who worked on this task => get children of task, look up their responsibleWorker
                    List<String> idsOfWorkersResponsibleForTask = workerResponsibleForTask.get(parentTask);
                    for (String id : idsOfWorkersResponsibleForTask) {
                        WorkerInfo responsibleWorker = healthyWorkers.get(id);
                        responsibleWorker.finalizeTask(parentTask);
                    }

                    //ArrayList<Task> subJobs = parentToChildrenTaskMap.get(parentTask);


                    Object resultValue = result.getResult(); // todo: instanceOf may be useful here
                    results.put(parentTask, result.getResult());

                }
        }
    }

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
                    Date lastTimeOfHeartBeat = (Date) pair.getValue();
                    // System.out.println(pair.getKey() + " = " + pair.getValue());
                    cal.setTime(lastTimeOfHeartBeat);
                    // have x minutes gone by since we last heard from this - check that Date.now is not before the last time of heartbeat + x minutes (If now is 12.00 and time limit is 5 minutes, then last heartBeat would have to be at 11.55 it would be before now)
                    long timeInMilis = cal.getTimeInMillis();
                    Date lastHeartBeatPlusTimeLimit = new Date(timeInMilis + (heartBeatFrequencyInMinutes * ONE_MINUTE_IN_MILLIS));
                    if (now.before(lastHeartBeatPlusTimeLimit)) {
                        // node has crashed
                        String nodeId = (String) pair.getKey();
                        //                    removeNodeAndReplace(nodeId);
                        it.remove();
                    }


                }
            }
        }
    }
}

