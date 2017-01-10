import javax.naming.CompositeName;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class Scheduler {

    //TODO: Handle case where a reducer crashes, so it is replaced with another. A worker then crashes

    private List<ReducerInfo> reducers; //
    // private List<String> healthyWorkers = new ArrayList<>();
    private HashMap<String, WorkerInfo> healthyWorkers = new HashMap<>();
    private HashMap<String, ArrayList<Task>> parentToChildrenTaskMap = new HashMap<>(); //
    public List<Task> tasks;
    private HashMap<String, Object> results;
    private HashMap<String, Date> heartBeatHashMap;
private HashMap<String, List<String>> workerResponsibleForTask = new HashMap<>(); // Look up the workers who have active tasks for
private HashMap<String, List<SubTaskData>> parentToChildren = new HashMap<>(); // Look up the workers who have active tasks for

    public Scheduler(List<WorkerInfo> workers, List<ReducerInfo> reducers) throws Exception {
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

    /**
     * method to start everything. Tempting to do in the constructor, but you can't use a constructor for that.
     */

    public void run() {

// start network listener thread

// start task distributor thread
        new TaskDistributor().run();
        // start fault detection

// start task loader
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

                  //  for (Task subJob : subjobs) {
                       //  sendTask(subJob);

                    }
                 //   parentToChildrenTaskMap.put(parentJobId, subTasks);
                }
            }
        }

        /**
         * Method creates two subtask objects - one for sending and one for internal state keeping. The latter only contains the collection that
         * is sent to a node, in case the node crashes, and the ID of the parent.
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

            for (Collection collection : data) { // note: Collection of collections
                // Create the message
                newTask = new TaskMessage(subTaskId);
                newTask.setMap(mapFunction);
                newTask.setReduce(reduceFunction);
                newTask.setData(collection);
                newTask.setReducer(reducer); // we need to save the reducer in case of failure
                newTask.setName(taskName);
                newTask.setParentId(parentJobid);

                // create the state Object
                subTaskForState = new SubTaskData(subTaskId, collection, parentJobid);
                tasks.add(newTask);
                tasksForState.add(subTaskForState);

            }
            List[] list  = new List[] {tasks, tasksForState};
        return list;
        }

        public ReducerInfo getNewReducer() {

            return reducers.get(1); //TODO get a new reducer in each call - optimize to get one that isn't assigned to other things
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
            while(true) {
                // Notify when worker is ready
                if (tasks.isEmpty() OR availableNodes.isEmpty()) {
                    try {
                        this.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    // the scheduler should consider only those nodes that are not busy
                    schedulingStrategy.schedule(tasks, availableNodes);

                }
            }
        }
    }


    private void sendTask(WorkerInfo worker, Task subJob) {

        try {
// open socket
            SubTaskData subtaskdata;
worker.setActive(subtaskdata);
            // only register as one of the nodes tasks if actually succeeds in sending
            worker.addActiveTask(task);

        } catch (Exception e) { // TODO: Change to better exception type
            System.out.println("Scheduler.sendTask() failed. Could not send task " + task.)
            // save all receivers GUIDs of a task to a HashMap so that we can move the info from the

        }
    }

    public class NetWorkListener implements Runnable {

        @Override
        public void run() {
            while (true) {
                Message message = null;
                MessageType type = message.getType();
                if (type == MessageType.FINISHEDTASK) {
                    String sender = message.getSender();
                    WorkerInfo worker = healthyWorkers.get(sender);
// Save time in WorkerInfo
                    // remove from WorkerInfo active tasks
                    if (worker.isInactive()) {
                        setWorkerNodeAsInactive(sender);
                    }
                } else if (type == MessageType.HEARTBEAT) {
                    String sender = message.getSender();
                    Date timeRecieved = new Date();
                    updateHeartbeats(sender, timeRecieved);
                } else if (type == MessageType.RESULT) {
                    Result result = (Result) message.getData();
                    String parentTask = result.getTaskID(); // parent task - now we need to know who worked on this task => get children of task, look up their responsibleWorker
                    ArrayList<Task> subJobs = parentToChildrenTaskMap.get(parentTask);
                    for (Task subJob : subJobs) {
                        // get responsible workers - move out of active jobs
                    }
                    Object resultValue = result.getResult(); // todo: instanceOf may be useful here
                    results.put(parentTask, result.getResult());

                }

            }
        }
    }

