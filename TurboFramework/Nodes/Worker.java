package TurboFramework.Nodes;

import TurboFramework.Enums.NodeType;
import TurboFramework.InformationObjects.ReducerInfo;
import TurboFramework.InformationObjects.SchedulerInfo;
import TurboFramework.Interfaces.Function;
import TurboFramework.Messages.*;
import TurboFramework.Enums.TaskPriority;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by JC Denton on 04-01-2017.
 * <p/>
 * On sent, a backup result is stored. If the recieving reducer crashes, these must be resent to another. However, they also have to be easy to remove when the reducer completes the task completely. Thus, we have a hashMap of hashmaps, where the reducerID is associated with <parentID, list of tasks>.
 * Then if a reducer crashes, we can look up all the tasks that has been sent there and send to others, as these are every element associated with every parent in the inner hashmap. When a task is finish, we can throw out the result.
 * The scheduler sends the reducer and parent task name, and we simply delete the parent element key from the inner map.
 * On a reducer crash, the reduce task is stored in outgoingMessages by the reducer id. Later the old and a new ReducerInfo will be provided so the old id can be looked up and the associated tasks can be sent to the new reducer.
 */


public class Worker implements Runnable {
    private String address;
    private int port;
    private String id;
    private SchedulerInfo scheduler;
    private ArrayList<SubtaskMessage> allTasks = new ArrayList<>();
    private ArrayList<SubtaskMessage> allPriorityTasks = new ArrayList<>();
    private HashMap<String, List<SubtaskMessage>> priorityIncomingTasks = new HashMap<>();
    private HashMap<String, List<SubtaskMessage>> incomingTasks = new HashMap<>();
    private HashMap<String, List<ReduceTask>> outgoingMessages = new HashMap<>();
    // ReducerID: <Sent result parent id, Sent Result>
    private HashMap<String, HashMap<String, ArrayList<ReduceTask>>> resultBackup = new HashMap<>();
    private HashSet<String> bannedReducers = new HashSet<>();

    private boolean processingTasks;
    // MonitorObjects
    private Object syncObject = new Object();
    private Object tasksAvailable = new Object();

    public Worker(String address, int port, String Id, SchedulerInfo scheduler) {

        this.address = address;
        this.port = port;
        this.id = Id;
        this.scheduler = scheduler;
    }

    @Override
    public void run() {
        // create heatbeat thread
        new Thread(new HeartBeatThread(scheduler.getHeartFrequency())).start();
        // create Executer thread
        new Thread(new Executor()).start();
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                Socket socket = serverSocket.accept();
                new Thread(new SocketHandler(socket)).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    class SocketHandler implements Runnable {
        Socket socket;

        public SocketHandler(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                Message message = (Message) inputStream.readObject();
                inputStream.close();
                MessageType type = message.getType();
                if (type == MessageType.NEWTASK) {

                    SubtaskMessage task = (SubtaskMessage) message.getData();
                    writeToLog("recieved message - NEW TASK:" + task.getId());
                    // We've got work to do, boys! Enqueue task and notify worker thread - let worker thread deal with it
                    String reducerID = task.getReducer().getID();
                    if (task.getPriority() == TaskPriority.HIGH) {
                        writeToLog("NEW TASK:" + task.getId() + " has high priority");

                        List<SubtaskMessage> priorityTasks = priorityIncomingTasks.get(reducerID);
                        if (priorityTasks == null) {
                            priorityTasks = new ArrayList<>();
                        }
                        allPriorityTasks.add(task);
                        priorityTasks.add(task);
                    } else {
                        List<SubtaskMessage> ordinaryTasks = incomingTasks.get(reducerID);
                        if (ordinaryTasks == null) {
                            ordinaryTasks = new ArrayList<>();
                            incomingTasks.put(reducerID, ordinaryTasks);
                        }
                        allTasks.add(task);
                        ordinaryTasks.add(task);
                    }

                    // notify taskWorker thread
                    synchronized (tasksAvailable) {
                        tasksAvailable.notifyAll();
                    }
                } else if (type == MessageType.REDUCERFAILED) {

                    // A reducer has failed - all tasks associated with that reducer must be rerouted
                    ReducerSwitchMessage switchMessage = (ReducerSwitchMessage) message.getData();
                    String oldReducer = switchMessage.getOldReducer();
                    ReducerInfo newReducer = switchMessage.getNewReducer();
                    writeToLog("recieved message - REDUCER FAILED. Rereouting " + oldReducer + "tasks to " + newReducer.getID());

                    new ReducerUpdater(oldReducer, newReducer).run();
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
            /*
        Thread recieves an ID of a failed Reducer - sends all outgoing results to the correct reducer and updates all incomplete tasks
         */

        class ReducerUpdater implements Runnable {
            private final String oldReducer;
            private final ReducerInfo newReducer;

            public ReducerUpdater(String oldReducer, ReducerInfo newReducer) {

                this.oldReducer = oldReducer;
                this.newReducer = newReducer;
            }

            @Override
            public void run() {
                List<SubtaskMessage> subtaskMessages = priorityIncomingTasks.get(oldReducer);
                if (subtaskMessages != null) {
                    for (SubtaskMessage msg : subtaskMessages) {
                        msg.setReducer(newReducer);
                    }
                }
                List<SubtaskMessage> incoming = incomingTasks.get(oldReducer);
                if (subtaskMessages != null) {
                    for (SubtaskMessage msg : incoming) {
                        msg.setReducer(newReducer);
                    }
                }
                List<ReduceTask> outgoingMsgs = outgoingMessages.get(oldReducer);
                {
                    try {

                        Socket socket = new Socket(newReducer.getIp(), newReducer.getPort());
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        for (ReduceTask outgoingMsg : outgoingMsgs) {
                            // todo: a reducer must be prepared to recieve multiple Msgs at the same time
                            objectOutputStream.writeObject(outgoingMsg);

                        }
                        objectOutputStream.close();
                    } catch (IOException e) {
                        bannedReducers.add(newReducer.getID());
                    }
                }
            }
        }
    }


    class Executor implements Runnable {
        @Override
        public void run() {
            while (true) {
                if (allTasks.isEmpty() && allPriorityTasks.isEmpty()) {
                    try {
                        processingTasks = false;
                        synchronized (tasksAvailable) {
                            tasksAvailable.wait();
                        }
                    } catch (InterruptedException e) {

                    }
                }
                    // we've been awakened - tell heartbeat that we are active
                    processingTasks = true;
                    synchronized (syncObject) {
                        syncObject.notify();

                    writeToLog("A task is available");
                    SubtaskMessage task;
                    boolean priorityTask = false;
                    if (!priorityIncomingTasks.isEmpty()) {
                        task = allPriorityTasks.get(0);
                        allPriorityTasks.remove(0);
                        priorityTask = true;
                    } else {
                        // todo: should probably be atomic - nothing can read when between get and remove
                        task = allTasks.get(0);
                        allTasks.remove(0);
                    }
                    writeToLog("processing task " + task.getId());
                        long startTime = System.nanoTime();
                        Function map = task.getMap();
                    Collection data = task.getData();
                    // it's up to the reduce function to cast result to something else
                    Object result = map.execute(data);
                    ReduceTask reduceTask = new ReduceTask(task.getParentId(), task.getId(), result, task.getReduce(), task.getSplitSize());
                    //  stop timer - inform scheduler of time and status
                        long estimatedTime = System.nanoTime() - startTime;
                    String schedulerIp = scheduler.getIp();
                    int schedulerPort = scheduler.getPort();
                    Long timeToComplete = estimatedTime; //fixme
                    Message message = new Message(MessageType.FINISHEDTASK, timeToComplete, id, task.getId());
                    try {
                        Socket socketToScheduler = new Socket(schedulerIp, schedulerPort);
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToScheduler.getOutputStream());
                        objectOutputStream.writeObject(message);
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    // if it is detected by one thread that the reducer is
                    // put in outgoing list - remove again when it is sent - it if fails or cant be sent it will be dealt with later
                    // outgoing should be HashMap where a reducerid is associated with a list of tasks - cleanup thread will then
                    ReducerInfo reducer = task.getReducer();
                    String reducerID = reducer.getID();

                    if (!bannedReducers.contains(reducerID)) {
                        String ip = reducer.getIp();
                        int port = reducer.getPort();
                        try {
                            Socket socketToReducer = new Socket(ip, port);
                            ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToReducer.getOutputStream());
                            objectOutputStream.writeObject(reduceTask);
                            objectOutputStream.close();

                            // Save the result for recovery
                            String parentID = reduceTask.getParentTaskID();
                            HashMap<String, ArrayList<ReduceTask>> tasksForReducer = resultBackup.get(reducerID);
                            ArrayList<ReduceTask> subTasksForTask;
                            if (tasksForReducer == null) {
                                tasksForReducer = new HashMap<>();
                                subTasksForTask = new ArrayList<>();
                                tasksForReducer.put(parentID, subTasksForTask);
                            } else {
                                subTasksForTask = tasksForReducer.get(parentID);
                            }
                            subTasksForTask.add(reduceTask);

                        } catch (IOException e) {
                            writeToLog("could not send" + task.getId() + " to" + reducerID + ".Reducer is banned now");
                            // reducer down - ban it so other threads don't attempt to use it
                            bannedReducers.add(reducerID);
                            List<ReduceTask> failedReduceTasks = outgoingMessages.get(reducerID);
                            if (failedReduceTasks == null) {
                                failedReduceTasks = new ArrayList<>();
                                outgoingMessages.put(reducerID, failedReduceTasks);
                            }
                            failedReduceTasks.add(reduceTask);
                            //  outgoingMessages.put(reducerID, failedReduceTasks);
                        }
                    }
                }
            }
        }
    }

    private void writeToLog(String information) {
        System.out.println("Worker " + id + ":" + information);
    }

    /*
    public void resendOldResult(String reducerID, String parentID) {
    hashmap = resultBackup.get(reducerID); // returns HashMap of all tasks that have been sent to the reducer
    for every element in hashMap, get value (a list of subtasks) and resend
    }

    public void removeCompletedTaskFromBackup


     */

    public class HeartBeatThread implements Runnable {
        private int beatFrequency;

        HeartBeatThread(int frequency) {
            this.beatFrequency = frequency;

        }

        public void run() {
            // a worker should only send a heartbeat when working on a task as the goal is to have many workers. Fewer reducers can be expected, so it is probably okay to always let them have a heartbeat. Also, the reducer heartbeat
            while (true) {
                // waits for processing of a task to start - initially false and set to false when a the task queue becomes empty AND last task has finished
                while (!processingTasks) {
                    try {
                        synchronized (syncObject) {
                            syncObject.wait();
                        }
                    }
                    // processing of a task notifies thread that it has started
                    catch (InterruptedException e) {

                    }
                    // send heartbeat every X minutes - THIS SHOULD BE LOWER THAN THE VALUE THAT FAILUREDETECTOR LISTENS FOR AS THERE IS A DELAY DUE TO ESTABLISHING CONNECTION, ETC
                    final int oneSecondInMilisecs = 1000;
                    final int SecondsPerMinute = 60;
                    try {
                        Thread.sleep(beatFrequency * SecondsPerMinute * oneSecondInMilisecs);
                    } catch (InterruptedException e) {
                        // not sure if needed, but thinking that processingTask may be set to true if a task finishes while the tread is sleeping

                    }
                    // To avoid uneccesary object creation, we should use the same heartBeat object everytime
                    try {
                        Socket socket = new Socket(scheduler.getIp(), scheduler.getPort());
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        Message message = new Message(MessageType.HEARTBEAT, NodeType.WORKER, id);
                        objectOutputStream.writeObject(message);
                        objectOutputStream.close();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
