import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class Worker {
    private String address;
    private int port;
    private String id;
    private SchedulerInfo scheduler;
    private ArrayList<TaskMessage> allTasks = new ArrayList<>();
    private ArrayList<TaskMessage> allPriorityTasks = new ArrayList<>();
    private HashMap<String, List<TaskMessage>> priorityIncomingTasks = new HashMap<>();
    private HashMap<String, List<TaskMessage>> incomingTasks = new HashMap<>();
    private HashMap<String, List<ReduceTask>> outgoingMessages = new HashMap<>();
    private HashSet<String> bannedReducers = new HashSet<>();
    private int taskCount;

    public Worker(String address, int port, String Id, SchedulerInfo scheduler) {

        this.address = address;
        this.port = port;
        this.id = Id;
        this.scheduler = scheduler;
    }

    public void run() {
        // create heatbeat thread
        // create Executer thread

        while (true) {
            ServerSocket serverSocket = null;
            try {
                serverSocket = new ServerSocket();

                Socket socket = null;

                socket = serverSocket.accept();

                new SocketHandler(socket).run();
            } catch (IOException e) {
                e.printStackTrace();
            }
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
                    MessageType type = message.getType();
                    if (type == MessageType.NEWTASK) {
                        TaskMessage task = (TaskMessage) message.getData();

                        // We've got work to do, boys! Enqueue task and notify worker thread - let worker thread deal with it
                        String reducerID = task.getReducer().getGuid();
                        if (task.getPriority() == TaskPriority.HIGH) {
                            List<TaskMessage> priorityTasks = priorityIncomingTasks.get(reducerID);
                            if (priorityTasks == null) {
                                priorityTasks = new ArrayList<>();
                            }
                            allPriorityTasks.add(task);
                            priorityTasks.add(task);
                        } else {
                            List<TaskMessage> ordinaryTasks = priorityIncomingTasks.get(reducerID);
                            if (ordinaryTasks == null) {
                                ordinaryTasks = new ArrayList<>();
                            }
                            allTasks.add(task);
                            ordinaryTasks.add(task);
                        }

                        taskCount++;
                        // notify taskWorker thread
                    } else if (type == MessageType.REDUCERFAILED) {
                        // A reducer has failed - all tasks associated with that reducer must be rerouted
                        ReducerSwitchMessage switchMessage = (ReducerSwitchMessage) message.getData();
                        String oldReducer = switchMessage.getOldReducer();
                        ReducerInfo newReducer = switchMessage.getNewReducer();
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
                    List<TaskMessage> taskMessages = priorityIncomingTasks.get(oldReducer);
                    if (taskMessages != null) {
                        for (TaskMessage msg : taskMessages) {
                            msg.setReducer(newReducer);
                        }
                    }
                    List<TaskMessage> incoming = incomingTasks.get(oldReducer);
                    if (taskMessages != null) {
                        for (TaskMessage msg : incoming) {
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
                        } catch (IOException e) {
                            bannedReducers.add(newReducer.getGuid());
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
                            wait();

                        } catch (InterruptedException e) {

                        }
                        // we've been awakened - tell heartbeat that we are active
// TODO: start timer
                        TaskMessage task;
                        boolean priorityTask = false;
                        if (!priorityIncomingTasks.isEmpty()) {
                            task = allPriorityTasks.get(0);
                            priorityTask = true;
                        } else {
                            // todo: should probably be atomic - nothing can read when between get and remove
                            task = allTasks.get(0);
                            allTasks.remove(0);
                        }
                        Function map = task.getMap();
                        Collection data = task.getData();
                        // it's up to the reduce function to cast result to something else
                        Collection result = (Collection) map.execute(data);
                        ReduceTask reduceTask = new ReduceTask(task.getParentId(), result, task.getReduce(), task.getSplitSize());
                        // TODO: stop timer - inform scheduler of time and status
                        String schedulerIp = scheduler.getIp();
                        int schedulerPort = scheduler.getPort();
                        Message message = new Message(MessageType.FINISHEDTASK, null, id);
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
                        String reducerID = reducer.getGuid();

                        if (!bannedReducers.contains(reducerID)) {
                            String ip = reducer.getIp();
                            int port = reducer.getPort();
                            try {
                                Socket socketToReducer = new Socket(ip, port);
                                ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToReducer.getOutputStream());
objectOutputStream.writeObject(reduceTask);
//TODO: Remove from incoming as it has been taken off - either it is sent or it is in outgoing collection
                                List<TaskMessage> taskMessages = null;
                                if (priorityTask) {
                                    taskMessages = priorityIncomingTasks.get(reducerID);
                                } else {
                                    incomingTasks.get(reducerID);
                                for (TaskMessage taskMsg : taskMessages) {

                                    }
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                                // reducer down - ban it so other threads don't attempt to use it
                                bannedReducers.add(reducerID);
                                List<ReduceTask> failedReduceTasks = outgoingMessages.get(reducerID);
                                if (failedReduceTasks == null) {
                                  failedReduceTasks =  new ArrayList<ReduceTask>();
                                }
                                failedReduceTasks.add(reduceTask);
                                outgoingMessages.put(reducerID, failedReduceTasks);
                            }
                        }
                    }
                }
            }
        }

}
