package TurboFramework.Nodes;

import TurboFramework.Enums.NodeType;
import TurboFramework.Interfaces.Function;
import TurboFramework.Messages.Message;
import TurboFramework.Messages.MessageType;
import TurboFramework.Messages.ReduceTask;
import TurboFramework.Messages.Result;
import TurboFramework.InformationObjects.SchedulerInfo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/*
A reducer has a variable heartbeat frequency, applies reduce functions when all relevant info has been recieved
 */

public class Reducer implements Runnable {

    private String address;
    private final int port;
    private final String id;
    private final SchedulerInfo scheduler;
    private ConcurrentHashMap<String, Collection<Object>> taskData = new ConcurrentHashMap<>();
    private boolean debug;

    public Reducer(String address, int port, String id, SchedulerInfo scheduler) {
        this.address = address;
        this.port = port;
        this.id = id;
        this.scheduler = scheduler;
    }

    public void run() {
        // start heartbeat tread
        // listen for communication
        try {
            ServerSocket serverSocket = new ServerSocket(port);
            while (true) {
                Socket socket = serverSocket.accept();
                new SocketHandler(socket).run();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void activateDebug() {
        this.debug = true;
    }


    public class SocketHandler implements Runnable {
        final private Socket socket;

        public SocketHandler(Socket socket) {

            this.socket = socket;
        }

        @Override
        public void run() {
            try {
                ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
                ReduceTask task = (ReduceTask) inputStream.readObject();
                inputStream.close();
                writeToLog("received a subtask " + task.getSubTaskID() + " for " + task.getParentTaskID());
                String parentTaskID = task.getParentTaskID();
                Collection<Object> subresults = taskData.get(parentTaskID);
                if (subresults == null) {
                    subresults = new ArrayList();
                    taskData.put(parentTaskID, subresults);
                }
                subresults.add(task.getData());
                // all subresults have been recieved
                if (subresults.size() == task.getSplitSize()) {
                    writeToLog("have recieved all subtasks for " + task.getParentTaskID());
                    Function reduce = task.getReduce();
                    Result finalResult = new Result(parentTaskID, reduce.execute(subresults));
                    Message message = new Message(MessageType.RESULT, finalResult, id);
                    Socket socketToScheduler = new Socket(scheduler.getIp(), scheduler.getPort());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socketToScheduler.getOutputStream());
                    objectOutputStream.writeObject(message);
                   objectOutputStream.close();
                    // no exceptions have been thrown, so the result has been succesfully transfered - now we can remove the subresults
                    taskData.remove(parentTaskID);

                }
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
    }
    public class HeartBeatThread implements Runnable {
        private int beatFrequency;

        HeartBeatThread(int frequency) {
            this.beatFrequency = frequency;

        }

        public void run() {
            while (true) {
                try {
                        Socket socket = new Socket(scheduler.getIp(), scheduler.getPort());
                        ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                        Message message = new Message(MessageType.HEARTBEAT, NodeType.REDUCER, id);
                        objectOutputStream.writeObject(message);
                        objectOutputStream.close();
                    } catch (UnknownHostException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    final int oneSecondInMilisecs = 1000;
                    final int SecondsPerMinute = 60;
                    try {
                        Thread.sleep(beatFrequency * SecondsPerMinute * oneSecondInMilisecs);
                    } catch (InterruptedException e) {
                        // not sure if needed, but thinking that processingTask may be set to true if a task finishes while the tread is sleeping

                    }
                }
            }
        }



    private void writeToLog(String information) {
        if (debug) {
            System.out.println("Reducer " + id + " " + information);
        }
        }

}
