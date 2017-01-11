import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

/**
 * Created by JC Denton on 04-01-2017.
 */
/*
A reducer has a variable heartbeat frequence, applies reduce functions when all relevant info has been recieved
 */

public class Reducer {

    private String address;
    private final int port;
    private final String guid;
    private final SchedulerInfo scheduler;
    private HashMap<String, Collection<Object>> taskData = new HashMap<>();

    public Reducer(String address, int port, String guid, SchedulerInfo scheduler) {
        this.address = address;
        this.port = port;
        this.guid = guid;
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
                String parentTaskID = task.getParentTaskID();
                Collection<Object> subresults = taskData.get(parentTaskID);
                if (subresults == null) {
                    subresults = new ArrayList();
                }
                subresults.add(task.getData());
                // all subresults have been recieved
                if (subresults.size() == task.getSplitSize()) {
                    Function reduce = task.getReduce();
                    Result finalResult = new Result(parentTaskID, reduce.execute(subresults));
                    Socket socketToScheduler = new Socket(scheduler.getIp(), scheduler.getPort());
                    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
                    objectOutputStream.writeObject(finalResult);
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

}
