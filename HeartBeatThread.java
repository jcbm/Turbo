import java.net.Socket;

/**
 * Created by JC Denton on 09-01-2017.
 */
public class HeartBeatThread implements Runnable {
    private int beatFrequency;

    HeartBeatThread(int frequency) {
        this.beatFrequency = frequency;

    }

    public void run() {
        // a worker should only send a heartbeat when working on a task as the goal is to have many workers. Fewer reducers can be expected, so it is probably okay to always let them have a heartbeat. Also, the reducer heartbeat
        while(true) {
            // waits for processing of a task to start - initially false and set to false when a the task queue becomes empty AND last task has finished
            while(!processingTask) {
                try {

                    wait();
                }
                // processing of a task notifies thread that it has started
                catch (InterruptedException e) {

                }

// send heartbeat every X minutes - THIS SHOULD BE LOWER THAN THE VALUE THAT FAILUREDETECTOR LISTENS FOR AS THERE IS A DELAY DUE TO ESTABLISHING CONNECTION, ETC
                final int oneSecondInMilisecs = 1000;
                final int SecondsPerMinute = 60;
                try {
                    Thread.sleep(beatFrequency * SecondsPerMinute * oneSecondInMilisecs);
                }catch (InterruptedException e) {
                    // not sure if needed, but thinking that processingTask may be set to true if a task finishes while the tread is sleeping

                }

                String SchedulerIP;
                int schedulerPort;
                // To avoid uneccesary object creation, we should use the same heartBeat object everytime
                Socket socket = new Socket(schedulerIP, shchedulerPort);

                socket.getObjectOutPutStream()
// send Msg of type Heartbeat
            }
        }
    }
}
