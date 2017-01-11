
import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by JC Denton on 04-01-2017.
 */
 /*
public class Application {
    // Initialize program from here

    public static void main(String[] args) throws Exception {
        String nodeIdentity = args[0];
        if (nodeIdentity.trim().equalsIgnoreCase("worker")) {
            new Worker();
        } else if (nodeIdentity.trim().equalsIgnoreCase("reducer")) {
            new Reducer();
        } else if (nodeIdentity.trim().equalsIgnoreCase("scheduler")) {
            String path = args[2];
            List[] workersAndReducers = initNodeInfoFromFile(path);
            List<WorkerInfo> workers = workersAndReducers[0];
            List<ReducerInfo> reducers = workersAndReducers[1];
           // new Scheduler(workers, reducers);

        }
    }

    private static List[] initNodeInfoFromFile(String path) {

        ArrayList<WorkerInfo> workers = new ArrayList<>();
        ArrayList<ReducerInfo> reducers = new ArrayList<>();

        File file = new File(path);
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(file));
            String text = null;
            while ((text = reader.readLine()) != null) { // TODO: allow a range too
                String[] ipPort = text.split(":");
                String address = ipPort[0];
                int port;
                // if a port is provided, use that, else use the default port
                if (ipPort.length >= 2) {
                    port = Integer.parseInt(ipPort[1]);
                } else {
                    final int defaultPort = 6666;
                    port = defaultPort;
                }

                if (text.contains("r")) {
                    ReducerInfo reducer = new ReducerInfo(address, port);

                    // user define a reducer node by adding r, e.g. 192.168.0.1:8080:r or 192.168.0.1:r
                    reducers.add(reducer);
                } else {
                    WorkerInfo worker = new WorkerInfo(address, port);

                    workers.add(worker);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new List[]{workers, reducers};

    }
}
*/
