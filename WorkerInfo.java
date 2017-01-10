import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class WorkerInfo {
    private final String guid;
    private String address;
    private int port;
    private HashMap<String, ArrayList<Task>> activeTasks; //
    private HashMap<String, ArrayList<Task>> historicalTasks; //

    public WorkerInfo(String address, int port) {
        this.address = address;
        this.port = port;
        this.guid = null; // TODO
    }

    public String getGUID() {
        return guid;
    }

    // if a node crashes, we need to have saved what it was working on. We store subjobs by the parent jobs id, but in a list, so we may have multiple subjobs associated with the same parentjob, in case the node execues more subjobs associated with this job
    public void addActiveTask(SubTaskData subtask) {
        String taskID = subtask.getParentID();
        ArrayList<Task> tasksForParent = activeTasks.get(taskID);
        // No tasks have previously been added for this parent
        if (tasksForParent == null) {
            tasksForParent = new ArrayList<>();
        }
        tasksForParent.add(subtask);
        activeTasks.put(taskID, tasksForParent);
    }

    // When we recieve a task finished message - when a final result is recieved -
    public void inactivateTask(Task subtask) {
        String taskID = subtask.getParentID();
        ArrayList<Task> removedTasks = activeTasks.get(taskID);
        historicalTasks.put(removedTasks);
        activeTasks.remove(taskID);

    }


    public boolean isInactive() {
        return activeTasks.isEmpty();
    }
}
