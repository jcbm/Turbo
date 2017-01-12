import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class WorkerInfo {
    private String guid;
    private String address;
    private int port;
    // ParentID to HashMap<subtask id to subtaskData> - tells which tasks are waiting to processed/are currently processed
    private HashMap<String, HashMap<String, SubTaskData>> activeTasks = new HashMap<>(); //
    // containedTasks tells which tasks have been completed at the worker, but not yet at the reducer
    private HashMap<String, ArrayList<SubTaskData>> containedTasks = new HashMap<>(); //
    private HashMap<String, HashMap<String, SubTaskData>> historicalTasks = new HashMap<>(); //
    private ArrayList<Evaluation> evaluations = new ArrayList();

    public WorkerInfo(String address, int port, String guid) {
        this.address = address;
        this.port = port;
        this.guid = guid;
    }

    public String getGUID() {
        return guid;
    }

    // if a node crashes, we need to have saved what it was working on. We store subjobs by the parent jobs id, but in a list, so we may have multiple subjobs associated with the same parentjob, in case the node execues more subjobs associated with this job
    public void addActiveTask(SubTaskData subtask) {
        System.out.println("WorkerInfo " + guid + " has added " + subtask.getId() + " to active set");
        String parentTask = subtask.getParentID();
        String subtaskID = subtask.getId();
        HashMap<String, SubTaskData> tasksForParent = activeTasks.get(parentTask);
        // No tasks have previously been added for this parent
        if (tasksForParent == null) {
            tasksForParent = new HashMap<>();
            activeTasks.put(parentTask, tasksForParent);

        }
        tasksForParent.put(subtaskID, subtask);
    }

    // When we recieve a task finished message - when a final result is recieved -
    // we inactivate a task when it is finished, that is, it is removed from the activeset. However, to know
    public void inactivateTask(String parentID, String subtaskID, int completionTime) { // this is obtained in the scheduler where we have SubtaskID:subtask (not data) map --> getParent
        //should never return null, so we can be sure we get a collection
        System.out.println("WorkerInfo " + guid + " is removing " + subtaskID + " from active set");
        HashMap<String, SubTaskData> subTaskDataHashMap = activeTasks.get(parentID);
        // this task has been completed
        SubTaskData subTaskData = subTaskDataHashMap.get(subtaskID);
        subTaskData.setCompletionTime(completionTime);
        HashMap<String, SubTaskData> compledTasksForParent = historicalTasks.get(parentID);
        if (compledTasksForParent == null) {
            compledTasksForParent = new HashMap<>();
        }
        compledTasksForParent.put(subtaskID, subTaskData);
        historicalTasks.put(parentID, compledTasksForParent);
        subTaskDataHashMap.remove(subtaskID);
        ArrayList<SubTaskData> notFinalizedTasks = containedTasks.get(parentID);
        if (notFinalizedTasks == null) {
            notFinalizedTasks = new ArrayList<>();
        }
        notFinalizedTasks.add(subTaskData);
        containedTasks.put(parentID, notFinalizedTasks);
        // maybe  activeTasks.put(parentID, subTaskDataHashMap);
    }

    // Called when all subtask for a given task has completed
    public void completeAndEvaluateTask(String parentID, int averageCompletionTime) { // we get the average time in the scheduler by using the taskID
// go into historical tasks - get each task associated with this parent and getTime;
        HashMap<String, SubTaskData> subTasksForThisTask = historicalTasks.get(parentID);
        Iterator<Map.Entry<String, SubTaskData>> iterator = subTasksForThisTask.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry pair = (Map.Entry) iterator.next();
            SubTaskData subtask = (SubTaskData) pair.getValue();
            String subtaskID = subtask.getId();
            int taskCompletionTime = subtask.getCompletionTime();
            Evaluation evaluation = new Evaluation(subtaskID, taskCompletionTime, averageCompletionTime);
            evaluations.add(evaluation);
        }
    }





    public boolean isInactive() {
        return activeTasks.isEmpty();
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    // called when the reducer has returned the final result - it is now safe to
    public void finalizeTask(String parentTask) {
        containedTasks.remove(parentTask);

    }
}
