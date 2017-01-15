package TurboFramework.InformationObjects;

import java.util.ArrayList;

public class SubTaskData {

    private long completionTime;

    public String getId() {
        return id;
    }

    public Object getData() {
        return data;
    }

    public String getParentID() {
        return parentID;
    }

    private final String id;
    private final Object data;
    private final String parentID;
    private String workerID;

    public SubTaskData(String id, Object data, String parentID) {
        this.id = id;
        this.data = data;
        this.parentID = parentID;
    }

    public void setCompletionTime(long completionTime) {
        this.completionTime = completionTime;
    }

    public long getCompletionTime() {
        return completionTime;
    }
}
