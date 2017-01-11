import java.util.ArrayList;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class SubTaskData {

    private int completionTime;

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

    public void setCompletionTime(int completionTime) {
        this.completionTime = completionTime;
    }

    public int getCompletionTime() {
        return completionTime;
    }
}
