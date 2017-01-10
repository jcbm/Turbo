import java.util.ArrayList;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class SubTaskData {

    private final String id;
    private final Object data;
    private final String parentID;
    private String workerID;

    public SubTaskData(String id, Object data, String parentID) {
        this.id = id;
        this.data = data;
        this.parentID = parentID;
    }
}
