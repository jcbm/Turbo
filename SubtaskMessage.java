import java.io.Serializable;
import java.util.Collection;

/**
 * Created by JC Denton on 09-01-2017.
 */
public class SubtaskMessage implements Serializable {
    private final String id;
    private Function map;
    private Function reduce;
    private Collection data;
    private ReducerInfo reducer;
    private String name;
    private String parentId;
    private TaskPriority priority;
    private int splitSize;

    public SubtaskMessage(String id) {
        this.id = id;
    }

    public void setMap(Function map) {
        this.map = map;
    }

    public void setReduce(Function reduce) {
        this.reduce = reduce;
    }

    public void setData(Collection data) {
        this.data = data;
    }

    public void setReducer(ReducerInfo reducer) {
        this.reducer = reducer;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setParentId(String parentID) {
        this.parentId = parentID;
    }

    public TaskPriority getPriority() {
        return priority;
    }

    public void setPriority(TaskPriority priority) {
        this.priority = priority;
    }

    public String getId() {
        return id;
    }

    public Function getMap() {
        return map;
    }

    public Function getReduce() {
        return reduce;
    }

    public Collection getData() {
        return data;
    }

    public ReducerInfo getReducer() {
        return reducer;
    }

    public String getName() {
        return name;
    }

    public String getParentId() {
        return parentId;
    }

    public int getSplitSize() {
        return splitSize;
    }

    public void setSplitSize(int splitSize) {
        this.splitSize = splitSize;
    }
}
