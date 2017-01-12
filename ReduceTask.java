import java.io.Serializable;
import java.util.Collection;

/**
 * Created by JC Denton on 09-01-2017.
 */

/*
A class to send jobs to the reducer. There's no need to include all the info that the Mapper recieves.
// Possible split this into two so mappers only send the data, the rest is sent from the scheduler
 */

public class ReduceTask implements Serializable{
    private String parentTaskID;
    private final Collection data;
    private final Function reduce;
    private final int splitSize;

    public ReduceTask(String parentTaskID, Collection data, Function reduce, int splitSize) {
        this.parentTaskID = parentTaskID;
        this.data = data;
        this.reduce = reduce;
        this.splitSize = splitSize;
    }
    public Collection getData() {
        return data;
    }

    public Function getReduce() {
        return reduce;
    }

    public int getSplitSize() {
        return splitSize;
    }


    public String getParentTaskID() {
        return parentTaskID;
    }
}
