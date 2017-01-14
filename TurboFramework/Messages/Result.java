package TurboFramework.Messages;

import java.io.Serializable;

/**
 * Created by JC Denton on 04-01-2017.
 */
public class Result implements Serializable{

    private final String taskID;
    private final Object result;

    public Result(String taskID, Object result) {

        this.taskID = taskID;
        this.result = result;
    }

    public Object getResult() { // this may be problem - possibly create a class for an ArrayListResult, IntegerResult, etc
        return result;
    }

    public String getTaskID() {
        return taskID;
    }
}
