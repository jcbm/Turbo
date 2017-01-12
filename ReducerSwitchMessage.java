import java.io.Serializable;

/**
 * Created by JC Denton on 10-01-2017.
 */
public class ReducerSwitchMessage implements Serializable {
    private final String oldReducer;
    private final ReducerInfo newReducer;


    public ReducerInfo getNewReducer() {
        return newReducer;
    }

    public String getOldReducer() {
        return oldReducer;
    }



    public ReducerSwitchMessage(String oldReducer, ReducerInfo newReducer) {
        this.oldReducer = oldReducer;
        this.newReducer = newReducer;
    }
}
