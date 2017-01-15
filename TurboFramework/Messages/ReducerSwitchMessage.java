package TurboFramework.Messages;

import TurboFramework.InformationObjects.ReducerInfo;

import java.io.Serializable;

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
