package TurboFramework.InformationObjects;/*

 */

import TurboFramework.InformationObjects.SubTaskData;
import TurboFramework.Messages.SubtaskMessage;

public class SubTaskMessageStatePair {



    private final SubtaskMessage message;
    private final SubTaskData state;

    public SubTaskMessageStatePair(SubtaskMessage message, SubTaskData state) {

        this.message = message;
        this.state = state;
    }

    public SubtaskMessage getMessage() {
        return message;
    }

    public SubTaskData getState() {
        return state;
    }
}
