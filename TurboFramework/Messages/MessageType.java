package TurboFramework.Messages;

import java.io.Serializable;

/**
 * Created by JC Denton on 04-01-2017.
 */
public enum MessageType implements Serializable{
    HEARTBEAT, RESULT, NEWTASK, FINISHEDTASK, REDUCERFAILED,
}
