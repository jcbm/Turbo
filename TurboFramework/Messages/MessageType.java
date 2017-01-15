package TurboFramework.Messages;

import java.io.Serializable;

public enum MessageType implements Serializable{
    HEARTBEAT, RESULT, NEWTASK, FINISHEDTASK, REDUCERFAILED,
}
