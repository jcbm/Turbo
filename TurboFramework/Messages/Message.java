package TurboFramework.Messages;

import java.io.Serializable;

public class Message implements Serializable{


    private final MessageType type;
    private final Object data;
    private String sender;
    private String task;

    public Message (MessageType type, Object data, String sender) {

        this.type = type;
        this.data = data;
        this.sender = sender;
    }

    public Message (MessageType type, Object data, String sender, String subtask) {

        this.type = type;
        this.data = data;
        this.sender = sender;
        this.task = subtask;
    }

    public MessageType getType() {
      return type;
    }

    public Object getData() {
        return data;
    }

    public String getSender() {
        return sender;
    }

    public String getTask() {
        return task;
    }
}
