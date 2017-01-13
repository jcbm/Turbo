/*

 */

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
