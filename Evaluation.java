/**
 * Created by JC Denton on 11-01-2017.
 */
public class Evaluation {
    private final String subtask;
    private final int score;

    public Evaluation(String subtaskId, int time, int averageTime) {
        this.subtask = subtaskId;
        this.score = calculateScore(time, averageTime);
    }

    private int calculateScore(int time, int averageTime) {
return 0; // todo: time > averageTime;
    }
}
