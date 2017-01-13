/**
 * Created by JC Denton on 11-01-2017.
 */
public class Evaluation {
    private final String subtask;
    private final EvaluationScore score;

    public Evaluation(String subtaskId, int time, int averageTime) {
        this.subtask = subtaskId;
        this.score = calculateScore(time, averageTime); // fixme - it's possible, but horrible practice to call same-object methods in constructor
    }

    private EvaluationScore calculateScore(int workersTime, int averageWorkerTime) {
 // fixme: silly measure
        EvaluationScore score = null;
        if (workersTime == averageWorkerTime) {
            score = EvaluationScore.AVERAGE;
        }
        if (workersTime > averageWorkerTime){
            score = EvaluationScore.GOOD;
        }
        if (workersTime < averageWorkerTime) {
            score = EvaluationScore.BAD;
        }
        return score;
    }

    public String getSubtask() {
        return subtask;
    }

    public EvaluationScore getScore() {
        return score;
    }
}

enum EvaluationScore {
    BAD, AVERAGE, GOOD
}
