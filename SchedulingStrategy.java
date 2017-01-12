import java.util.Collection;
import java.util.HashSet;

/**
 * Created by JC Denton on 11-01-2017.
 */
public interface SchedulingStrategy {
    void schedule(Collection<TaskMessage> tasks, HashSet<String> availableNodes);
}