package TurboFramework.Interfaces;

import TurboFramework.Messages.SubtaskMessage;

import java.util.Collection;
import java.util.HashSet;

public interface SchedulingStrategy {
    void schedule(Collection<SubtaskMessage> tasks, HashSet<String> availableNodes);
}
