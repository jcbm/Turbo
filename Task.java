import java.io.Serializable;
import java.util.Collection;

/**
 * Created by JC Denton on 04-01-2017.
 */
public interface Task extends Serializable {

    //private Function mapFunction;
    //private Function reduceFunction;
// private TaskType;
    public Collection getData();// { return null; // TODO: Replace w. old interface}

    public Collection<Collection> split(Collection data, int splitSize);

    public String getName();

    public Function getMapFunction();

    public Function getReduceFunction();
}
