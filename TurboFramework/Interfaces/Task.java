package TurboFramework.Interfaces;

import java.io.Serializable;
import java.util.Collection;


public interface Task extends Serializable {

    Collection getData();//

    Collection<Collection> split(Collection data);

    String getName();

    Function getMapFunction();

    Function getReduceFunction();

    int getSplitSize();
}
