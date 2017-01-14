package TurboFramework.Interfaces;

import TurboFramework.Interfaces.Function;

import java.io.Serializable;
import java.util.Collection;


public interface Task extends Serializable {

    Collection getData();//

    Collection<Collection> split(Collection data, int splitSize);

    String getName();

    Function getMapFunction();

    Function getReduceFunction();

    int getSplitSize();
}
