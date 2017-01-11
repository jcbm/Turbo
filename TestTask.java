import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by JC Denton on 11-01-2017.
 */
public class TestTask implements Task {

    private int splitSize;

    public TestTask(int splitSize) {
        this.splitSize = splitSize;
    }

    @Override
    public Collection getData() {

        ArrayList<Integer> numbers = new ArrayList<>(100);
        int size = splitSize * 3;
        for (int i = 1; i <= splitSize; i++) {
            numbers.add(i);
            //System.out.println(numbers.get(i - 1));
        }
        return numbers;
    }

    @Override
    public Collection<Collection> split(Collection data, int splitSize) {
        Collection<Collection> collections = new ArrayList<>(splitSize);
        int sizeOfSubCollections = data.size() / splitSize;

        // example: numbers 1-9 in data spread in 3 lists
        //

        for (int i = 0; i <= splitSize; i = i + sizeOfSubCollections) {
            ArrayList<Integer> subCollection = (ArrayList) ((ArrayList) data).subList(i, i + splitSize);
            collections.add(subCollection);
        }
        return collections;
    }

    @Override
    public String getName() {
        return "Test";
    }

    @Override
    public Function getMapFunction() {
        class Mapper implements Function {

            @Override
            public Object execute(Collection<Object> data) {
                int sum = 0;
                for (Object integer : data) {
                    sum = sum + ((Integer) integer);

                }
                Object returnValue = new Integer(sum);
                return returnValue;
            }
        }
        return new Mapper();
    }

    @Override
    public Function getReduceFunction() {
        class Reducer implements Function {
            @Override
            public Object execute(Collection<Object> data) {
                int sum = 0;
                for (Object integer : data) {
                    sum = sum + ((Integer) integer);
                }


                return sum;
            }
        }
        return new Reducer();
    }
}
