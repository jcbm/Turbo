import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
        int size = splitSize * 3;
        ArrayList<Integer> numbers = new ArrayList<>(size);

        for (int i = 1; i <= size; i++) {
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
        int firstElementOfSublist = 0;
        for (int i = 0; i < splitSize; i++) {
            ArrayList<Integer> subCollection = new ArrayList<>(((ArrayList) data).subList(firstElementOfSublist, firstElementOfSublist + splitSize));
            collections.add(subCollection);
            firstElementOfSublist += splitSize;
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
                // Have added sleep to delay execution
                try {
                    Thread.sleep(15 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
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
