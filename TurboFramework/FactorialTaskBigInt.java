package TurboFramework;

import TurboFramework.Interfaces.Function;
import TurboFramework.Interfaces.Task;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;


public class FactorialTaskBigInt implements Task {

    private String name;
    private int splitSize;

    public FactorialTaskBigInt(int splitSize) {
        this.splitSize = splitSize;
    }

    public FactorialTaskBigInt(String name, int splitSize) {
        this.splitSize = splitSize;
        this.name = name;
    }


    @Override
    public Collection getData() {
        int collectionSize = 10000;
        ArrayList<BigInteger> numbers = new ArrayList<>(collectionSize);

        for (int i = 1; i <= collectionSize; i++) {
            numbers.add(BigInteger.valueOf(20));
        }
        return numbers;
    }

    @Override
    public Collection<Collection> split(Collection data) {
        int upperLimit;
        int subListIncrement;
        Collection<Collection> collections = new ArrayList<>(splitSize);
        int sizeOfSubCollections = data.size() / splitSize;
        int remainder = data.size() % splitSize;
        boolean checkForLastSplit = false;
        if (data.size() <= splitSize) {
            // we can't split a task more than the size of the collection
            upperLimit = data.size();
            subListIncrement = 1;
        } else {
           subListIncrement = sizeOfSubCollections;
            upperLimit = splitSize;
            checkForLastSplit = true;
        }

        int firstElementOfSublist = 0;

        for (int i = 0; i < upperLimit; i++) {
            // last split size has a different size if data size is not a multiple of splitsize.
            int lastElementOfSublist = firstElementOfSublist + subListIncrement;
            if (checkForLastSplit && (remainder != 0 && (i + 1) == splitSize)) {
                lastElementOfSublist = firstElementOfSublist + remainder;
            }
            ArrayList<BigInteger> subCollection = new ArrayList<>(((ArrayList) data).subList(firstElementOfSublist, lastElementOfSublist));
            collections.add(subCollection);
            firstElementOfSublist += subListIncrement;
        }

        return collections;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Function getMapFunction() {
        class Mapper implements Function {

            @Override
            public Object execute(Collection<Object> data) {
                BigInteger sum = BigInteger.ZERO;
                // Have added sleep to delay execution
                for (Object bigInt : data) {
                    sum = sum.add(factorial((BigInteger) bigInt));
                }

                Object returnValue = sum;
                return returnValue;
            }
        }
        return new Mapper();
    }

    public BigInteger factorial(BigInteger n) {
        BigInteger factorial = BigInteger.valueOf(1);

        for (int i = 1; i <= n.intValue(); i++) {
            factorial = factorial.multiply(BigInteger.valueOf(i));
        }

        return factorial;
    }

    @Override
    public Function getReduceFunction() {
        class Reducer implements Function {
            @Override
            public Object execute(Collection<Object> data) {
                BigInteger sum = BigInteger.ZERO;
                for (Object integer : data) {
                    sum = sum.add((BigInteger) integer);
                }


                return sum;
            }
        }
        return new Reducer();
    }

    public int getSplitSize() {
        return splitSize;
    }
}
