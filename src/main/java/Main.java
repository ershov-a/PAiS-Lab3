import PAiS_Lab3.HopscotchHashMap;
import PAiS_Lab3.Map;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class Main {
    private final static int KEY_LIMIT_VALUE = 1000000;
    private final static int HASH_MAP_SIZE = 10000;
    private final static int LOAD_FACTOR = 1;
    private final static int MAX_THREADS_COUNT = 128;
    private final static int THREAD_MULTIPLY_VALUE = 2;
    private final static int OPERATIONS_PER_THREAD = 100000;
    private final static int ADD_OPERATIONS_COUNT = (int) (0.23F * OPERATIONS_PER_THREAD);
    private final static int GET_OPERATIONS_COUNT = (int) (0.23F * OPERATIONS_PER_THREAD);
    private final static int REMOVE_OPERATIONS_COUNT = (int) (0.20F * OPERATIONS_PER_THREAD);
    private final static int CONTAINS_OPERATIONS_COUNT = (int) (0.34F * OPERATIONS_PER_THREAD);

    public static void main(String[] args) throws InterruptedException {

        ArrayList<Map<Integer, Integer>> hashmapList = new ArrayList<>();

        StringBuilder result = new StringBuilder("threads,time");
        for (int currentThreadCount = 1; currentThreadCount <= MAX_THREADS_COUNT; currentThreadCount *= THREAD_MULTIPLY_VALUE) {
            hashmapList.clear();
            hashmapList.add(new HopscotchHashMap<>(HASH_MAP_SIZE, currentThreadCount));
            result.append("\n").append(currentThreadCount).append(",");
            for (Map<Integer, Integer> hashMap : hashmapList) {
                generateInitialData(hashMap);

                ArrayList<Thread> threadList = new ArrayList<>();
                Thread currentThread;

                long startTime = System.nanoTime();

                for (int i = 0; i < currentThreadCount; i++) {
                    currentThread = new OperationsThread(hashMap);
                    threadList.add(currentThread);
                    currentThread.start();
                }

                for (Thread t : threadList) {
                    t.join();
                }

                double totalTimeMilliseconds = (System.nanoTime() - startTime) / 1000000.0;

                result.append(Math.round((OPERATIONS_PER_THREAD * currentThreadCount) / totalTimeMilliseconds));
            }
        }
        System.out.print(result);
    }

    private static void generateInitialData(Map<Integer, Integer> hashMap) {
        for (int i = 0; i < HASH_MAP_SIZE * LOAD_FACTOR; i++) {
            hashMap.put(ThreadLocalRandom.current().nextInt(0, KEY_LIMIT_VALUE), 2 * HASH_MAP_SIZE * LOAD_FACTOR);
        }
    }

    public static class OperationsThread extends Thread {
        private final Map<Integer, Integer> hashMap;

        OperationsThread(Map<Integer, Integer> hashMap) {
            this.hashMap = hashMap;
        }

        @Override
        public void run() {
            // Put
            for (int i = 0; i < ADD_OPERATIONS_COUNT; i++) {
                hashMap.put(ThreadLocalRandom.current().nextInt(0, KEY_LIMIT_VALUE), ThreadLocalRandom.current().nextInt(0, 2 * LOAD_FACTOR * HASH_MAP_SIZE));
            }
            // Contains
            for (int i = 0; i < CONTAINS_OPERATIONS_COUNT; i++) {
                hashMap.containsKey(ThreadLocalRandom.current().nextInt(0, KEY_LIMIT_VALUE));
            }
            // Remove
            for (int i = 0; i < REMOVE_OPERATIONS_COUNT; i++) {
                hashMap.remove(ThreadLocalRandom.current().nextInt(0, KEY_LIMIT_VALUE));
            }
            // Get
            for (int i = 0; i < GET_OPERATIONS_COUNT; i++) {
                hashMap.get(ThreadLocalRandom.current().nextInt(0, KEY_LIMIT_VALUE));
            }
        }
    }

}
