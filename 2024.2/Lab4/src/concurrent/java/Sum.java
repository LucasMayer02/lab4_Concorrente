import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Sum {

    private static final AtomicLong totalSum = new AtomicLong(0);
    private static final ConcurrentHashMap<Long, CopyOnWriteArrayList<String>> sumMap = new ConcurrentHashMap<>();
    private static Semaphore semaphore;

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: java Sum filepath1 filepath2 filepathN");
            System.exit(1);
        }
    
        int maxConcurrent = Math.max(1, args.length / 2);
        semaphore = new Semaphore(maxConcurrent);
        ExecutorService executor = Executors.newFixedThreadPool(maxConcurrent);

        for (String path : args) {
            executor.submit(new SumTask(path));
        }

        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

        System.out.println("Total sum: " + totalSum.get());

        for (var entry : sumMap.entrySet()) {
            if (entry.getValue().size() > 1) {
                System.out.print(entry.getKey() + " ");
                for (String file : entry.getValue()) {
                    System.out.print(file + " ");
                }
                System.out.println();
            }
        }
    }

    static class SumTask implements Runnable {
        private final String path;

        SumTask(String path) {
            this.path = path;
        }

    @Override
    public void run() {
        try{
            semaphore.acquire();

            long sum = sum(path);

            totalSum.addAndGet(sum);
            System.out.println(path + " : " + sum);

            sumMap.computeIfAbsent(sum, k -> new CopyOnWriteArrayList<>()).add(path);

            semaphore.release();
        } catch (Exception e) {
            System.err.println("Error processing file " + path + ": " + e.getMessage());
        }
    }
    
    private long sum(String path) throws IOException {
        Path filePath = Paths.get(path);
        if (Files.isRegularFile(filePath)) {
            try (FileInputStream fis = new FileInputStream(filePath.toString())) {
                int byteRead;
                long sum = 0;
                while ((byteRead = fis.read()) != -1) {
                    sum += byteRead;
                }
                return sum;
            }
       	    
        } else {
            throw new RuntimeException("Non-regular file: " + path);
        }
    
    }
}
}

