package textfile.utils;

public class StopWatch {
    
    private long startTime = 0;
    private long stopTime = 0;
    private boolean running = false;

    
    public void start() {
        this.startTime = System.nanoTime();
        this.running = true;
    }

    
    public void stop() {
        this.stopTime = System.nanoTime();
        this.running = false;
    }

    
    //elaspsed time in milliseconds
    public long getElapsedTime() {
        long elapsed;
        if (running) {
             elapsed = (System.nanoTime() - startTime);
        }
        else {
            elapsed = (stopTime - startTime);
        }
        return elapsed;
    }
    
    
    //elaspsed time in seconds
    public long getElapsedTimeSecs() {
        long elapsed;
        if (running) {
            elapsed = (long) ((System.nanoTime() - startTime) / 1000000000.0);
        }
        else {
            elapsed = (long) ((stopTime - startTime) / 1000000000.0);
        }
        return elapsed;
    }
}
