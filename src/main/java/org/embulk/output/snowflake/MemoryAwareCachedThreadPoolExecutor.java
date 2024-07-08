package org.embulk.output.snowflake;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ThreadPoolExecutor} that limits the number of concurrently running tasks based on the
 * memory usage.
 *
 * <p>The executor will not accept new tasks if the memory usage exceeds the threshold. It will
 * queue the tasks and wait until the memory usage falls below the threshold to execute them.
 *
 * <p>This executor mimics the behavior of {@link Executors#newCachedThreadPool()} but adds memory
 * usage awareness.
 *
 * <p>It has a core pool size of 0, a maximum pool size of Integer.MAX_VALUE, and a keep-alive time
 * of 60 seconds for idle threads. It uses a {@link SynchronousQueue} for task queuing.
 */
public class MemoryAwareCachedThreadPoolExecutor extends ThreadPoolExecutor {

  private final Logger logger = LoggerFactory.getLogger(MemoryAwareCachedThreadPoolExecutor.class);
  private static final double MEMORY_THRESHOLD = 0.8; // Memory usage threshold (80%)
  private static final int SCHEDULER_THREAD_COUNT = 1; // Number of threads in the scheduler
  private final MemoryMXBean memoryMXBean;
  private final BlockingQueue<Runnable> pendingTasks = new LinkedBlockingQueue<>();
  private final ScheduledExecutorService scheduler =
      Executors.newScheduledThreadPool(SCHEDULER_THREAD_COUNT);

  /**
   * Constructs a new MemoryAwareCachedThreadPoolExecutor with the same settings as {@link
   * Executors#newCachedThreadPool()}.
   *
   * <p>This executor has a core pool size of 0, a maximum pool size of Integer.MAX_VALUE, and a
   * keep-alive time of 60 seconds for idle threads. It uses a {@link SynchronousQueue} for task
   * queuing.
   */
  public MemoryAwareCachedThreadPoolExecutor() {
    super(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    this.memoryMXBean = ManagementFactory.getMemoryMXBean();
    startTaskProcessor();
  }

  /**
   * This constructor is intended for testing purposes only. It allows the injection of a mock
   * {@link MemoryMXBean}.
   */
  MemoryAwareCachedThreadPoolExecutor(MemoryMXBean memoryMXBean) {
    super(0, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    this.memoryMXBean = memoryMXBean;
    startTaskProcessor();
  }

  /**
   * Submits the given task for execution. If the memory usage exceeds the defined threshold, the
   * task is queued and will be executed when the memory usage falls below the threshold.
   *
   * @param command the task to execute
   */
  @Override
  public void execute(Runnable command) {
    if (getMemoryUsage() < MEMORY_THRESHOLD) {
      super.execute(command);
      logger.info("Memory usage is below the threshold, executing task immediately: {}", command);
    } else {
      pendingTasks.offer(command);
      logger.info(
          "Memory usage is above the threshold, queuing task for later execution: {}", command);
    }
  }

  /**
   * Initiates an orderly shutdown in which previously submitted tasks are executed, but no new
   * tasks will be accepted. Waits for the memory usage to drop below the threshold and processes
   * pending tasks before shutting down.
   *
   * <p>Note: This method may take longer than the standard ThreadPoolExecutor's shutdown method as
   * it waits for the memory usage to drop below the threshold before processing pending tasks. If
   * you need to shutdown immediately without waiting for pending tasks to execute, use {@link
   * #shutdownNow()} instead.
   */
  @Override
  public void shutdown() {
    try {
      processPendingTasks();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("Interrupted while processing pending tasks during shutdown.", e);
    }
    super.shutdown();
  }

  /**
   * Attempts to stop all actively executing tasks, halts the processing of waiting tasks, and
   * returns a list of the tasks that were awaiting execution. Does not process pending tasks before
   * shutting down.
   *
   * <p>Note: Tasks that are in the pending queue (waiting to be executed due to high memory usage)
   * will not be executed and will be included in the returned list.
   *
   * @return the list of tasks that never commenced execution.
   */
  @Override
  public List<Runnable> shutdownNow() {
    List<Runnable> tasks = super.shutdownNow();
    tasks.addAll(pendingTasks);
    pendingTasks.clear();
    return tasks;
  }

  /**
   * Blocks until all tasks have completed execution after a shutdown request, or the timeout
   * occurs, or the current thread is interrupted, whichever happens first. Uses the {@link
   * ThreadPoolExecutor#awaitTermination(long, TimeUnit)} implementation.
   *
   * <p>Note: This method relies on {@link #shutdown()} or {@link #shutdownNow()} being called
   * before it is invoked. It does not call {@code processPendingTasks()}, so any tasks added to
   * {@code pendingTasks} after {@code shutdown()} or {@code shutdownNow()} are ignored.
   *
   * @param timeout the maximum time to wait.
   * @param unit the time unit of the timeout argument.
   * @return {@code true} if this executor terminated and {@code false} if the timeout elapsed
   *     before termination.
   * @throws InterruptedException if interrupted while waiting.
   */
  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return super.awaitTermination(timeout, unit);
  }

  /**
   * Checks if there are any pending tasks in the queue.
   *
   * @return {@code true} if there are pending tasks in the queue, {@code false} otherwise.
   */
  public boolean hasPendingTasks() {
    return !pendingTasks.isEmpty();
  }

  /** Processes the pending tasks if the memory usage is below the threshold. */
  private void processPendingTasks() throws InterruptedException {
    while (!pendingTasks.isEmpty()) {
      if (getMemoryUsage() < MEMORY_THRESHOLD) {
        Runnable task = pendingTasks.poll();
        if (task == null) {
          break;
        }
        super.execute(task);
        logger.info("Memory usage is below the threshold, executing queued task: {}", task);
      } else {
        Thread.sleep(1000);
      }
    }
  }

  /**
   * Starts a background task that processes pending tasks when memory usage is below the threshold.
   * The task processor runs periodically to check the memory usage and execute queued tasks.
   */
  private void startTaskProcessor() {
    scheduler.scheduleAtFixedRate(
        () -> {
          try {
            while (getMemoryUsage() < MEMORY_THRESHOLD) {
              Runnable task = pendingTasks.poll();
              if (task == null) {
                break;
              }
              super.execute(task);
              logger.info("Memory usage is below the threshold, executing queued task: {}", task);
            }
          } catch (RejectedExecutionException e) {
            logger.error("Task execution was rejected.", e);
          }
        },
        0,
        500,
        TimeUnit.MILLISECONDS);
  }

  /**
   * Gets the current memory usage as a fraction of the maximum heap memory.
   *
   * @return the current memory usage ratio
   */
  private double getMemoryUsage() {
    MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
    long usedMemory = heapMemoryUsage.getUsed();
    long maxMemory = heapMemoryUsage.getMax();
    return (double) usedMemory / maxMemory;
  }
}
