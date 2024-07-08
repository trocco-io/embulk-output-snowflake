package org.embulk.output.snowflake;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Test class for MemoryAwareCachedThreadPoolExecutor. */
public class MemoryAwareCachedThreadPoolExecutorTest {

  private MemoryAwareCachedThreadPoolExecutor executor;
  private MemoryMXBean memoryMXBean;
  private MemoryUsage memoryUsageHigh;
  private MemoryUsage memoryUsageLow;

  @BeforeEach
  public void setUp() {
    memoryMXBean = mock(MemoryMXBean.class);
    memoryUsageHigh = mock(MemoryUsage.class);
    memoryUsageLow = mock(MemoryUsage.class);

    long maxMemory = Runtime.getRuntime().maxMemory();
    long usedMemoryHigh = (long) (maxMemory * 0.85); // Simulate 85% usage
    long usedMemoryLow = (long) (maxMemory * 0.1); // Simulate 10% usage

    when(memoryUsageHigh.getUsed()).thenReturn(usedMemoryHigh);
    when(memoryUsageHigh.getMax()).thenReturn(maxMemory);

    when(memoryUsageLow.getUsed()).thenReturn(usedMemoryLow);
    when(memoryUsageLow.getMax()).thenReturn(maxMemory);

    // Set initial state to return memoryUsageHigh
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);

    executor = new MemoryAwareCachedThreadPoolExecutor(memoryMXBean);
  }

  /**
   * Test the execute method with low memory usage.
   *
   * <p>This test verifies that a task is executed immediately when the memory usage is below the
   * threshold.
   */
  @Test
  public void testExecuteWithLowMemoryUsage() throws InterruptedException {
    // Set initial state to return memoryUsageLow
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);

    CountDownLatch latch = new CountDownLatch(1);

    executor.execute(latch::countDown);

    assertTrue(
        latch.await(1, TimeUnit.SECONDS),
        "Task should be executed immediately due to low memory usage");

    verify(memoryMXBean, atLeastOnce()).getHeapMemoryUsage();
  }

  /**
   * Test the execute method with high memory usage.
   *
   * <p>This test verifies that a task is queued when the memory usage is above the threshold, and
   * it is executed later when the memory usage drops below the threshold.
   */
  @Test
  public void testExecuteWithHighMemoryUsage() throws InterruptedException {
    // Set initial state to return memoryUsageHigh
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);

    CountDownLatch latch = new CountDownLatch(1);

    // Verify initial memory usage is not null before executing the first task
    assertNotNull(memoryMXBean.getHeapMemoryUsage(), "Initial memory usage should not be null");

    new Thread(
            () -> {
              try {
                Thread.sleep(1000);
                when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);
                synchronized (executor) {
                  executor.notifyAll();
                }
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            })
        .start();

    executor.execute(latch::countDown);

    assertFalse(
        latch.await(1, TimeUnit.SECONDS), "Task should be delayed due to high memory usage");

    assertTrue(
        latch.await(10, TimeUnit.SECONDS), "Task should be executed after memory usage drops");

    verify(memoryMXBean, atLeastOnce()).getHeapMemoryUsage();
  }

  /**
   * Test the hasPendingTasks method.
   *
   * <p>This test verifies that the hasPendingTasks method correctly identifies if there are pending
   * tasks.
   */
  @Test
  public void testHasPendingTasks()
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);

    executor.execute(latch1::countDown);
    executor.execute(latch2::countDown);

    assertTrue(
        executor.hasPendingTasks(), "Executor should have pending tasks due to high memory usage");

    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);
    Method processPendingTasks =
        MemoryAwareCachedThreadPoolExecutor.class.getDeclaredMethod("processPendingTasks");
    processPendingTasks.setAccessible(true);
    processPendingTasks.invoke(executor);

    assertFalse(
        executor.hasPendingTasks(), "Executor should have no pending tasks after processing");
  }

  /**
   * Test the processPendingTasks method.
   *
   * <p>This test verifies that pending tasks are processed and executed when the memory usage drops
   * below the threshold.
   */
  @Test
  public void testProcessPendingTasks()
      throws InterruptedException, NoSuchMethodException, InvocationTargetException,
          IllegalAccessException {
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);
    CountDownLatch latch = new CountDownLatch(1);

    executor.execute(latch::countDown);
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);

    Method processPendingTasks =
        MemoryAwareCachedThreadPoolExecutor.class.getDeclaredMethod("processPendingTasks");
    processPendingTasks.setAccessible(true);
    processPendingTasks.invoke(executor);

    assertTrue(latch.await(1, TimeUnit.SECONDS), "Pending tasks should be processed");
  }

  /**
   * Test the processPendingTasks method to verify it waits until memory usage drops below the
   * threshold.
   *
   * <p>This test verifies that the processPendingTasks method waits until the memory usage drops
   * below the threshold before processing the pending tasks.
   */
  @Test
  public void testProcessPendingTasksWaitForMemoryThreshold()
      throws InterruptedException, Exception {
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);
    CountDownLatch latch = new CountDownLatch(1);

    executor.execute(latch::countDown);

    new Thread(
            () -> {
              try {
                Thread.sleep(3000); // Simulate delay before memory usage drops
                when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            })
        .start();

    Method processPendingTasks =
        MemoryAwareCachedThreadPoolExecutor.class.getDeclaredMethod("processPendingTasks");
    processPendingTasks.setAccessible(true);

    long startTime = System.nanoTime();
    processPendingTasks.invoke(executor);
    long duration = System.nanoTime() - startTime;

    assertTrue(
        latch.await(1, TimeUnit.SECONDS),
        "Pending tasks should be processed after memory usage drops");
    assertTrue(
        duration >= TimeUnit.SECONDS.toNanos(3),
        "processPendingTasks should wait until memory usage drops below the threshold");
  }

  /**
   * Test the processPendingTasks method to ensure pendingTasks queue is emptied.
   *
   * <p>This test verifies that the processPendingTasks method processes all pending tasks until the
   * queue is empty.
   */
  @Test
  public void testProcessPendingTasksEmptyQueue() throws InterruptedException, Exception {
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);

    executor.execute(latch1::countDown);
    executor.execute(latch2::countDown);
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);

    Method processPendingTasks =
        MemoryAwareCachedThreadPoolExecutor.class.getDeclaredMethod("processPendingTasks");
    processPendingTasks.setAccessible(true);
    processPendingTasks.invoke(executor);

    assertTrue(latch1.await(1, TimeUnit.SECONDS), "First pending task should be processed");
    assertTrue(latch2.await(1, TimeUnit.SECONDS), "Second pending task should be processed");
    assertFalse(executor.hasPendingTasks(), "Pending tasks queue should be empty");
  }

  /**
   * Test the awaitTermination method.
   *
   * <p>This test verifies that the executor waits for the specified time for all tasks to complete
   * after a shutdown request.
   */
  @Test
  public void testAwaitTermination() throws InterruptedException, Exception {
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);

    executor.execute(latch1::countDown);
    executor.execute(latch2::countDown);

    new Thread(
            () -> {
              try {
                Thread.sleep(5000); // Simulate delay before memory usage drops
                when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            })
        .start();

    executor.shutdown();

    assertTrue(
        executor.awaitTermination(60, TimeUnit.SECONDS),
        "Executor should terminate within the timeout");
    assertTrue(latch1.await(1, TimeUnit.SECONDS), "First pending task should be processed");
    assertTrue(latch2.await(1, TimeUnit.SECONDS), "Second pending task should be processed");
    assertFalse(
        executor.hasPendingTasks(), "Pending tasks queue should be empty after termination");
  }

  /**
   * Test the shutdownNow method with immediate shutdown.
   *
   * <p>This test verifies that the executor immediately stops all actively executing tasks and
   * halts the processing of waiting tasks, returning a list of the tasks that were awaiting
   * execution.
   */
  @Test
  public void testShutdownNowImmediate() throws InterruptedException {
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);
    CountDownLatch latch3 = new CountDownLatch(1);

    Runnable task1 = latch1::countDown;
    Runnable task2 = latch2::countDown;
    Runnable task3 = latch3::countDown;

    executor.execute(task1);
    executor.execute(task2);
    executor.execute(task3);

    List<Runnable> pendingTasks = executor.shutdownNow();

    // latch1, latch2, and latch3 should not be executed because memory usage is high and
    // shutdownNow is called immediately
    assertFalse(
        latch1.await(1, TimeUnit.SECONDS),
        "First task should not be executed due to immediate shutdown");
    assertFalse(
        latch2.await(1, TimeUnit.SECONDS),
        "Second task should not be executed due to immediate shutdown");
    assertFalse(
        latch3.await(1, TimeUnit.SECONDS),
        "Third task should not be executed due to immediate shutdown");

    // pendingTasks should contain task1, task2, and task3
    assertEquals(3, pendingTasks.size(), "Three tasks should be pending after shutdownNow");
    assertTrue(pendingTasks.contains(task1), "Pending tasks should include the first task");
    assertTrue(pendingTasks.contains(task2), "Pending tasks should include the second task");
    assertTrue(pendingTasks.contains(task3), "Pending tasks should include the third task");

    assertFalse(
        executor.hasPendingTasks(), "Pending tasks queue should be empty after shutdownNow");
  }

  /**
   * Test the shutdown method.
   *
   * <p>This test verifies that the executor processes pending tasks and waits for the specified
   * time for all tasks to complete after a shutdown request.
   */
  @Test
  public void testShutdown() throws InterruptedException {
    when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageHigh);
    CountDownLatch latch1 = new CountDownLatch(1);
    CountDownLatch latch2 = new CountDownLatch(1);

    executor.execute(latch1::countDown);
    executor.execute(latch2::countDown);

    new Thread(
            () -> {
              try {
                Thread.sleep(5000); // Simulate delay before memory usage drops
                when(memoryMXBean.getHeapMemoryUsage()).thenReturn(memoryUsageLow);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            })
        .start();

    executor.shutdown();

    assertTrue(
        executor.awaitTermination(60, TimeUnit.SECONDS),
        "Executor should terminate within the timeout");
    assertTrue(latch1.await(1, TimeUnit.SECONDS), "Task should be executed and completed");
    assertTrue(latch2.await(1, TimeUnit.SECONDS), "Task should be executed and completed");
    assertFalse(executor.hasPendingTasks(), "Pending tasks queue should be empty after shutdown");
  }
}
