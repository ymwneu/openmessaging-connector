package io.openmessaging.connect.runtime.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CountDownLatch2Test {

    private CountDownLatch2 countDownLatch2 = new CountDownLatch2(1);

    @Test
    public void testAwaitResetCountDown() throws Exception {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    countDownLatch2.await();
                    Thread.currentThread().sleep(100);
                    countDownLatch2.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
        Thread.currentThread().sleep(10);
        assertEquals(Thread.State.WAITING, thread.getState());
        countDownLatch2.countDown();
        Thread.currentThread().sleep(10);
        assertEquals(Thread.State.TIMED_WAITING, thread.getState());
        countDownLatch2.reset();
        Thread.sleep(200);
        assertEquals(Thread.State.WAITING, thread.getState());
        countDownLatch2.countDown();
        Thread.currentThread().sleep(100);
        assertEquals(Thread.State.TERMINATED, thread.getState());
    }
}