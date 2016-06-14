package io.reactivesocket.client.stat;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

public class MedianTest {
    private double errorSum = 0;
    private double maxError = 0;
    private double minError = 0;

    @Test
    public void testMedian() {
        Random rng = new Random("Repeatable tests".hashCode());
        int n = 1;
        for (int i = 0; i < n; i++) {
            testMedian(rng);
        }
        System.out.println("Error avg = " + (errorSum/n)
            + " in range [" + minError + ", " + maxError + "]");
    }

    /**
     * Test Median estimation with normal random data
     */
    private void testMedian(Random rng) {
        int n = 100 * 1024;
        int range = Integer.MAX_VALUE >> 16;
        Median m = new Median();

        int[] data = new int[n];
        for (int i = 0; i < data.length; i++) {
            int x = Math.max(0, range/2 + (int) (range/5 * rng.nextGaussian()));
            data[i] = x;
            m.insert(x);
        }
        Arrays.sort(data);

        int expected = data[data.length / 2];
        double estimation = m.estimation();
        double error = Math.abs(expected - estimation) / expected;

        errorSum += error;
        maxError = Math.max(maxError, error);
        minError = Math.min(minError, error);

        Assert.assertTrue("p50=" + estimation + ", real=" + expected
            + ", error=" + error, error < 0.02);
    }
}