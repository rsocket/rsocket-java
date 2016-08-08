package io.reactivesocket.examples.kafkaserver;

/**
 * Simple implementation of a tuple
 * @param <K>
 * @param <V>
 */
public class Tuple<K, V> {

    private K k;
    private V v;

    public Tuple(K k, V v) {
        this.k = k;
        this.v = v;
    }

    /**
     * Returns K
     * @return K
     */
    public K getK() {
        return this.k;
    }

    /**
     * Returns V
     * @return V
     */
    public V getV() {
        return this.v;
    }
}
