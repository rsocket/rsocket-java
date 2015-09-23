package io.reactivesocket.aeron.internal;

/**
 * Creates a pair, with one value an int and the other an Object.
 */
public class Int2ObjectPair<T> {
    private int i;
    private T t;

    private Int2ObjectPair(int i, T t) {
        this.i = i;
        this.t = t;
    }

    public static <T> Int2ObjectPair pairOf(int i, T t) {
        return new Int2ObjectPair(i, t);
    }

    public int getInt() {
        return i;
    }

    public T getObject() {
        return t;
    }
}
