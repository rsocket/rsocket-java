package io.reactivesocket.tckdrivers.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.IterableSerializer;
import io.reactivesocket.Payload;
import io.reactivesocket.util.PayloadImpl;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rx.Observable;
import rx.RxReactiveStreams;
import rx.functions.Func1;
import rx.observables.AsyncOnSubscribe;
import rx.observables.SyncOnSubscribe;
import rx.schedulers.Schedulers;
import rx.subjects.*;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class MarblePublisher implements Publisher<Payload> {

    private Publisher<Payload> pub;
    private ReplaySubject<Observable<Payload>> ps;
    private Map<String, Map<String, String>> argMap;

    public MarblePublisher() {
        this.ps = ReplaySubject.<Observable<Payload>>create();
        Observable<Payload> outputToNetwork = Observable.<Payload>concat(ps.asObservable()).onBackpressureBuffer()
                .subscribeOn(Schedulers.io());

        this.pub = RxReactiveStreams.toPublisher(outputToNetwork);
    }

    @Override
    public void subscribe(Subscriber<? super Payload> s) {
        this.pub.subscribe(s);
        //this.pub2.subscribe(s);
        // TODO: Is there a cleaner way to do this?
        while (!this.ps.hasObservers()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Add part of a marble diagram to this. We want to remove the "-" characters since they are useless
     * This should stage the data to be sent.
     * @param marble the marble diagram string
     */
    public void add(String marble) {
        if (marble.contains("&&")) {
            String[] temp = marble.split("&&");
            marble = temp[0];
            ObjectMapper mapper = new ObjectMapper();
            try {
                argMap = mapper.readValue(temp[1], new TypeReference<Map<String, Map<String, String>>>() {
                });
            } catch (Exception e) {
                System.out.println("couldn't convert argmap");
            }
        }
        if (marble.contains("|")  || marble.contains("#")) {
            ps.onNext(createObservable(marble));
        } else {
            ps.onNext(createHotObservable(marble));
        }
    }

    /**
     * This function uses the dictionary and the marble string to create the Observable that specifies the marble
     * behavior. We remove the '-' characters because they don't matter in reactivesocket.
     * @param marble
     * @return an Obervable that does whatever the marble does
     */
    private List<Payload> createList(String marble) {
        Queue<Character> marb = new ConcurrentLinkedQueue<>();
        List<Payload> toReturn = new ArrayList<>();
        for (char c : marble.toCharArray()) {
            if (c != '-') {
                switch (c) {
                    case '|':
                        break;
                    case '#':
                        break;
                    default:
                        if (argMap != null) {
                            // this is hacky, but we only expect one key and one value
                            Map<String, String> tempMap = argMap.get(c + "");
                            if (tempMap == null) {
                                toReturn.add(new PayloadImpl(c + "", c + ""));
                                break;
                            }
                            List<String> key = new ArrayList<>(tempMap.keySet());
                            List<String> value = new ArrayList<>(tempMap.values());
                            toReturn.add(new PayloadImpl(key.get(0), value.get(0)));
                        } else {
                            toReturn.add(new PayloadImpl(c + "", c + ""));
                        }

                        break;
                }
            }
        }
        return toReturn;
    }

    /**
     * This function seeks to create a more complex publisher behavior
     * @param marble
     * @return
     */
    private Observable<Payload> createObservable(String marble) {
        return Observable.create(SyncOnSubscribe.<Iterator<Character>, Payload>createStateful(
                () -> {
                    List<Character> list = new ArrayList<>();
                    for (char c : marble.toCharArray()) {
                        if (c != '-') list.add(c);
                    }
                    return list.iterator();
                },
                (state, sub) -> {
                    if (state.hasNext()) {
                        char c = state.next();
                        switch (c) {
                            case '|':
                                System.out.println("calling onComplete");
                                sub.onCompleted();
                                break;
                            case '#':
                                sub.onError(new Throwable());
                                break;
                            default:
                                if (argMap != null) {
                                    // this is hacky, but we only expect one key and one value
                                    Map<String, String> tempMap = argMap.get(c + "");
                                    if (tempMap == null) {
                                        sub.onNext(new PayloadImpl(c + "", c + ""));
                                        break;
                                    }
                                    List<String> key = new ArrayList<>(tempMap.keySet());
                                    List<String> value = new ArrayList<>(tempMap.values());
                                    sub.onNext(new PayloadImpl(key.get(0), value.get(0)));
                                } else {
                                    sub.onNext(new PayloadImpl(c + "", c + ""));
                                }

                                break;
                        }
                        return state;
                    }
                    System.out.println("calling onComplete");
                    sub.onCompleted();
                    return state;
                }
        )).retryWhen(new Func1<Observable<? extends Throwable>, Observable<?>>() {
            @Override
            public Observable<?> call(Observable<? extends Throwable> observable) {
                return Observable.empty();
            }
        });
    }

    private Observable<Payload> createHotObservable(String marble) {
        List<Character> list = new ArrayList<>();
        for (char c : marble.toCharArray()) {
            if (c != '-') list.add(c);
        }
        Iterator<Character> iter = list.iterator();
        return Observable.<Payload>create((rx.Subscriber<? super Payload> s) -> {
            while (iter.hasNext()) {
                char c = iter.next();
                if (argMap != null) {
                    // this is hacky, but we only expect one key and one value
                    Map<String, String> tempMap = argMap.get(c + "");
                    if (tempMap == null) {
                        s.onNext(new PayloadImpl(c + "", c + ""));
                        break;
                    }
                    List<String> key = new ArrayList<>(tempMap.keySet());
                    List<String> value = new ArrayList<>(tempMap.values());
                    s.onNext(new PayloadImpl(key.get(0), value.get(0)));
                } else {
                    s.onNext(new PayloadImpl(c + "", c + ""));
                }
            }
        }).subscribeOn(Schedulers.io());
    }

    private Observable<Payload> createAsyncObservable(String marble) {
        return Observable.create(AsyncOnSubscribe.<Iterator<Character>, Payload>createStateful(
                () -> {
                    List<Character> list = new ArrayList<>();
                    for (char c : marble.toCharArray()) {
                        if (c != '-') list.add(c);
                    }
                    return list.iterator();
                },
                (state, n, ob) -> {
                    if (state.hasNext()) {
                        char c = state.next();
                        switch (c) {
                            case '|':
                                ob.onCompleted();
                                break;
                            case '#':
                                ob.onError(new Throwable());
                                break;
                            default:
                                if (argMap != null) {
                                    // this is hacky, but we only expect one key and one value
                                    Map<String, String> tempMap = argMap.get(c + "");
                                    if (tempMap == null) {
                                        ob.onNext(Observable.just(new PayloadImpl(c + "", c + "")));
                                        break;
                                    }
                                    List<String> key = new ArrayList<>(tempMap.keySet());
                                    List<String> value = new ArrayList<>(tempMap.values());
                                    ob.onNext(Observable.just(new PayloadImpl(key.get(0), value.get(0))));
                                } else {
                                    ob.onNext(Observable.just(new PayloadImpl(c + "", c + "")));
                                }

                                break;
                        }
                    }
                    return state;
                }
        ));
    }
}
