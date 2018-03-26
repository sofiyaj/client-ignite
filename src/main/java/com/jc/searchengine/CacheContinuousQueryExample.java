package com.jc.searchengine;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;

/**
 * @Author: wangjie
 * @Description:
 * @Date: Created in 15:23 2018/3/23
 */
public class CacheContinuousQueryExample {

    //Cache name 全局变量
    private static final String CACHE_NAME = CacheContinuousQueryExample.class.getSimpleName();

    public static void main(String[] args) throws InterruptedException {

        try (Ignite ignite = Ignition.start()) {

            System.out.println(CACHE_NAME.toString());
            System.out.println(">>> Cache continuous query example started <<<");

            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {
                int keyCnt = 20;

                for (int i = 0; i < keyCnt; i++) {
                    cache.put(i, Integer.toString(i));
                }

                // Create new continuous query.
                ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();
                qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
                    @Override public boolean apply(Integer key, String val) {
                        return key > 10;
                    }
                }));
                System.out.println("init query is ok!");


                // Callback that is called locally when update notifications are received.
                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                    @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {
                        for (CacheEntryEvent<? extends Integer, ? extends String> e : evts) {
                            System.out.println("Updated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                        }
                    }
                });

                //This filter will be evaluated remotely on all nodes,Entry that pass this filter will be sent to the caller.
                qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<Integer, String>>() {
                    @Override public CacheEntryEventFilter<Integer, String> create() {
                        return new CacheEntryEventFilter<Integer, String>() {
                            @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e) {
                                return e.getKey() > 10;
                            }
                        };
                    }
                });


                try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                    // Iterate through existing data.
                    for (Cache.Entry<Integer, String> e : cur) {
                        System.out.println("Queried existing entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                    }

            }
            finally {
                ignite.destroyCache(CACHE_NAME);
            }



            }


            }

        }



}
