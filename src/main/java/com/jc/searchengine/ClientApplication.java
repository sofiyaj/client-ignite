package com.jc.searchengine;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.cache.Cache;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryUpdatedListener;

/**
 * @Author: wangjie
 * @Description:
 * @Date: Created in 10:26 2018/3/27
 */
@SpringBootApplication
public class ClientApplication {

    //cache name
    private static final String CACHE_NAME = "serverCache";

    public static void main(String[] args) throws InterruptedException {

        SpringApplication.run(ClientApplication.class, args);


        try (Ignite ignite = Ignition.start("example-cache.xml")) {
            ignite.active(true);
            System.out.println("**********Cache continuous query example started**********");

            try (IgniteCache<Integer, String> cache = ignite.getOrCreateCache(CACHE_NAME)) {

                // Create new continuous query.
                ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();

                qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
                    @Override
                    public boolean apply(Integer key, String val) {
                        return key > 0;
                    }
                }));

                qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
                    @Override
                    public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {
                        for (CacheEntryEvent<? extends Integer, ? extends String> e : evts) {
                            System.out.println("Updated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                        }
                    }
                });


                try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
                    // Iterate through existing data.
                    for (Cache.Entry<Integer, String> e : cur) {
                        System.out.println("Queried existing entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                        Thread.sleep(2000000000);
                    }
                } finally {
                    ignite.destroyCache(CACHE_NAME);
                }

            }

        }
    }
}


