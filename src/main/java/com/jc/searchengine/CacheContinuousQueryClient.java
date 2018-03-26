package com.jc.searchengine;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryUpdatedListener;
import java.util.Arrays;

/**
 * @Author: wangjie
 * @Description:
 * @Date: Created in 18:49 2018/3/23
 */
public class CacheContinuousQueryClient {
    public static void main(String[] args) throws InterruptedException {
        //此节点配置为客户端
        Ignition.setClientMode(true);
        IgniteConfiguration cfg = new IgniteConfiguration();
        TcpCommunicationSpi commSpi = new TcpCommunicationSpi();
        commSpi.setSlowClientQueueLimit(1000);
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipfinder = new TcpDiscoveryVmIpFinder();
        ipfinder.setAddresses(Arrays.asList("localhost"));
        discoverySpi.setIpFinder(ipfinder);
        cfg.setCommunicationSpi(commSpi);
        cfg.setDiscoverySpi(discoverySpi);

        Ignite ignite = Ignition.start("example-cache.xml");
        ClusterGroup clusterGroup = ignite.cluster().forClients();
        IgniteCache<Integer, String> cache = ignite.getOrCreateCache("serverCache");
        IgniteCompute compute = ignite.compute(clusterGroup);
        compute.broadcast(() -> {
            for (int i = 0; i < 10  ; i++) {
                System.out.println("******************" + cache.get(i) + "******************");
            }
        });


        ContinuousQuery<Integer, String> qry = new ContinuousQuery<>();
        qry.setInitialQuery(new ScanQuery<>(new IgniteBiPredicate<Integer, String>() {
            @Override public boolean apply(Integer key, String val) {
                return key > 10;
            }
        }));

        System.out.println("init query is ok!");

       qry.setLocalListener(new CacheEntryUpdatedListener<Integer, String>() {
            @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends String>> evts) {
                for (CacheEntryEvent<? extends Integer, ? extends String> e : evts) {
                    System.out.println("Updated entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
                }
            }
        });
        System.out.println("local lisetner created  ****************");

        qry.setRemoteFilterFactory(new Factory<CacheEntryEventFilter<Integer, String>>() {
            @Override public CacheEntryEventFilter<Integer, String> create() {
                return new CacheEntryEventFilter<Integer, String>() {
                    @Override public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends String> e) {
                        return e.getKey() > 10;
                    }
                };
            }
        });
        System.out.println("set remote filter factory succeed!");


        try (QueryCursor<Cache.Entry<Integer, String>> cur = cache.query(qry)) {
            // Iterate through existing data.
            for (Cache.Entry<Integer, String> e : cur) {
                System.out.println("Queried existing entry [key=" + e.getKey() + ", val=" + e.getValue() + ']');
            }
            Thread.sleep(2000000000);
        }
        finally {
            ignite.destroyCache("serverCache");
        }


    }
}
