package  com.jc.searchengine;

import com.jc.searchengine.po.Person;
import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.lang.IgnitePredicate2X;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import javax.cache.Cache;
import java.time.Period;
import java.util.Arrays;

/**
 * @Author: wangjie
 * @Description:
 * @Date: Created in 10:55 2018/3/23
 */

public class Application{
    public static void main(String[] args) {

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

        Ignite ignite = Ignition.start(cfg);

        ClusterGroup clusterGroup = ignite.cluster().forClients();

      IgniteCache<Long,Person> cache = ignite.getOrCreateCache("serverCache");
        IgniteCompute compute = ignite.compute(clusterGroup);
            compute.broadcast(() -> {
                Person s1 = cache.get(1L);
                Person s2 = cache.get(2L);
                Person s3 = cache.get(3L);
                System.out.println(s1.toString() + " " + s2.toString() + " " + s3.toString());
            });

    }

}