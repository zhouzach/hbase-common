package org.rabbit;

import ch.hsr.geohash.GeoHash;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.awt.geom.Point2D;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;

public class QueryMatch {
    public String uid;
    public String hash;
    public double lon, lat;
    public double distance = Double.NaN;

    @Override
    public String toString() {
        return "QueryMatch{" +
                "uid='" + uid + '\'' +
                ", hash='" + hash + '\'' +
                ", lon=" + lon +
                ", lat=" + lat +
                ", distance=" + distance +
                '}';
    }

    public QueryMatch(String uid, String hash, double lon, double lat, double distance) {
        this.uid = uid;
        this.hash = hash;
        this.lon = lon;
        this.lat = lat;
        this.distance = distance;
    }

    public Collection<QueryMatch> takeN(Comparator<QueryMatch> comp,
                                        String prefix, int n
    ) throws IOException {
        Collection<QueryMatch> candidates = MinMaxPriorityQueue.orderedBy(comp).maximumSize(n).create();

        String tablename = "ods:anchor_location";
        String family = "cf";
        TableName userTable = TableName.valueOf(tablename);
        try {
            Configuration conf = HBaseConfiguration.create();
            Connection connection = ConnectionFactory.createConnection(conf);

            Table table = connection.getTable(userTable);

            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(prefix));
//            scan.setFilter(new PrefixFilter(Bytes.toBytes(prefix)));

            scan.addFamily(family.getBytes());
            scan.setCaching(50);


            ResultScanner scanner = table.getScanner(scan);
            Point2D orign = new Point2D.Double(lon,lat );
            for (Result r : scanner) {
                String hash = Bytes.toString(r.getRow());
                long uid = Bytes.toLong(r.getValue(family.getBytes(), "uid".getBytes()));
                double longitude = Bytes.toDouble(r.getValue(family.getBytes(), "longitude".getBytes()));
                double latitude = Bytes.toDouble(r.getValue(family.getBytes(), "latitude".getBytes()));
                String tag1 = new String(r.getValue(family.getBytes(), "tag1".getBytes()));

                candidates.add(new QueryMatch(String.valueOf(uid), hash, longitude, latitude, orign.distance(longitude,latitude)));
            }
            table.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return candidates;
    }

    public Collection<QueryMatch> query(int n) throws IOException {

        DistanceComparator comp = new DistanceComparator(lon, lat);
        Collection<QueryMatch> ret = MinMaxPriorityQueue.orderedBy(comp).maximumSize(n).create();

        GeoHash target = GeoHash.withCharacterPrecision(lat, lon, 1);
        System.out.println("prefix: " + target.toBase32());
        ret.addAll(takeN(comp, target.toBase32(), n));

        for (GeoHash h : target.getAdjacent()) {
            ret.addAll(takeN(comp, h.toBase32(), n));
        }
        return ret;
    }

    public static void main(String[] args) throws IOException {
        String uid = "9";
        String hash = "ek0z635rq30e45C48C";
        QueryMatch queryMatch = new QueryMatch(uid, hash, -32.59170314251077, 23.78115770591269,0.0);

        queryMatch.query(5)
                .forEach(System.out::println);
    }
}
