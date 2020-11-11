package org.rabbit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import java.io.IOException;

public class HBaseApiHelper {

    public static void main(String[] args){

        Configuration conf = HBaseConfiguration.create();
        try {

            Connection connection = ConnectionFactory.createConnection(conf);
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public static void scan(Connection connection, String tablename, String family,String column,String prefix){
        TableName userTable = TableName.valueOf(tablename);

        try {

            Table table = connection.getTable(userTable);

            Scan scan = new Scan(prefix.getBytes());
            scan.setFilter(new PrefixFilter(prefix.getBytes()));

            scan.addFamily(family.getBytes());
            scan.setMaxVersions(1);
            scan.setCaching(50);

            ResultScanner scanner=table.getScanner(scan);

            Result result = scanner.next();


        }catch (IOException e){
            e.printStackTrace();
        }

    }
}
