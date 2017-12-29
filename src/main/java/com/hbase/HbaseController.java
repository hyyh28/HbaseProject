package com.hbase;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class HbaseController
{
     static Configuration hbaseConnection()
    {
        Configuration config = HBaseConfiguration.create();
        config.addResource(new Path(System.getenv("HBASE_CONF_DIR"), "hbase-site.xml"));
        config.addResource(new Path(System.getenv("HADOOP_CONF_DIR"), "core-site.xml"));
        return config;
    }

    static void hbaseCreateCar(Configuration config)
    {
        String[] carTableName = {"buying", "maint", "doors", "persons", "lug_boot", "safety", "isAcc"};
        Admin admin;
        Connection connection;
        String tableCar = "CarData";
        TableName tableName = TableName.valueOf(tableCar);
        try
        {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableCar));
        for(int i = 0; i < carTableName.length; i++)
        {
            table.addFamily(new HColumnDescriptor(carTableName[i]));
        }
        System.out.println("Table has been created");
        ArrayList<String[]> myData = DataProcessing.ProcessCarData();
        System.out.println("Start inserting data");
        for(int i = 0; i < myData.size(); i++)
        {
            String rowKey = "aabbcc" + i;
            Put put = new Put(rowKey.getBytes());
            for(int j = 0; j < myData.get(i).length; j++)
            {
                put.add(carTableName[i].getBytes(), null, myData.get(i)[j].getBytes());
            }
        }
        System.out.println("Successfully added Data Car");
    }

    static void hbaseCreateIris(Configuration config)
    {
        String[] irisName = {"sepal length", "sepal width", "petal length", "petal width", "type"};
        Admin admin;
        Connection connection;
        String tableIris = "IrisData";
        TableName tableName = TableName.valueOf(tableIris);
        try
        {
            connection = ConnectionFactory.createConnection(config);
            admin = connection.getAdmin();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableIris));
        for(int i = 0; i < irisName.length; i++)
        {
            table.addFamily(new HColumnDescriptor(irisName[i]));
        }
        System.out.println("Table has been created");
        ArrayList<String[]> myData = DataProcessing.ProcessIrisData();
        System.out.println("Start inserting data");
        for(int i = 0; i < myData.size(); i++)
        {
            String rowKey = "ddeeff" + i;
            Put put = new Put(rowKey.getBytes());
            for(int j = 0; j < myData.get(i).length; j++)
            {
                put.add(irisName[i].getBytes(), null, myData.get(i)[j].getBytes());
            }
        }
        System.out.println("Successfully added Data Car");
    }
    public static void main(String[] args)
    {
        Configuration configuration = hbaseConnection();
        hbaseCreateCar(configuration);
        hbaseCreateIris(configuration);
        /*ArrayList<String[]> result = DataProcessing.ProcessIrisData();
        System.out.println(result.size());
        for(int i = 0; i < result.size(); i++)
        {
            for(int j = 0; j < 5; j++)
            {
                System.out.print(result.get(i)[j]+" ");
            }
            System.out.print('\n');
        }
        System.out.println(result.size());*/
    }
}
