package com.hbase;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class DataProcessing
{
    public static ArrayList<String[]> ProcessCarData()
    {
        ArrayList<String[]> resultList = new ArrayList<>();
        FileReader fr;
        BufferedReader br;
        try
        {
            fr = new FileReader("./src/main/resources/car.data");
            br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null)
            {
                String[] dataLine = line.split(",");
                resultList.add(dataLine);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return resultList;
    }

    public static ArrayList<String[]> ProcessIrisData()
    {
        ArrayList<String[]> resultList = new ArrayList<>();
        FileReader fr;
        BufferedReader br;
        try
        {
            fr = new FileReader("./src/main/resources/Iris.data");
            br = new BufferedReader(fr);
            String line;
            while((line = br.readLine()) != null)
            {
                String[] dataLine = line.split(",");
                resultList.add(dataLine);
            }
        }
        catch (FileNotFoundException e)
        {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultList;
    }
}
