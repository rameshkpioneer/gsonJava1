package com.ci.test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.*;


public class ReadingCSVFile {

    BufferedReader br = null;
    String line = "";
    String cvsSplitBy = ",";

    public ReadingCSVFile(){

    }
    public static void main(String args[]) throws  IOException{
        ReadingCSVFile rsRead = new ReadingCSVFile();
             rsRead.testAnother();
    }

      public void testAnother() throws FileNotFoundException,IOException{
          int i =0;
          String s="";
          String s1="";
          String s2 ="";
          List<List<String>> records = new ArrayList<>();
          try (BufferedReader br = new BufferedReader(new FileReader("D:/CustomeIntegrations/top10records.csv"))) {
              String line;
              while ((line = br.readLine()) != null) {
                  String[] values = line.split(cvsSplitBy);
                 // System.out.println("Values "+values.length);
                   records.add(Arrays.asList(values));
            }

          List<String> entites = new ArrayList<String>();
              String testValue="";
          for( i =0 ;i<records.size(); i++){
                List<String> li = records.get(i);
                List<String> liFirst = records.get(0);
                for(int j =0; j<li.size();j++){
                    //TO skip Adding Headers
               if( i >0){
                     String quotes= "\"";
                     String key = quotes+liFirst.get(j)+quotes;
                     //  li.get(j).indexOf()
                     String values = quotes+li.get(j).replace("\"","\\\"")+quotes;
                     s2=s2+ s.concat(key+": {").concat("\"values\":" +"[ {"+"\"source \": "+"\"internal\",").concat("\"locale\" : \"en-us\", ").concat("\"value\":"+values).concat("} ] } ,");
                 }
             }

             if(i==0){
                 continue;
             }else{
                 s2= s1.concat("{").concat("\"attributes\" : {").concat(s2);
                 // System.out.println("Enitites "+entites.toString());
                 s2=s2.substring(0, s2.length() - 1).concat("}}");
              }
              entites.add(s2);
             s2="";
          }
              System.out.println("Testing "+ testValue);
           //   System.out.println("Enitites "+entites.toString());
              String s3="{\"entites\": "+entites.toString()+"}";
              System.out.println("s3" +s3);

          }
          catch(Exception e){
              e.printStackTrace();
          }
      }

}
