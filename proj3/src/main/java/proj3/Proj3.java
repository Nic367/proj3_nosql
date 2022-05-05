/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package proj3;

import org.bson.Document;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.eq;
import java.util.ArrayList;
import java.util.Arrays;
import static com.mongodb.client.model.Aggregates.*;
import com.mongodb.client.model.Sorts;
import java.text.DecimalFormat;

/**
 *
 * @author nicole
 */
public class Proj3 {

    public static void main(String[] args) {
        //Connection to MongoDB Atlas
        MongoClient client = MongoClients.create("mongodb+srv://user3:pass3@cluster0.3bpui.mongodb.net/gaming?retryWrites=true&w=majority");
        MongoDatabase database = client.getDatabase("gaming");
        System.out.println();System.out.println();

        /******************************NICOLE******************************/
        /*QUESTION OF ANALYSIS: What genres are most popular to each given region?
        The way I went going about this was first getting a distinct list of genres then going through each 
        and summing up every regions total for that specific genre. After adding each document to my master
        array "all_arr" I went through and creating my graph for visual comparison before adding the max
        sales region per document to my "arr" array and listing out where each genre would most likely have
        the most sales based on 40 years of data*/
        MongoCollection<Document> collection = database.getCollection("globalvgsales");
              
        //gaming> db.globalvgsales.distinct("Genre")
        DistinctIterable<String> unique_genres = collection.distinct("Genre", String.class);
        /*[
            'Action',   'Adventure',
            'Fighting', 'Misc',
            'Platform', 'Puzzle',
            'Racing',   'Role-Playing',
            'Shooter',  'Simulation',
            'Sports',   'Strategy'
        ]*/
        if (unique_genres == null) {
            System.out.println("No Genres");
        }else{
            ArrayList<Document> arr = new ArrayList();
            ArrayList<Document> all_arr = new ArrayList();
            for(var ug : unique_genres) {
                AggregateIterable<Document> na_counts = collection.aggregate(Arrays.asList(
                        Aggregates.match(Filters.eq("Genre", ug)),
                        Aggregates.group(ug, Accumulators.sum("NA", "$NA_Sales"))
                ));
                AggregateIterable<Document> eu_counts = collection.aggregate(Arrays.asList(
                        Aggregates.match(Filters.eq("Genre", ug)),
                        Aggregates.group(ug, Accumulators.sum("EU", "$EU_Sales"))
                ));
                AggregateIterable<Document> jp_counts = collection.aggregate(Arrays.asList(
                        Aggregates.match(Filters.eq("Genre", ug)),
                        Aggregates.group(ug, Accumulators.sum("JP", "$JP_Sales"))
                ));
                AggregateIterable<Document> other_counts = collection.aggregate(Arrays.asList(
                        Aggregates.match(Filters.eq("Genre", ug)),
                        Aggregates.group(ug, Accumulators.sum("Other", "$Other_Sales"))
                ));
                for (Document n : na_counts) {
                    all_arr.add(n);
                    //System.out.println(n.toJson());
                }
                for (Document e : eu_counts) {
                    all_arr.add(e);
                    //System.out.println(e.toJson());
                }
                for (Document j : jp_counts) {
                    all_arr.add(j);
                    //System.out.println(j.toJson());
                }
                for (Document o : other_counts) {
                    all_arr.add(o);
                    //System.out.println(o.toJson());
                }
            }
            /*
            ...
            Sports 2346
            Strategy 681

            ...
            Sports 683.349
            Strategy 68.7
            */
            
            System.out.println("\t\t\tGRAPH OF GENRE SALES IN THE 10 MILLIONS");
            System.out.println("______________________________________________________________________________________________________");
            
            for (int y = 0; y < all_arr.size(); y++) {

                System.out.print(all_arr.get(y).getString("_id")+"\n\t|");
                int tens = (int) (all_arr.get(y).getDouble("NA") / 10);
                for (int x = 0; x < tens; x++) {
                    System.out.print("*");
                }
                System.out.print("\t"+tens+"\tNorth America\n");

                System.out.print("\t|");
                tens = (int) (all_arr.get(y + 1).getDouble("EU") / 10);
                for (int x = 0; x < tens; x++) {
                    System.out.print("*");
                }
                System.out.print("\t"+tens+"\tEurope\n");

                System.out.print("\t|");
                tens = (int) (all_arr.get(y + 2).getDouble("JP") / 10);
                for (int x = 0; x < tens; x++) {
                    System.out.print("*");
                }
                System.out.print("\t"+tens+"\tJapan\n");

                System.out.print("\t|");
                tens = (int) (all_arr.get(y + 3).getDouble("Other") / 10);
                for (int x = 0; x < tens; x++) {
                    System.out.print("*");
                }
                System.out.print("\t"+tens+"\tOther Regions\n");
                y += 3;
            }
            
            int index = 0;
            while (index < all_arr.size()) {
                Document a;

                if (all_arr.get(index).getDouble("NA") > all_arr.get(index + 1).getDouble("EU")) {
                    if (all_arr.get(index).getDouble("NA") > all_arr.get(index + 2).getDouble("JP")) {
                        if (all_arr.get(index).getDouble("NA") > all_arr.get(index + 3).getDouble("Other")) {
                            a = all_arr.get(index);
                        } else {
                            a = all_arr.get(index + 3);
                        }
                    } else {
                        if (all_arr.get(index + 2).getDouble("JP") > all_arr.get(index + 3).getDouble("Other")) {
                            a = all_arr.get(index + 2);
                        } else {
                            a = all_arr.get(index + 3);
                        }
                    }
                } else {
                    if (all_arr.get(index + 1).getDouble("EU") > all_arr.get(index + 2).getDouble("JP")) {
                        if (all_arr.get(index + 1).getDouble("EU") > all_arr.get(index + 3).getDouble("Other")) {
                            a = all_arr.get(index + 1);
                        } else {
                            a = all_arr.get(index + 3);
                        }
                    } else {
                        if (all_arr.get(index + 2).getDouble("JP") > all_arr.get(index + 3).getDouble("Other")) {
                            a = all_arr.get(index + 2);
                        } else {
                            a = all_arr.get(index + 3);
                        }
                    }
                }

                arr.add(a);
                index += 4;
            }
            System.out.println();
            System.out.println();
            
            System.out.println("TOP REGION SALES PER GENRE IN THE MILLIONS");
            System.out.println("__________________________________________");
            for (var a : arr) {
                if(a.getDouble("NA")!=null){
                    System.out.print("N. America\t"+a.getDouble("NA"));
                }else if(a.getDouble("EU")!=null){
                    System.out.print("   Europe\t"+a.getDouble("EU"));
                }else if(a.getDouble("JP")!=null){
                    System.out.print("   Japan\t"+a.getDouble("JP"));
                }else{
                    System.out.print("   Other\t"+a.getDouble("Other"));
                }
                System.out.print("\t\t"+a.getString("_id"));
                System.out.println();
            }
        }
        
        /*QUESTION OF ANALYSIS: who should I market it towards (platform / device)?
        I went about this by first ranking all the data on Global_Sales then grouping 
        them based on their distinct platforms and finding their corresponding max.
        JAVA is reading it in as an integer if the only characters in the string are
        numbers (i.e. Platform 2600)., which is why I needed to implement a try catch.
        */
        System.out.println();
        System.out.println();
        AggregateIterable<Document> jPlat = collection.aggregate(Arrays.asList(
                Aggregates.sort(eq("Global_Sales", -1)),
                Aggregates.group("$Platform", Accumulators.max("Sales", "$Global_Sales")),
                Aggregates.sort(eq("Global_Sales", -1))
        ));
        
        String platID = "";
        Double platSale = -1.0;
        System.out.println("          TOP PLATFORMS IN GLOBAL SALES");
        System.out.println("        ___________________________________");
        for (Document j : jPlat) {
            try{
                System.out.print(j.getString("_id")+"\n\t|");
                for (int x = 0; x < j.getDouble("Sales"); x++) {
                    System.out.print("*");
                }
                System.out.print("\t" + j.getDouble("Sales")+"\n");
                if(j.getDouble("Sales")>platSale){
                    platID = j.getString("_id");
                    platSale = j.getDouble("Sales");
                }
            }catch (Exception e) {
                System.out.print(j.getInteger("_id")+"\n\t|");
                for (int x = 0; x < j.getDouble("Sales"); x++) {
                    System.out.print("*");
                }
                System.out.print("\t" + j.getDouble("Sales")+"\n");
                if(j.getDouble("Sales")>platSale){
                    platID = j.getInteger("_id").toString();
                    platSale = j.getDouble("Sales");
                }
            }
        }
        
        System.out.println();System.out.println();
        AggregateIterable<Document> jPlat2 = collection.aggregate(Arrays.asList(
                Aggregates.sort(eq("Global_Sales", -1)),
                Aggregates.group("$Platform", Accumulators.avg("Average", "$Global_Sales")),
                Aggregates.sort(eq("Global_Sales", -1))
        ));
        
        String platID2 = "";
        Double platAvg = -1.0;
        System.out.println("          TOP PLATFORMS GLOBAL SALE AVGS");
        System.out.println("        ___________________________________");
        for (Document j : jPlat2) {
            try{
                System.out.print(j.getString("_id")+"\n\t|");
                for (int x = 0; x < j.getDouble("Average"); x++) {
                    System.out.print("*");
                }
                System.out.print("\t" + j.getDouble("Average")+"\n");
                if(j.getDouble("Average")>platAvg){
                    platID2 = j.getString("_id");
                    platAvg = j.getDouble("Average");
                }
            }catch (Exception e) {
                System.out.print(j.getInteger("_id")+"\n\t|");
                for (int x = 0; x < j.getDouble("Average"); x++) {
                    System.out.print("*");
                }
                System.out.print("\t" + j.getDouble("Average")+"\n");
                if(j.getDouble("Average")>platAvg){
                    platID2 = j.getInteger("_id").toString();
                    platAvg = j.getDouble("Average");
                }
            }
        }
        System.out.println();
        System.out.println("TOP PLATFORM GOES TO: \n\t\t\t" + platID + " WITH " + platSale + " MILLION SALES");
        System.out.println();
        System.out.println("BEST PLATFORM ON AVERAGE GOES TO: \n\t\t\t\t\t"+platID2+" WITH AN AVERAGE OF $"+platAvg+" MILLION SALES");
        System.out.println();System.out.println();
        
        /******************************EMRE******************************/
        MongoCollection<Document> collection1 = database.getCollection("globalvgsales");
        MongoCollection<Document> collection2 = database.getCollection("globalvgratings");

        DistinctIterable<String> unique_publishers = collection1.distinct("Publisher", String.class);
        //DistinctIterable<String> unique_platforms = collection2.distinct("Platform", String.class);
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);

        if (unique_publishers == null) {
            System.out.println("No Publishers");
        } else {
            ArrayList<Document> arr = new ArrayList();
            ArrayList<Document> all_arr = new ArrayList();
            for (var ug : unique_publishers) {
                AggregateIterable<Document> global_counts = collection1.aggregate(Arrays.asList(
                        Aggregates.match(Filters.eq("Publisher", ug)),
                        Aggregates.sort(Sorts.descending("Global_Sales")),
                        Aggregates.group(ug, Accumulators.sum("Global", "$Global_Sales"))
                ));

                for (Document n : global_counts) {
                    all_arr.add(n);
                    //System.out.println(n.toJson());
                }
            }

            for (int y = 0; y < all_arr.size(); y++) {

                int tens = (int) (all_arr.get(y).getDouble("Global") / 10);
                for (int x = 0; x < tens; x++) {
                    continue;
                }
            }

            int index = 0;
            while (index < all_arr.size()) {
                Document a;
                a = all_arr.get(index);
                arr.add(a);
                index += 4;
            }

            System.out.println();
            System.out.println();
            System.out.println("ANALYSIS 1");
            System.out.println();

            System.out.println("SALES PER PUBLISHER IN THE MILLIONS");
            System.out.println("__________________________________________");
            System.out.println();
            for (var a : arr) {
                if (a.getDouble("Global") != null) {
                    System.out.print("Global Sales:  " + df.format(a.getDouble("Global")));
                }

                System.out.print("\tPublisher: " + a.getString("_id"));
                System.out.println();
            }

            System.out.println();
            System.out.println();
            System.out.println("THE TOP 20 BEST SELLING GAMES WITH PUBLISHERS AND NAMES");
            System.out.println("__________________________________________");

            System.out.println();
            AggregateIterable<Document> global = collection1.aggregate(Arrays.asList(
                    Aggregates.sort(eq("Global_Sales", -1)),
                    Aggregates.limit(20)
            ));

            for (Document j : global) {
                if (j.getString("Publisher").length() > 15) {
                    System.out.println("Publisher Name: " + j.getString("Publisher") + "\t Game Name: " + j.getString("Name"));
                } else {
                    System.out.println("Publisher Name: " + j.getString("Publisher") + "\t\t Game Name: " + j.getString("Name"));
                }
            }

            System.out.println();

            AggregateIterable<Document> output = collection1.aggregate(Arrays.asList(
                    //new Document("$unwind", "$views"),
                    //new Document("$gt", new Document("$Year_of_Release", (2015))),
                    //new Document("$match", new Document("Year_of_Release", (2018))),
                    //new Document("$match", new Document("Year_of_Release", (2019))),
                    //match(Filters.gt("Year_of_Release", 2000)),
                    //match(Filters.lt("Year_of_Release", 2010)),
                    new Document("$sort", new Document("Global_Sales", -1)),
                    new Document("$limit", 20),
                    new Document("$project", new Document("_id", 0)
                            .append("Global_Sales", "$Global_Sales")
                            .append("Name", "$Name")
                            .append("Publisher", "$Publisher"))
            ));

            System.out.println();
            System.out.println("THE TOP 20 BEST SELLING GAMES INFORMATION");
            System.out.println("__________________________________________");
            System.out.println();

            for (Document dbObject : output) {
                System.out.println(dbObject);
            }

            //Analysis 2
            System.out.println();
            System.out.println();
            System.out.println("ANALYSIS 2");

            System.out.println("THE TOP 20 BEST SELLING GAMES WITH PLATFORMS - 2000 - 2010");
            System.out.println("__________________________________________");

            System.out.println();
            AggregateIterable<Document> output2 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2000)),
                    match(Filters.lt("Year_of_Release", 2010)),
                    new Document("$sort", new Document("Global_Sales", -1)),
                    new Document("$limit", 20),
                    new Document("$project", new Document("_id", 0)
                            .append("Global_Sales", "$Global_Sales")
                            .append("Name", "$Name")
                            .append("Platform", "$Platform"))
            ));

            System.out.println();

            for (Document dbObject : output2) {

                System.out.println("Platform Name: " + dbObject.getString("Platform") + "\t Game Name: " + dbObject.getString("Name"));
            }

            System.out.println();
            Document myresult = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2000)),
                    match(Filters.lt("Year_of_Release", 2010)),
                    match(Filters.eq("Platform", "Wii")),
                    match(Filters.gt("Global_Sales", 0.5)),
                    count("Number of Wii Platform Games Sold Over Half Million - 2000-2010"))).first();

            System.out.println(myresult.toJson());

            Document myresult5 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2000)),
                    match(Filters.lt("Year_of_Release", 2010)),
                    match(Filters.eq("Platform", "PS3")),
                    match(Filters.gt("Global_Sales", 0.5)),
                    count("Number of PS3 Platform Games Sold Over Half Million - 2000-2010"))).first();

            System.out.println(myresult5.toJson());

            Document myresult10 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2000)),
                    match(Filters.lt("Year_of_Release", 2010)),
                    match(Filters.eq("Platform", "PC")),
                    match(Filters.gt("Global_Sales", 0.5)),
                    count("Number of PC Platform Games Sold Over Half Million - 2000-2010"))).first();

            System.out.println(myresult10.toJson());

            System.out.println();
            System.out.println("THE TOP 20 BEST SELLING GAMES WITH PLATFORMS - 2010 - 2020");
            System.out.println("__________________________________________");

            System.out.println();
            AggregateIterable<Document> output3 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2010)),
                    match(Filters.lt("Year_of_Release", 2020)),
                    new Document("$sort", new Document("Global_Sales", -1)),
                    new Document("$limit", 20),
                    new Document("$project", new Document("_id", 0)
                            .append("Global_Sales", "$Global_Sales")
                            .append("Name", "$Name")
                            .append("Platform", "$Platform"))
            ));

            System.out.println();

            for (Document dbObject : output3) {
                System.out.println("Platform Name: " + dbObject.getString("Platform") + "\t Game Name: " + dbObject.getString("Name"));
            }

            System.out.println();
            Document myresult2 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2010)),
                    match(Filters.lt("Year_of_Release", 2020)),
                    match(Filters.eq("Platform", "Wii")),
                    match(Filters.gt("Global_Sales", 0.5)),
                    count("Number of Wii Platform Games Sold Over Half Million - 2010-2020"))).first();

            System.out.println(myresult2.toJson());

            Document myresult6 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2010)),
                    match(Filters.lt("Year_of_Release", 2020)),
                    match(Filters.eq("Platform", "PS3")),
                    match(Filters.gt("Global_Sales", 0.5)),
                    count("Number of PS3 Platform Games Sold Over Half Million - 2010-2020"))).first();

            System.out.println(myresult6.toJson());

            Document myresult7 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2010)),
                    match(Filters.lt("Year_of_Release", 2020)),
                    match(Filters.eq("Platform", "PS4")),
                    match(Filters.gt("Global_Sales", 0.5)),
                    count("Number of PS4 Platform Games Sold Over Half Million - 2010-2020"))).first();

            System.out.println(myresult7.toJson());

            Document myresult11 = collection2.aggregate(Arrays.asList(
                    match(Filters.gt("Year_of_Release", 2010)),
                    match(Filters.lt("Year_of_Release", 2020)),
                    match(Filters.eq("Platform", "PC")),
                    match(Filters.gt("Global_Sales", 0.5)),
                    count("Number of PC Platform Games Sold Over Half Million - 2010-2020"))).first();

            System.out.println(myresult11.toJson());
            System.out.println();
            
            /******************************CHRIS******************************/
        //5) What are the top 5 selling videogames in each country
        System.out.println();System.out.println();
        MongoCollection<Document> coll = database.getCollection("globalvgsales");
        
        AggregateIterable<Document> jp = coll.aggregate(Arrays.asList(
                Aggregates.sort(eq("JP_Sales", -1)),
                Aggregates.limit(5)
        ));
        for(Document j:jp){
            System.out.println(j.getString("Name")+"\t\t"+j.getString("Platform"));
        }
        
        System.out.println();
        AggregateIterable<Document> eu = coll.aggregate(Arrays.asList(
                Aggregates.sort(eq("EU_Sales", -1)),
                Aggregates.limit(5)
        ));
        for(Document e:eu){
            System.out.println(e.getString("Name")+"\t\t"+e.getString("Platform"));
        }
        
        System.out.println();
        AggregateIterable<Document> na = coll.aggregate(Arrays.asList(
                Aggregates.sort(eq("NA_Sales", -1)),
                Aggregates.limit(5)
        ));
        
        for(Document n:na){
            System.out.println(n.getString("Name")+"\t\t"+n.getString("Platform"));
        }

        }
    }

}
