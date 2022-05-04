/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */

package proj3;

import com.mongodb.BasicDBObject;
import static com.mongodb.client.model.Filters.eq;
import org.bson.Document;
import com.mongodb.Block;
import com.mongodb.DBObject;
import com.mongodb.DBCursor;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.AggregateIterable.*;
import com.mongodb.client.internal.AggregateIterable.*;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Aggregates.*;
import com.mongodb.client.model.Sorts;
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
                System.out.print("\t"+a.getString("_id"));
                System.out.println();
            }
        }
        //4) who should I market it towards (platform / device)
        
        
        
        
        
        /******************************CHRIS******************************/
        //5) What are the top 5 selling videogames in each country
        System.out.println();System.out.println();
        MongoCollection<Document> coll = database.getCollection("globalvgsales");
        
        AggregateIterable<Document> jp2 = coll.aggregate(Arrays.asList(
                Aggregates.sort(eq("JP_Sales", -1)),
                Aggregates.limit(5)
        ));
        
        for(Document j:jp2){
            System.out.println(j.toJson());
        }
        
        /*db.globalvgratings.aggregate([
        {
            $sort:
            { 
                Name: 1, JP_Sales: 1 
            }
        },
        {
            $group:
            {
                _id: "$Name",JP_Sales: 
                { 
                    $first: "$JP_Sales" 
                }
            }
        },
        {
            $sort:{
                JP_Sales:-1
            }
        },
        {
            $limit: 5
        }])*/
        //db.globalvgratings.aggregate([{$sort:{ Name: 1, NA_Sales: 1 } },{$group:{_id: "$Name",NA_Sales: { $first: "$NA_Sales" }}},{$sort:{NA_Sales:-1}},{$limit: 5}])
        //db.globalvgratings.aggregate([{$sort:{ Name: 1, EU_Sales: 1 } },{$group:{_id: "$Name",EU_Sales: { $first: "$EU_Sales" }}},{$sort:{EU_Sales:-1}},{$limit: 5}])
        
        
    }

}
