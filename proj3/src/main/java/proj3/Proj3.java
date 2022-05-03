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
            System.out.println("Failed");
        }else{
            for(var ug : unique_genres) {
                FindIterable<Document> result = collection.find(eq("Genre", ug));
            }
        }
        
        /*
        {
            "_id" : ObjectId("59b6b96423b65d0a04de128d"),
            "itemCount": 25,
            "defectiveItemCount": 5,
            "time": ISODate("x")
        },
        {
            "_id" : ObjectId("59b6b96423b65d0a04de128d"),
            "itemCount": 20,
            "defectiveItemCount": 7,
            "time": ISODate("x")
        }
        Aggregation pipeline = newAggregation(
                match(Criteria.where("time").gt(time)),
                group().sum("itemCount").as("total").sum("defectiveItemCount").as("defective"),
                project("total", "defective")
        );*/
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        
        /*
        //Connection to MongoDB Atlas
        MongoClient client = MongoClients.create("mongodb+srv://user3:pass3@cluster0.3bpui.mongodb.net/gaming?retryWrites=true&w=majority");
        MongoDatabase database = client.getDatabase("gaming");

        MongoCollection<Document> collection = database.getCollection("test1");
        //Document query = collection.find(eq("Name","New Super Mario Bros. Wii")).iterator().next();
        Document query = collection.find(eq("Name","New Super Mario Bros. Wii")).first();
        System.out.println();
        System.out.println(query.toJson());
        System.out.println();
        System.out.println("Test:"+query.getString("Name"));
        System.out.println("Test:"+query.getInteger("Year_of_Release"));
        
        Document query2 = collection.find(eq("User_Count", 10179)).first();
        System.out.println();
        System.out.println(query2.toJson());
        System.out.println();
        System.out.println("Test:"+query2.getString("Name"));
        System.out.println("Test:"+query2.getInteger("Year_of_Release"));
        System.out.println();
        
        //{User_Count:{$gte:10000}}
        FindIterable<Document> result = collection.find(
        new Document("User_Count", new Document("$gte", 10000)));
        if (result == null) {
            System.out.println("Failed");
        }
        for (Document r : result) {
            System.out.println(r.toJson());
        }
        */
    }
}
