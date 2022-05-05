package proj3_test;

//import com.mongodb.BasicDBObject;
//import com.mongodb.Block;
//import com.mongodb.DBObject;
//import com.mongodb.DBCursor;
//import com.mongodb.client.FindIterable;
//import com.mongodb.client.AggregateIterable.*;
//import com.mongodb.client.MongoCursor;
//import static com.mongodb.client.model.Filters.and;
//import java.util.Iterator;
//import java.util.List;
//import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;
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
import java.util.ArrayList;
import java.util.Arrays;
import static com.mongodb.client.model.Aggregates.*;
import com.mongodb.client.model.Sorts;
import java.text.DecimalFormat;


public class proj3_test {

    public static void main(String[] args) {

        MongoClient client = MongoClients.create("mongodb+srv://user2:pass2@cluster0.3bpui.mongodb.net/gaming?retryWrites=true&w=majority");
        MongoDatabase database = client.getDatabase("gaming");
        System.out.println();System.out.println();

        MongoCollection<Document> collection = database.getCollection("globalvgsales");
        MongoCollection<Document> collection2 = database.getCollection("globalvgratings");
              

        DistinctIterable<String> unique_publishers = collection.distinct("Publisher", String.class);
        //DistinctIterable<String> unique_platforms = collection2.distinct("Platform", String.class);
        DecimalFormat df = new DecimalFormat();
        df.setMaximumFractionDigits(2);

        if (unique_publishers == null) {
            System.out.println("No Publishers");
        }
        else{
            ArrayList<Document> arr = new ArrayList();
            ArrayList<Document> all_arr = new ArrayList();
            for(var ug : unique_publishers) {
                AggregateIterable<Document> global_counts = collection.aggregate(Arrays.asList(
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
                if(a.getDouble("Global")!=null){
                    System.out.print("Global Sales:  "+df.format(a.getDouble("Global")));
                }
                
                System.out.print("\tPublisher: "+a.getString("_id"));
                System.out.println();
            }
            
        System.out.println(); 
        System.out.println();
        System.out.println("THE TOP 20 BEST SELLING GAMES WITH PUBLISHERS AND NAMES");
        System.out.println("__________________________________________");
            
        System.out.println();
        AggregateIterable<Document> global = collection.aggregate(Arrays.asList(
                Aggregates.sort(eq("Global_Sales", -1)),
                Aggregates.limit(20)
        ));
        
        for(Document j:global){
        	if (j.getString("Publisher").length()>15) {
        		System.out.println("Publisher Name: "+j.getString("Publisher")+"\t Game Name: "+j.getString("Name"));
        	}
        	else {
        		System.out.println("Publisher Name: "+j.getString("Publisher")+"\t\t Game Name: "+j.getString("Name"));
        	}
        }
        
        System.out.println();
        
        AggregateIterable<Document> output = collection.aggregate(Arrays.asList(
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

        for (Document dbObject : output)
        {
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
        

        for (Document dbObject : output2)
        {

        	System.out.println("Platform Name: "+dbObject.getString("Platform")+"\t Game Name: "+dbObject.getString("Name"));
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
        

        for (Document dbObject : output3)
        {
        	System.out.println("Platform Name: "+dbObject.getString("Platform")+"\t Game Name: "+dbObject.getString("Name"));
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
        
        }
    }
}
        