package com.tok;

import com.mongodb.*;

import java.net.UnknownHostException;

/**
 * Shafi tokhi
 */
public class RemoveLowestHomeWrokDocument {
    public static void main(String[] args) {
        MongoClient client = null;
        try {
            client = new MongoClient();
        } catch (UnknownHostException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
        DB db = client.getDB("students");
        DBCollection collection = db.getCollection("grades");
        DBObject one = collection.findOne();
        //System.out.println(one);
	// query only  records with type homework
        DBObject query = QueryBuilder.start("type").is("homework").get();
	// sort students by id  in ascending order, then sort them by score
        DBCursor cursor = collection.find(query).sort(new BasicDBObject("student_id",1).append("score",-1));
        try{
            int counter = 1;
            int studentID = 0;
            Object id = null;
            while(cursor.hasNext()){
                DBObject cur = cursor.next();
                if(counter == 1){
                    studentID = Integer.parseInt(cur.get("student_id").toString());
                    id =  cur.get("_id");
                    counter++;
                }
                else{
                    if(studentID != Integer.parseInt(cur.get("student_id").toString())){
                        System.out.println("student id changed: "+studentID);
                        BasicDBObject document = new BasicDBObject();
                        document.put("_id", id);
                        collection.remove(document);
                        counter = 1;
                    }
                }
                System.out.println(cur);
            }
        }finally {
            cursor.close();
        }


    }
}
