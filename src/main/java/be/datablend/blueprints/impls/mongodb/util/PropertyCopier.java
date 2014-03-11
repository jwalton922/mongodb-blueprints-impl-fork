/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.datablend.blueprints.impls.mongodb.util;

import be.datablend.blueprints.impls.mongodb.util.MongoDBUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author jfwalto
 */
public class PropertyCopier {

    private final static String OBJECT_TYPE_FIELD = ":type";
    private final static String IDENTIFIER_FIELD = ":identifier";
    private Set<String> fieldsToCopy;
    private String sourceDatabase;
    private String objectType;
    public PropertyCopier(String sourceDatabase, Set<String> fieldsToCopy, String objectType) {
        this.sourceDatabase = sourceDatabase;
        this.fieldsToCopy = fieldsToCopy;
        this.objectType = objectType;
    }

    public void copyProperties(Set<String> targetDatabases) {
        try {
            Mongo sourceMongo = new Mongo(sourceDatabase, 27017);
            DB sourceDB = sourceMongo.getDB(MongoDBUtil.GRAPH_DATABASE);
            DBCollection sourceCollection = sourceDB.getCollection(MongoDBUtil.VERTEX_COLLECTION);
            DBObject sourceQuery = new BasicDBObject().append(OBJECT_TYPE_FIELD, objectType);

            DBCursor sourceCursor = sourceCollection.find(sourceQuery);
            if (!sourceCursor.hasNext()) {
                System.out.println("Could not find any source objects matching the query!");
                sourceMongo.close();
                return;
            }
            List<DBObject> sourceObjectList = new ArrayList<DBObject>();
            while (sourceCursor.hasNext()) {
                sourceObjectList.add(sourceCursor.next());
            }

            sourceMongo.close();
            for (String targetDatabaseHost : targetDatabases) {
                Mongo targetMongo = new Mongo(targetDatabaseHost, 27017);
                DB targetDB = targetMongo.getDB(MongoDBUtil.GRAPH_DATABASE);
                DBCollection targetCollection = targetDB.getCollection(MongoDBUtil.VERTEX_COLLECTION);

                for (DBObject sourceObject : sourceObjectList) {
                    Object identifier = sourceObject.get(IDENTIFIER_FIELD);
                    
                    DBObject updateQuery = new BasicDBObject().append(OBJECT_TYPE_FIELD, objectType).append(IDENTIFIER_FIELD, identifier);
                    BasicDBObject setValue = new BasicDBObject();
                    for (String fieldToCopy : fieldsToCopy) {
                        if (sourceObject.containsField(fieldToCopy)) {
                            setValue.append(fieldToCopy, sourceObject.get(fieldToCopy));
                        }
                    }
                    
                    DBObject updateSet = new BasicDBObject().append("$set", setValue);
                    System.out.println(targetDatabaseHost+" writing: Update query: "+updateQuery.toString()+" updateSet: "+updateSet.toString());
                    targetCollection.update(updateQuery, updateSet, false, false);
                }

            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static void main(String[] args){
        String sourceDatabase = "localhost";
        Set<String> fieldsToCopy = new HashSet<String>(Arrays.asList("PORTRAIT", "EMAIL", "PHONE", "LOCATION"));
        PropertyCopier copier = new PropertyCopier(sourceDatabase, fieldsToCopy, "USER");
        
        Set<String> targetDatabases = new HashSet<String>(Arrays.asList("r01sv10a.mis01", "r01sv12b.mis01"));
        
        copier.copyProperties(targetDatabases);
        
    }
}
