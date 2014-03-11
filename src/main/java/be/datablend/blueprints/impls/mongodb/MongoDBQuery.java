/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package be.datablend.blueprints.impls.mongodb;

import be.datablend.blueprints.impls.mongodb.util.MongoDBUtil;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Predicate;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultGraphQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author jwalton
 */
public class MongoDBQuery extends DefaultGraphQuery {

    private MongoDBGraphFork graph;
    private BasicDBObject query = new BasicDBObject();
    private List<PredicateContainer> predicates = new ArrayList<PredicateContainer>();

    public MongoDBQuery(Graph graph) {
        super(graph);
        this.graph = (MongoDBGraphFork) graph;
    }

    private void addToQuery(String key, Object value) {
        String mongoKey = MongoDBUtil.createPropertyKey(key);
        if (this.query.containsField(mongoKey)) {
            List<DBObject> andValues = null;

            if (query.containsField("$and")) {
                andValues = (List<DBObject>) query.get("$and");
            } else {
                andValues = new ArrayList<DBObject>();

            }
            //remove the value that was stored in the key and add it to the and.
            Object removedObject = query.remove(mongoKey);
            BasicDBObject removedObjectQuery = new BasicDBObject().append(mongoKey, removedObject);
            andValues.add(removedObjectQuery);
            //add new value eto and list
            andValues.add(new BasicDBObject().append(mongoKey, value));
            this.query.put("$and", andValues);
        } else {
            query.append(mongoKey, value);
        }
        //System.out.println("Current mongo query: " + query.toString());
    }

    @Override
    public Iterable<Vertex> vertices() {
        if (query.keySet().isEmpty()) {
            return super.vertices();
        }
        System.out.println("Mongo query objs: " + query.toString());
        DBCursor cursor = graph.getVertexCollection().find(query);
        return new MongoDBIterable<Vertex>(cursor, graph, Vertex.class, predicates, super.limit);
    }

    @Override
    public Iterable<Edge> edges() {
        if (query.keySet().isEmpty() && predicates.isEmpty()) {
            return super.edges();
        }
        System.out.println("Mongo query objs: " + query.toString());
        DBCursor cursor = graph.getEdgeCollection().find(query);
        return new MongoDBIterable<Edge>(cursor, graph, Edge.class, predicates, super.limit);
    }

    @Override
    public <T extends Comparable<T>> GraphQuery has(String key, T value, Compare compare) {
        super.has(key, value, compare);

        String operator = null;
        if (compare == Compare.GREATER_THAN) {
            operator = "$gt";
        } else if (compare == Compare.GREATER_THAN_EQUAL) {
            operator = "$gte";
        } else if (compare == Compare.LESS_THAN) {
            operator = "$lt";
        } else if (compare == Compare.LESS_THAN_EQUAL) {
            operator = "$lte";
        } else if (compare == Compare.NOT_EQUAL) {
            operator = "$neq";
        }

        if (operator != null) {

            BasicDBObject queryObj = new BasicDBObject().append(operator, value);
            addToQuery(key, queryObj);
        } else {
            addToQuery(key, value);
        }
        System.out.println("Adding compare query to query object: " + query.toString());
        return this;
    }

    @Override
    public GraphQuery has(String key, Predicate predicate, Object value) {
        String operator = null;
        if (predicate == com.tinkerpop.blueprints.Compare.GREATER_THAN) {
            operator = "$gt";
        } else if (predicate == com.tinkerpop.blueprints.Compare.GREATER_THAN_EQUAL) {
            operator = "$gte";
        } else if (predicate == com.tinkerpop.blueprints.Compare.LESS_THAN) {
            operator = "$lt";
        } else if (predicate == com.tinkerpop.blueprints.Compare.LESS_THAN_EQUAL) {
            operator = "$lte";
        } else if (predicate == com.tinkerpop.blueprints.Compare.NOT_EQUAL) {
            operator = "$ne";
        }

        if (operator != null) {
            BasicDBObject query = new BasicDBObject().append(operator, value);
            addToQuery(key, query);

            return this;
        }

        if (predicate == com.tinkerpop.blueprints.Compare.EQUAL) {
            addToQuery(key, value);
            return this;
        }

        if (predicate instanceof ArrayRangeSearchPredicate) {
            Object min = null;
            Object max = null;
            if (value instanceof Object[]) {
                Object[] valueArray = (Object[]) value;
                min = valueArray[0];
                max = valueArray[1];
                return this;
            } else if (value instanceof List) {
                List valueList = (List) value;
                min = valueList.get(0);
                max = valueList.get(1);
            } else {
                String errorMsg = "Do not know how to deal with value of type: " + value.getClass().getName() + " for ArrayRangeSearchPredicate";
                System.out.println(errorMsg);
                throw new RuntimeException(errorMsg);
            }
            BasicDBObject query = new BasicDBObject().append("$gte", min).append("$lte", max);
            addToQuery(key, query);
            return this;
        } else if (predicate instanceof ArraySearchPredicate) {
            if (value instanceof Map) {
                Map<String, Object> valueMap = (Map<String, Object>) value;
                BasicDBObject query = new BasicDBObject();
                for (String mapKey : valueMap.keySet()) {
                    query.put(mapKey, valueMap.get(mapKey));
                }
                addToQuery(key, query);
            } else if (value instanceof DBObject) {
                DBObject query = (DBObject) value;
                addToQuery(key, query);
            } else {
                String errorMsg = "Do not know how to deal with value of type " + value.getClass().getName() + " for ArraySearchPredicate";
                System.out.println(errorMsg);
                throw new RuntimeException(errorMsg);
            }
            return this;
        }

        super.has(key, predicate, value);
        this.predicates.add(new PredicateContainer(key, predicate, value));
        System.out.println("MongoDbQuery has Called with predicate, not sure what to do here");
        return this;
    }

    @Override
    public GraphQuery hasNot(String key, Object value) {
        super.hasNot(key, value);
        String operator = "$neq";
        BasicDBObject queryObj = new BasicDBObject().append(operator, value);
        query.append(MongoDBUtil.createPropertyKey(key), queryObj);
        return this;
    }

    @Override
    public GraphQuery has(String key, Object value) {
        super.has(key, value);
        String mongoKey = MongoDBUtil.createPropertyKey(key);
        if (query.containsField(mongoKey)) {
            List<DBObject> andValues = null;
            if (query.containsField("$and")) {
                andValues = (List<DBObject>) query.get("$and");
            } else {
                andValues = new ArrayList<DBObject>();
            }
            andValues.add(new BasicDBObject().append(mongoKey, value));
            andValues.add(new BasicDBObject().append(mongoKey, query.remove(mongoKey)));
            query.append("$and", andValues);
        } else {
            query.append(MongoDBUtil.createPropertyKey(key), value);
        }
        return this;
    }

    @Override
    public GraphQuery hasNot(String key) {
        super.hasNot(key);
        String operator = "$exists";
        BasicDBObject queryObj = new BasicDBObject(operator, false);
        query.append(MongoDBUtil.createPropertyKey(key), queryObj);
        return this;
    }

    @Override
    public GraphQuery has(String key) {
        super.has(key);
        String operator = "$exists";
        BasicDBObject queryObj = new BasicDBObject(operator, true);
        query.append(MongoDBUtil.createPropertyKey(key), queryObj);
        return this;
    }

    @Override
    public GraphQuery limit(int limit) {
        return super.limit(limit); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <T extends Comparable<?>> GraphQuery interval(String key, T startValue, T endValue) {
        BasicDBObject queryObj = new BasicDBObject().append("$gte", startValue).append("$lt", endValue);
        this.query.append(MongoDBUtil.createPropertyKey(key), queryObj);
        return this;
//        return super.interval(key, startValue, endValue); //To change body of generated methods, choose Tools | Templates.
    }
}
