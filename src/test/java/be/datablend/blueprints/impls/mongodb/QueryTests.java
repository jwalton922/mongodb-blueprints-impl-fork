/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.datablend.blueprints.impls.mongodb;

import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import java.util.Date;
import org.junit.Test;

/**
 *
 * @author jfwalto
 */
public class QueryTests {
    
    @Test
    public void alertQueryTest(){
        MongoDBGraphFork graph = new MongoDBGraphFork("r01sv10a.mis01", 27017);
        long current = new Date().getTime();
        long lookBack = current - (1000*60*60*10);
        GraphQuery q = graph.query()
                .has("_type", "ALERT")
                .has("ALERT_CREATION_TIME", lookBack, Query.Compare.GREATER_THAN).has("PRIORITY", "LOW").has("PRIORITY","HIGH");
        
        Iterable<Vertex> vertices =  q.vertices();
        int countResults = 0;
        for(Vertex v : vertices){
            countResults++;
        }
        System.err.println("Found "+countResults+" alerts in last 10 hours");
    }
}
