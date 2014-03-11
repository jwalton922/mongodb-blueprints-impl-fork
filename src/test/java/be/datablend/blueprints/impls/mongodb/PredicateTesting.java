/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.datablend.blueprints.impls.mongodb;

import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.tg.TinkerGraph;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.script.Bindings;
import javax.script.ScriptEngine;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author jfwalto
 */
public class PredicateTesting {
    
    @Test
    public void testArrayRangeSearchPredicate(){
        Graph graph = new TinkerGraph();
        Vertex v1 = graph.addVertex(null);
        Vertex v2 = graph.addVertex(null);
        System.out.println("Vertex 1 id = "+v1.getId()+" Vertex 2 id: "+v2.getId());
        Vertex v3 = graph.addVertex("myId");
        System.out.println("Vertex 3 id: "+v3.getId());
        Edge e = graph.addEdge(null, v1, v2, "label");
        List<Long> longArray = new ArrayList<Long>(Arrays.asList(10L, 15L, 20L));
        e.setProperty("array", longArray);
        
        Iterable<Edge> edges = graph.query().has("array", new ArrayRangeSearchPredicate(), new ArrayList<Long>(Arrays.asList(9L,11L))).edges();
        boolean foundEdge = false;
        for(Edge edge: edges){
            foundEdge = true;
            System.out.println("Found matching edge!");
            Assert.assertTrue(e.getId().equals(edge.getId()));
        }
        Assert.assertTrue(foundEdge);
        
        edges = graph.query().has("array", new ArrayRangeSearchPredicate(), new ArrayList<Long>(Arrays.asList(3L,5L))).edges();
        Assert.assertFalse(edges.iterator().hasNext());
        
    }
    
}
