/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.datablend.blueprints.impls.mongodb;

import com.tinkerpop.blueprints.Predicate;

/**
 *
 * @author jwalton
 */
public class PredicateContainer {
    private String key;
    private Object object;
    private Predicate predicate;
    
    public PredicateContainer(String key, Predicate predicate, Object object){
        this.key = key;
        this.predicate = predicate;
        this.object = object;
        
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public Predicate getPredicate() {
        return predicate;
    }

    public void setPredicate(Predicate predicate) {
        this.predicate = predicate;
    }
    
}
