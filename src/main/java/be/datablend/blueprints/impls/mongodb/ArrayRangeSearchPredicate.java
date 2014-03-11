/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.datablend.blueprints.impls.mongodb;

import com.tinkerpop.blueprints.Predicate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 *
 * @author jfwalto
 */
public class ArrayRangeSearchPredicate implements Predicate {
    
    @Override
    public boolean evaluate(Object o1, Object o2) {
        boolean passesFilter = true;
        if (o1 == null || o2 == null) {
            System.out.println("One of input objects is null! failing ArrayRangeSearch");
            return false;
        }
        List propList = null;
        Object min = null;
        Object max = null;
        if (o1 instanceof List && o2 instanceof List) {
            //System.out.println("ArrayRangeSearch called on lists");
            List a = (List) o1;
            List b = (List) o2;
            //System.out.println("List a size: " + a.size() + " b size: " + b.size());
            propList = a;
            if (b.size() == 2) {
                min = b.get(0);
                max = b.get(1);
            } else {
                System.out.println("List should be size two for ArrayRangeSearch! It's: " + b.size());
            }

        } else if (o1 instanceof Object[] && o2 instanceof Object[]) {
            //System.out.println("ArrayRangeSearch called on arrays");
            Object[] array1 = (Object[]) o1;
            Object[] array2 = (Object[]) o2;
            propList = new ArrayList<Object>(Arrays.asList(array1));
            if (array2.length == 2) {
                min = array2[0];
                max = array2[1];
            } else {
                System.out.println("Array should be length two for ArrayRangeSearch! It's: " + array2.length);
            }
        }

        if (min != null && max != null) {

            if (min instanceof Comparable && max instanceof Comparable) {
                //System.out.println("min and max are comparable");
                Comparable minComp = (Comparable) min;
                Comparable maxComp = (Comparable) max;
                //System.out.println("Found min amd max, checking to see if property has value in range");
                passesFilter = false;
                for (int i = 0; i < propList.size(); i++) {
                    Object value = propList.get(i);
                    if (minComp.compareTo(value) <= 0 && maxComp.compareTo(value) >= 0) {
                        passesFilter = true;
                        break;
                    }
                }
            } else {
                System.out.println("Min and max are not comparable! Failing search");
                passesFilter = false;
            }
        }
        return passesFilter;
    }
}
