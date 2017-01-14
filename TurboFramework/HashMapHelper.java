package TurboFramework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
Class for looking up a collection-value in a hashMap. If no collection exists for the key, it is created, put in the map and returned.
 */

 public class HashMapHelper {

   static ArrayList safeGetHashMapCollection(HashMap<String, ArrayList<String>> hashMap, String key) {
       ArrayList<String> value = hashMap.get(key);
       if (value == null) {

         value = new ArrayList<>();
           hashMap.put(key, value);
       }
       return value;

   }

   public static void main(String[] args) {
       HashMap<String, ArrayList<String>> testMap = new HashMap<>();
       ArrayList arrayList = safeGetHashMapCollection(testMap, "hej");
       arrayList.add("swag");
       testMap.get("hej");

   }
   }


