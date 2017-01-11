import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by JC Denton on 11-01-2017.
 */
 public class HashMapHelper {

   static List<?> safeGetHashMapCollection(HashMap<String, List<?>> hashMap, Object key) {
       List<?> value = hashMap.get(key);
       return value != null ? value : new ArrayList<>();

   }

}
