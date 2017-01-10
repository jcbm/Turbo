import java.util.Date;

/**
 * Created by JC Denton on 09-01-2017.
 */
@Deprecated // dont use this for now - Iterator should only iterate over the workers that are active - we don't care about inactive workers. Once way is to specify it in the HashMap, which means we still iterate over all workers. Instead, just remove from the hashMap when it becomes inactive, and add when it is active.
public class HeartBeatInfo {
    public HeartBeatInfo(Date timestamp, boolean active) {

    }
}
