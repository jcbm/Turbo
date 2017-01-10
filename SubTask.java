import java.util.Collection;

/**
 * Created by JC Denton on 09-01-2017.
 */
public class SubTask {
    private final String parent;
    private final Collection data;

    public SubTask(String parent, Collection data) {
        this.parent = parent;
        this.data = data;
    }
}
