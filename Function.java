import java.io.Serializable;
import java.util.Collection;

/**
 * Created by JC Denton on 09-01-2017.
 */
public interface Function extends Serializable {
Object execute(Collection<Object> data);
}
