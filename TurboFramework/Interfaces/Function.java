package TurboFramework.Interfaces;

import java.io.Serializable;
import java.util.Collection;

public interface Function extends Serializable {
Object execute(Collection<Object> data);
}
