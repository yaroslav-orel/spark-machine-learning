import static java.lang.String.format;
import static java.lang.System.getProperty;

public class MiscUtil {

    public static String getFilePath(String fileName){
        return format("%s/%s", getProperty("user.dir"), fileName);
    }
}
