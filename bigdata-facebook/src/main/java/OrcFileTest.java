import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Created by ricdong on 15-8-1.
 */
public class OrcFileTest {

    private final String typeString = "struct<air_temp:double,station_id:string,lat:double,lon:double>";

    private final TypeInfo typeInfo = TypeInfoUtils
            .getTypeInfoFromTypeString(typeString);
    private final ObjectInspector oip = TypeInfoUtils
            .getStandardJavaObjectInspectorFromTypeInfo(typeInfo);


    public OrcFileTest() {

    }


    public static void main(String args[]) {
         new OrcFileTest();

    }
}
