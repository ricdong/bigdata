import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ricdong on 15-8-4.
 */
public class ORCReducer extends Reducer<NullWritable,Text,NullWritable,Writable> {

    private final OrcSerde serde = new OrcSerde();

    //Define the struct which will represent each row in the ORC file
    // final String typeString = "struct<air_temp:double,station_id:string,lat:double,lon:double>";

    private final String typeString = "struct<id:string,name:string>";

    private final TypeInfo typeInfo = TypeInfoUtils
            .getTypeInfoFromTypeString(typeString);
    private final ObjectInspector oip = TypeInfoUtils
            .getStandardJavaObjectInspectorFromTypeInfo(typeInfo);

    public void reduce(NullWritable key, Iterable<Text> values,
                       Context context) throws IOException, InterruptedException {

        for(Text t : values) {
            System.out.println("value " + t);
        }

        List<Object> struct = new ArrayList<Object>(2);
        struct.add(0, "001");
        struct.add(1, "ricdong");
        Writable row = serde.serialize(struct, oip);

        System.out.println("vlaue " + values);
        context.write(NullWritable.get(), row);
    }
}
