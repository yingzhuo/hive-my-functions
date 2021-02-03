/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 _   _ _             __  __         _____                 _   _
| | | (_)_   _____  |  \/  |_   _  |  ___|   _ _ __   ___| |_(_) ___  _ __  ___
| |_| | \ \ / / _ \ | |\/| | | | | | |_ | | | | '_ \ / __| __| |/ _ \| '_ \/ __|
|  _  | |\ V /  __/ | |  | | |_| | |  _|| |_| | | | | (__| |_| | (_) | | | \__ \
|_| |_|_| \_/ \___| |_|  |_|\__, | |_|   \__,_|_| |_|\___|\__|_|\___/|_| |_|___/
                            |___/
 https://github.com/yingzhuo/hive-my-functions
* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
package com.github.yingzhuo.hive.function;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.Collections;
import java.util.List;

/**
 * 爆炸string
 */
@Description(name = "ExplodeStringFunction", value = "Explode the string of Given Column", extended = "SELECT explode_str('hadoop, hive, hbase', ',');")
public class ExplodeStringFunction extends GenericUDTF {

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        final List<? extends StructField> fields = argOIs.getAllStructFieldRefs();

        if (fields.size() != 2) {
            throw new UDFArgumentLengthException("ExplodeStringFunction take 2 arguments.");
        }

        if (!"string".equalsIgnoreCase(fields.get(0).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentTypeException(1, "ExplodeStringFunction parameter 1 need to string.");
        }

        if (!"string".equalsIgnoreCase(fields.get(1).getFieldObjectInspector().getTypeName())) {
            throw new UDFArgumentTypeException(2, "ExplodeStringFunction parameter 2 need to string.");
        }

        return ObjectInspectorFactory.getStandardStructObjectInspector(
                Collections.singletonList("col1"),
                Collections.singletonList(PrimitiveObjectInspectorFactory.javaStringObjectInspector)
        );
    }

    @Override
    public void process(Object[] args) throws HiveException {

        String text = args[0].toString();
        String separator = args[1].toString();

        if (StringUtils.isBlank(text) || StringUtils.isBlank(separator)) {
            return;
        }

        for (String it : StringUtils.split(text, separator)) {
            super.forward(Collections.singletonList(it.trim()));
        }
    }

    @Override
    public void close() throws HiveException {
        // close
    }

}
