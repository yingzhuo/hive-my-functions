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
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

/**
 * 脱敏函数
 */
@Description(name = "Desensitize", value = "Desensitize the Given Column", extended = "SELECT desensitize('Hello World!');")
public class DesensitizeFunction extends GenericUDF {

    private StringObjectInspector input;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 1) {
            throw new UDFArgumentLengthException("DesensitizeFunction only take 1 argument.");
        }

        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "DesensitizeFunction only take string type.");
        }

        this.input = (StringObjectInspector) arguments[0];

        return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        final String text = input.getPrimitiveJavaObject(arguments[0].get());

        if (text == null) return null;
        if (StringUtils.isBlank(text)) return "";

        return StringUtils.replacePattern(text, ".", "-");
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }

}
