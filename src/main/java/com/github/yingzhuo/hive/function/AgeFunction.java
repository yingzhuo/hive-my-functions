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

import org.apache.commons.lang3.time.DateUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;

import java.text.ParseException;
import java.util.Date;

/**
 * 获取年龄
 */
@Description(name = "Age", value = "Calculate the Age of Given Column", extended = "SELECT age('1970-01-01', 'yyyy-MM-dd');")
public class AgeFunction extends GenericUDF {

    private StringObjectInspector dateObject;
    private StringObjectInspector patternObject;

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

        if (arguments.length != 2) {
            throw new UDFArgumentLengthException("AgeFunction take 2 arguments.");
        }

        if (!(arguments[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "AgeFunction parameter 1 need to string.");
        }

        if (!(arguments[1] instanceof StringObjectInspector)) {
            throw new UDFArgumentTypeException(0, "AgeFunction parameter 2 need to string.");
        }

        this.dateObject = (StringObjectInspector) arguments[0];
        this.patternObject = (StringObjectInspector) arguments[1];

        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        try {
            final String dateString = dateObject.getPrimitiveJavaObject(arguments[0].get());
            final String patternString = patternObject.getPrimitiveJavaObject(arguments[1].get());

            final Date now = new Date();
            final Date date = DateUtils.parseDate(dateString, patternString);

            long timeBetween = now.getTime() - date.getTime();
            double yearsBetween = timeBetween / 3.15576e+10;
            return (int) Math.floor(yearsBetween);
        } catch (ParseException e) {
            throw new HiveException(e.getMessage());
        }
    }

    @Override
    public String getDisplayString(String[] children) {
        return null;
    }

}
