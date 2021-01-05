# hive-my-functions

```hiveql
drop function if exists age;
create function 
    age as 'com.github.yingzhuo.hive.function.AgeFunction'
    using jar 'hdfs://192.168.99.130:8020/user/hive/jars/hive-my-functions-1.0.0.jar';

drop function if exists desensitize;
create function 
    desensitize as 'com.github.yingzhuo.hive.function.DesensitizeFunction'
    using jar 'hdfs://192.168.99.130:8020/user/hive/jars/hive-my-functions-1.0.0.jar';

drop function if exists explode_str;
create function 
    explode_str as 'com.github.yingzhuo.hive.function.ExplodeStringFunction'
    using jar 'hdfs://192.168.99.130:8020/user/hive/jars/hive-my-functions-1.0.0.jar';
```
