# hive-my-functions

```hiveql
create function 
desensitize as 'com.github.yingzhuo.hive.function.DesensitizeFunction' 
using jar 'hdfs://192.168.99.130:8020/user/hive/jars/hive-my-functions-1.0.0.jar';
```
