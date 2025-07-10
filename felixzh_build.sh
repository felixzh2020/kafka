 /d/downloads/gradle-6.6/bin/gradle clean releaseTarGz -x test

 # https://issues.apache.org/jira/browse/KAFKA-9726

 基于上述PR支持单向同步同名Topic之后，同步方向来回切换的时候，两个集群Topic会有脏数据写入。
 本次修改尝试解决上述问题！