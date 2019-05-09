# ratelimiter

初步实现令牌桶法的redis调用部分  
  
每一个限流的key在redis中对应两个键值  
(1) key  
(2) key_idx  
其中key记录当前的限制次数, key_idx记录当前修改的序号, 当前key_idx表示为{key},  
所以对key有所限制, 当前限制为禁止包含右括号(}), 具体可以参考db包中的CheckKeyValid函数

如果想调用db_test.go中的测试, 可以参考如下命令:  
(1) 单实例redis  
go test ratelimiter/db -args "localhost:6379" "" "hello"  
(2) redis集群  
go test ratelimiter/db -args "localhost:30001 localhost:30002 localhost:30003" "" "hello"
