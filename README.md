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

对于使用redis的"{}"键实现的尽量有N个节点作为"加令牌"节点的实现，有如下需要说明：  
(1) redis需要启动键空间监听，即在redis.conf中添加 notify-keyspace-events Kgl  
(2) 多个节点启动之前，需要清空redis中的"{}"键，因为，现在的实现无法确定第一个启动的节点   
(3) 当前的实现，只是用来说明可以通过这种方式实现，实现的细节并不十分完善，而且我没有进行详细的测试，  
  也没有与更新操作相结合，所以，不建议在实际运行中使用这个实现，我打算使用redis的subscribe和publish  
  来实现一个更加简单的版本，那个版本应该更合适使用  
(4) 上述代码中如果存在什么问题，欢迎指出，如果有人对此进行了benchmark，也欢迎添加到README中  
(5) redis键空间提醒*redis.Message channel缓存的默认大小为100，我没有测试，  
如果这个满了之后，会出现什么情况。这里可以考虑添加一个go程将消息中间存储。
