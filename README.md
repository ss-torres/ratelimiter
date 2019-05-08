# ratelimiter

初步实现令牌桶法的redis调用部分  
  
每一个限流的key在redis中对应两个键值  
(1) key  
(2) key_idx  
其中key记录当前的限制次数, key_idx记录当前修改的序号
