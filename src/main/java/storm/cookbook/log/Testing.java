package storm.cookbook.log;

import redis.clients.jedis.Jedis;
public class Testing {

        public static void main(String[] args) throws InterruptedException {




            String cacheKey = "languages";
            Jedis jedis = new Jedis("localhost");
            //Adding a set as value
            jedis.sadd(cacheKey,"Java","C#","Python");//SADD

            //Getting all values in the set: SMEMBERS
            System.out.println("Languages: " + jedis.smembers(cacheKey));
            //Adding new values
            jedis.sadd(cacheKey,"Java","Ruby");
            //Getting the values... it doesn't allow duplicates
            System.out.println("Languages: " + jedis.smembers(cacheKey));


           /* String cacheKey = "cachekey";
            Jedis jedis = new Jedis("localhost");
            //adding a new key
            jedis.set(cacheKey, "cached value");
            //setting the TTL in seconds
            jedis.expire(cacheKey, 15);
            //Getting the remaining ttl
            System.out.println("TTL:" + jedis.ttl(cacheKey));
            Thread.sleep(1000);
            System.out.println("TTL:" + jedis.ttl(cacheKey));
            //Getting the cache value
            System.out.println("Cached Value:" + jedis.get(cacheKey));

            //Wait for the TTL finishs
            Thread.sleep(15000);

            //trying to get the expired key
            System.out.println("Expired Key:" + jedis.get(cacheKey));*/
        }


}
