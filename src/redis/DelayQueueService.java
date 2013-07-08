package redis;

import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;


public class DelayQueueService {

	/**
	 * @param args
	 */
	private Jedis jedis = new Jedis("192.168.183.128",6379);
	
	public void queueMessage(String queue,String message,Long delay){
		Long time = (Long) (System.currentTimeMillis()/1000 + delay);
		jedis.zadd(queue, time, message);
	}
	
	public Set<String> getMessages(String queue){
		Long starttime = (long) 0;
		Long endtime = (Long) (System.currentTimeMillis()/1000);
		Transaction t = jedis.multi();
		Response<Set<String>> response = t.zrangeByScore(queue, starttime, endtime);
		t.zremrangeByScore(queue, starttime, endtime);
		t.exec();
		return response.get();		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DelayQueueService delayqueue = new DelayQueueService();
		delayqueue.queueMessage("queue", "testzset2", (long) 3);
		Set dqset = delayqueue.getMessages("queue");
		System.out.println(dqset.toString());
        try {
			Thread.sleep(4000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        dqset = delayqueue.getMessages("queue");
		System.out.println(dqset.toString());
	}

}
