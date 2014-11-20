package Fetch_Reg_Load;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RepeatData {
	Set s = new HashSet<String>();
	Map m = new HashMap<String, Integer>(); 
	
	
	public synchronized boolean sContains(String key){
		return s.contains(key);
	}
	
	public synchronized boolean mContains(String key){
		return m.containsKey(key);
	}
	
	public synchronized void sAdd(String key){
		s.add(key);
	}
	
	public synchronized void mAdd(String key, Integer value){
		m.put(key, value);
	}
	
	public synchronized String mGet(String key){
		return m.get(key).toString();
	}
}
