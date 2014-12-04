package Fetch_Reg_Load;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.*;
import org.apache.commons.codec.binary.Base64;


public class MyCrawlerThread extends Thread{
	
	private final int TIMEOUT = 60; //second

	@Override
	public void run() {
		Data data = null;
		while (true) {
			try {
				data = undoQueue.poll(TIMEOUT, TimeUnit.SECONDS);
			} catch (InterruptedException e) {		
				break;
			}
			if (data == null) {
				//System.out.println("crawler thread timeout");
				break;
			}
			
			if(isGoodURL(data.url)){
				if(rept.mContains(data.url)){
					data.data_voc = rept.mGet(data.url);
				}else{
					data.data_voc = getContent(data.url);
					if(data.data_voc.length() < 20){
						rept.sAdd(data.url);
						continue;
					}else{
						rept.mAdd(data.url, data.id);
					}
				}
				doneQueue.offer(data);
			}
			
		}
		
	}
	

	private List<String> blackList;
	private BlockingQueue<Data> undoQueue;
	private BlockingQueue<Data> doneQueue;
	private RepeatData rept;
	
	private void initBlackList(){
		blackList = new ArrayList<String>();
		blackList.add("mp3");
		blackList.add("wav");
		blackList.add("mp4");
		blackList.add("wmv");
		blackList.add("swf");
		blackList.add("zip");
		blackList.add("rar");
		blackList.add("doc");
		blackList.add("docx");
		blackList.add("xls");
		blackList.add("xlsx");
		blackList.add("ppt");
		blackList.add("pptx");
		blackList.add("pdf");
		blackList.add("txt");
		blackList.add("jpg");
		blackList.add("jpeg");
		blackList.add("gif");
		blackList.add("png");
		blackList.add("ico");
		blackList.add("psd");
		blackList.add("json");
		blackList.add("cab");
		blackList.add("js");
		blackList.add("css");
		blackList.add("dat");
		blackList.add("exe");
	}
	
	public MyCrawlerThread(){
		initBlackList();
	}
	
	public MyCrawlerThread(BlockingQueue<Data> undoQ, BlockingQueue<Data> doneQ, RepeatData r){
		initBlackList();
		undoQueue = undoQ;
		doneQueue = doneQ;
		rept = r;
	}
	
	private boolean judgeByEnds(String url){
		for (String str : blackList) {
			if(url.endsWith(str)) return false;
		}
		return true;
	}
	
	private boolean judgeByHeader(String curl){
		try {
			URL url = new URL(curl);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setConnectTimeout(200);
			connection.setReadTimeout(200);
			connection.setRequestMethod("HEAD");
			if(connection.getResponseCode() == HttpURLConnection.HTTP_OK){
				String head = connection.getHeaderField("Content-Type");
				if(head == null || head.length() == 0) 
					return false;
				if(head.contains("text/html"))
					return true;
			}
		} catch (IOException e) {
			return false;
		} catch (Exception e) {
			System.out.println("Unexpected error in URL:" + curl);
			return false;
		}
		return false;
	}
	
	public boolean isGoodURL(String url){
		if(rept.sContains(url))
			return false;
		if(rept.mContains(url))
			return true;
	
		if(judgeByEnds(url) == false){
			rept.sAdd(url);
			return false;
		}
		if(judgeByHeader(url) == false){
			rept.sAdd(url);
			return false;
		}
		return true;
	}
	
	private String getCharset(HttpURLConnection connection){
		String content = connection.getHeaderField("Content-Type");
		if(content == null || content.length() == 0)
			return "UTF-8";
		String[] strs = content.split(";");
		if(strs.length == 0) 
			return "UTF-8";
		for (String str : strs) {
			str = str.trim();
			if(str.startsWith("charset"))			
				return str.split("=").length == 2?str.split("=")[1]:"UTF-8";
		}
		return "UTF-8";
	}
	
	public String getContent(String curl){
		try {
			URL url = new URL(curl);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setConnectTimeout(1000);
			connection.setReadTimeout(2000);
			connection.setRequestMethod("GET");
			connection.setRequestProperty("User-Agent","Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; InfoPath.1; CIBA)");  
			connection.connect();
			InputStream inputStream = connection.getInputStream();	
			String charset = getCharset(connection);
			//System.out.println(charset);
			String content = inputStream2String(inputStream, charset);
			return Base64.encodeBase64String(content.getBytes());
		} catch (IOException e) {
			return "";
		} catch (Exception e){
			return "";
		}
	}

	private String inputStream2String(InputStream is, String charset) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int i = -1;
		while ((i = is.read()) != -1) {
			baos.write(i);
		}
		return baos.toString(charset);
	}
	public static void main(String[] args){
		//MyCrawlerThread myCrawler = new MyCrawlerThread();
		//System.out.println(myCrawler.isGoodURL("http://www.baidu.com"));
		//System.out.println(myCrawler.getContent("http://www.baidu.com"));
	}
}
