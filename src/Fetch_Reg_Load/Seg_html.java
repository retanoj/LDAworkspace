package Fetch_Reg_Load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.lionsoul.jcseg.ASegment;
import org.lionsoul.jcseg.core.ADictionary;
import org.lionsoul.jcseg.core.DictionaryFactory;
import org.lionsoul.jcseg.core.IWord;
import org.lionsoul.jcseg.core.JcsegException;
import org.lionsoul.jcseg.core.JcsegTaskConfig;
import org.lionsoul.jcseg.core.SegmentFactory;

public class Seg_html extends Thread{
	
	private BlockingQueue<Data> undoQueue;
	private final int TIMEOUT = 30; 
	private final String STOPWORD = "./lib/stopword.txt";
	private HtmlFilter filter;
	private HashSet<String> stopSet;
	ASegment seg = null;
	private Writer w;
	String name;
	
	@Override
	public void run() {
		try{
			ChineseSeg("");
		} catch(Exception e){
			;
		}
		
		Data data = null;
		try{
			while (true) {
				try {
					data = undoQueue.poll(TIMEOUT, TimeUnit.SECONDS);
					if(data == null) 
						break;
				} catch (InterruptedException e) {
					break;
				}	
				if(data.data_voc == null || data.data_voc.length() == 0 )
					continue;
				
				String html = decodeBase64(data.data_voc);
				
				// filter html
				String afteFilter = "";
				try {
					afteFilter = filter.filterHtml(html);
				} catch (Exception e) {
					continue;
				}
				
				// seg
				String afterSeg = "";
				try {
					afterSeg = ChineseSeg(afteFilter);
				} catch (Exception e) {
					continue;
				}
				if(afterSeg.trim().length() < 20)
					continue;
				
				data.data_voc = afterSeg.trim();
				
				// write into file
				try {
					w.write(data.id + "|" +data.data_voc +'\n');
				} catch (IOException e) {
					e.printStackTrace();
					continue;
				}
			}	
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			System.out.println(String.format("%s is done.", this.name));
			try {
				w.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	public Seg_html(BlockingQueue<Data> undoQ, String name){
		this.undoQueue = undoQ;
		this.name = name;
		try{
			File f = new File(name);
			f.createNewFile();
			w = new OutputStreamWriter(new FileOutputStream(f), "UTF-8");
		}catch(Exception e){
			e.printStackTrace();
			return ;
		}		
		filter = new HtmlFilter();
		initStopWord();	
		initSeg();
	}
	public Seg_html(){
		initSeg();
	}
	
	private void initSeg(){
		JcsegTaskConfig config = new JcsegTaskConfig();
		ADictionary dic = DictionaryFactory.createDefaultDictionary(config,false);
		try {
			dic.loadFromLexiconDirectory(config.getLexiconPath()[0]);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		System.out.println(config.getLexiconPath()[0]);
		try {
			seg = (ASegment) SegmentFactory
					.createJcseg(JcsegTaskConfig.COMPLEX_MODE,
					new Object[]{config, dic});
		} catch (JcsegException e) {
			e.printStackTrace();
		}
	}
		
	private void initStopWord(){
		stopSet = new HashSet<String>();
		File file = new File(STOPWORD);
		//HashSet<String> stopword = new HashSet<String>();
		try {
			Reader reader = new InputStreamReader(new FileInputStream(file), "UTF-8");
			BufferedReader bReader = new BufferedReader(reader);
			while(true){
				String line = bReader.readLine();
				if(line == null)
					break;
				if(line.trim().length() == 0)
					continue;
				stopSet.add(line.trim());
			}
			bReader.close();
		} catch (IOException e) {
			System.out.println("File " + STOPWORD + " not found.");;
			return ;
		}
	}
	
	public String decodeBase64(String base64){
		try {
			return new String(Base64.decodeBase64(base64), "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return "";
		}
	}
	
	public String ChineseSeg(String content) throws Exception{
		//List<Term> resulTerms = ToAnalysis.parse(content);
		seg.reset(new StringReader(content));
		IWord word = null;
		StringBuffer result = new StringBuffer();
		while ((word = seg.next()) != null) {
			if (isGoodWord(word.getValue())) {
				result.append(word.getValue());
				result.append(" ");
			}
		}
		return result.toString();
	}
	
	private boolean isChinese(char c){
		if(c >= '\u4e00' && c <= '\u9fa5')
			return true;
		else
			return false;
	}
	private boolean isNumber(char c){
		if(c >= '\u0030' && c <= '\u0039')
			return true;
		else 
			return false;
	}
	private boolean isLetter(char c){
		if((c >= '\u0041' && c <= '\u005a') || (c >= '\u0061' && c <= '\u007a'))
			return true;
		else 
			return false;
	}
	private boolean isOther(char c){
		if(isChinese(c))
			return false;
		else 
			return true;
	}
	private boolean isGoodWord(String word){
		if(word.length() == 1)
			return false;
		if(stopSet.contains(word.trim())){
			return false;
		}
		for(int i = 0; i < word.length(); i++){
			char c = word.charAt(i);
			if(isOther(c))
				return false;
		}
		return true;
	}
	
	public static void main(String[] args) throws IOException{
		Seg_html s = new Seg_html();
		
	}	
}
class HtmlFilter{
	private Pattern blankLine, cData, script, style, br, h, comment, body, charEntity;
	public HtmlFilter(){
		blankLine = Pattern.compile("[\n|\r|\t| ]+");
		cData = Pattern.compile("//<!\\[CDATA\\[[^>]*//\\]\\]>");
		script = Pattern.compile("<\\s*script[^>]*>[\\s\\S]*?<\\s*/\\s*script\\s*>");
		style = Pattern.compile("<\\s*style[^>]*>[\\s\\S]*?<\\s*/\\s*style\\s*>");
		br = Pattern.compile("<br\\s*?/?>");
		h = Pattern.compile("</?\\w+[^>]*>");
		comment = Pattern.compile("<[!|#]--.*?-->");
		body = Pattern.compile("<\\s*body[^>]*?>([\\s\\S]*?)<\\s*/\\s*body");
		charEntity = Pattern.compile("&#?(?<name>\\w+);");
	}
	private String replaceCharEntity(String input){
		HashMap<String,String> map = new HashMap<String,String>(){};
		map.put("nbsp", " ");
		map.put("160", " ");
		map.put("lt", "<");
		map.put("60", "<");
		map.put("gt", ">");
		map.put("62", ">");
		map.put("amp", "&");
		map.put("38", "&");
		map.put("quot", "\"");
		map.put("34", "\"");
		Matcher matcher = charEntity.matcher(input);
		while(matcher.find()){
			String key = matcher.group("name");
			if(map.containsKey(key))
				input = matcher.replaceFirst(map.get(key));
			else 
				input = matcher.replaceFirst("");
			matcher = charEntity.matcher(input);
		}
		return input;
	}
	private String filterTag(String input){
		Matcher matcher = body.matcher(input);
		if(matcher.find())
			input = matcher.group(1);
		input = cData.matcher(input).replaceAll("");
		input = script.matcher(input).replaceAll("");
		input = style.matcher(input).replaceAll("");
		input = br.matcher(input).replaceAll("\n");
		input = h.matcher(input).replaceAll("");
		input = comment.matcher(input).replaceAll("");
		input = blankLine.matcher(input).replaceAll("\n");
		
		return input;
	}
	public String filterHtml(String input) throws Exception{
		String output = filterTag(input);
		return replaceCharEntity(output);
	}
}
