package Fetch_Reg_Load;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;

import de.tototec.cmdoption.CmdOption;
import de.tototec.cmdoption.CmdlineParser;
import de.tototec.cmdoption.CmdlineParserException;

import org.lionsoul.jcseg.core.DictionaryFactory;
import org.lionsoul.jcseg.util.Util;

class fileSeg extends Thread{
	int max;
	String pre_f;
	LinkedBlockingQueue<Data> qUndo;
	
	public fileSeg(LinkedBlockingQueue<Data> qUndo, String pre_f, int max){
		this.max = max;
		this.pre_f = pre_f;
		this.qUndo = qUndo;
	}
	
	@Override
	public void run(){
		try {
			System.out.println("\033[1;31;40m fileSeg thread is running... \033[0m");
			
			String tmpStr = null;
			BufferedReader reader = null;
			for(int i=0; i<=max; i++){
				File f = new File(pre_f +i);
				if(!f.exists())
					continue;
				try{
					System.out.println(String.format("\033[1;32;40m Will open file: %s \033[0m", f.getName()));
					reader = new BufferedReader(new FileReader(f));
					while((tmpStr = reader.readLine()) != null){
						while(qUndo.size() > 500){
							Thread.sleep(1000);
							System.out.println(String.format("Undo queue size %d.",qUndo.size()));
						}
						Data t = new Data();
						t.id = Integer.parseInt(tmpStr.split(" ")[0]);
						t.data_voc = tmpStr.split(" ")[1];
						qUndo.add(t);
					}
				}catch(IOException e){
					e.printStackTrace();
					continue;
				}finally{
					if(reader != null){
						reader.close();
					}
				}
			}
			
			System.out.println("\033[1;31;40m fileSeg thread is done. \033[0m");
		} catch (Exception e1) {
			e1.printStackTrace();
		}
	}
}
public class seg_html_into_file {
	int tNum;
	int max;
	String pre_f;
	
	LinkedBlockingQueue<Data> qUndo;
	
	private void start_fileSeg() {
		
		this.qUndo = new LinkedBlockingQueue<Data>();
		Thread[] t = new Thread[tNum];
		
		for (int i=0;i<tNum; i++){
			t[i] = new Seg_html(qUndo, "seg"+i);
			t[i].start();
		}
		
		try{
			Thread.sleep(5000);
		} catch (InterruptedException e) {	
			e.printStackTrace();
		}
		
		(new fileSeg(qUndo, pre_f, max)).start();
		
	}
	
	
	public static void main(String[] args) {	
		Config config = new Config();
		CmdlineParser cp = new CmdlineParser(config);
		
		try{
			cp.parse(args);
		} catch (CmdlineParserException e){
			System.err.println("Error: " +e.getLocalizedMessage());
			System.exit(1);
		}
		
		if(config.help){
			cp.usage();
			System.exit(0);
		}
		
		seg_html_into_file m = new seg_html_into_file();
		m.pre_f = config.pre_filename;
		m.max = config.max;
		m.tNum = config.tNum; //分词线程数
		m.start_fileSeg();
	}
	
	public static class Config {
		@CmdOption(names = {"--help", "-h"}, description = "show usage.", isHelp=true)
		public boolean help;
		
		@CmdOption(names = {"--pre_Filename", "-pf"}, args = {"[pre Filename]"}, description = "文件名前缀", minCount=1, maxCount=-1)
		public String pre_filename;
		
		@CmdOption(names = {"--max_FileNumber", "-m"}, args = {"[100]"}, description = "文件名后缀最大值", minCount=1, maxCount=-1)
		public Integer max;
		
		@CmdOption(names = {"--threadNum", "-tn"}, args = {"[thread Number]"}, description="线程数", maxCount=-1)
		public Integer tNum = 1;
	}

}
