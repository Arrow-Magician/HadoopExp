package MovieActor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import com.alibaba.fastjson.JSON;

public class Main {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader br = new BufferedReader(new FileReader("E:\\金峰\\学习\\大三\\Hadoop\\Hadoop部署实践\\Film.json"));
		FileWriter fw = new FileWriter(new File("E:\\金峰\\学习\\大三\\Hadoop\\Hadoop部署实践\\Film1.csv"));
		
		MovieInfo m = null;
		String line = null;
		while((line = br.readLine())!=null) {
			m = JSON.parseObject(line, MovieInfo.class);
			if(m.getActor().indexOf("山口胜平")!=-1) {
				String mId = m.getFilm_page();
				String[] actors = m.getActor().split(",");
				
				for(String ac : actors) {
					if(ac.equals("山口胜平")) {
						continue;
					}
					fw.append(mId + "," + m.getTitle() + "," + ac + ","+m.getStar() + "\n");
					
				}
			}
		}

	}

}
