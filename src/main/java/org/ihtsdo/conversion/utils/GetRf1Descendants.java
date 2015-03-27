
package org.ihtsdo.conversion.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.ihtsdo.conversion.tasks.AbstractTask;

public class GetRf1Descendants extends AbstractTask {

	private static Logger logger = Logger.getLogger(GetRf1Descendants.class.getName());
	private static final String ISA_SCTID = "116680003";
	private File relationshipFile;
	private HashSet<String> parentConcepts;
	private File descendantsFile;


	public GetRf1Descendants(HashSet<String>parentConcepts, File relationshipFile, File descendantsFile) {
		super();
		this.parentConcepts=parentConcepts;
		this.relationshipFile=relationshipFile;
		this.descendantsFile=descendantsFile;
	}


	public void execute(){

		try {
			long start1 = System.currentTimeMillis();

			String nextLine;
			String[] splittedLine;
			double lines = 0;
			
			if (descendantsFile.exists())
				descendantsFile.delete();
			
			FileOutputStream fos = new FileOutputStream( descendantsFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			BufferedWriter bw = new BufferedWriter(osw);

			bw.append("COMPONENTID");
			bw.append("\r\n");
			HashSet<String> toFile = new HashSet<String>();
			for (String key:parentConcepts){
				toFile.add(key);
			}
			while (parentConcepts.size()>0){

				HashSet<String> child = new HashSet<String>();
				
				FileInputStream rfis = new FileInputStream(relationshipFile	);
				InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
				BufferedReader rbr = new BufferedReader(risr);


				rbr.readLine();

				nextLine=null;
				splittedLine=null;

				while ((nextLine= rbr.readLine()) != null) {
					splittedLine = nextLine.split("\t",-1);
					if (splittedLine[2].compareTo(ISA_SCTID)==0
							&& parentConcepts.contains(splittedLine[3])){
						toFile.add(splittedLine[1]);
						child.add(splittedLine[1]);
					}
				}
				rbr.close();
				rfis=null;
				risr=null;
				System.gc();
				parentConcepts=child;
			}
			for (String desc:toFile){
				bw.append(desc);
				bw.append("\r\n");
			}
			bw.close();
			bw=null;
			osw=null;
			fos=null;
			long end1 = System.currentTimeMillis();
			long elapsed1 = (end1 - start1);
			System.out.println(lines + " lines in output file  : " + descendantsFile.getAbsolutePath());
			System.out.println("Completed in " + elapsed1 + " ms");

		} catch (FileNotFoundException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (Exception e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		}
	}


}
