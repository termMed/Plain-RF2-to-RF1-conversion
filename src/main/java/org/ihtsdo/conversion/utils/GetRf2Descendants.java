
package org.ihtsdo.conversion.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashSet;

public class GetRf2Descendants {

	private static final String ISA_SCTID = "116680003";
	private File relationshipFile;
	private HashSet<String> parentConcepts;
	private File descendantsFile;


	public GetRf2Descendants(HashSet<String>parentConcepts, File relationshipFile, File descendantsFile) {
		super();
		this.parentConcepts=parentConcepts;
		this.relationshipFile=relationshipFile;
		this.descendantsFile=descendantsFile;
	}


	public HashSet<String> execute() throws IOException{

		long start1 = System.currentTimeMillis();

		String nextLine;
		String[] splittedLine;
		double lines = 0;
		BufferedWriter bw=null;
		if (descendantsFile!=null){
			if (descendantsFile.exists())
				descendantsFile.delete();

			FileOutputStream fos = new FileOutputStream( descendantsFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			bw = new BufferedWriter(osw);

			bw.append("COMPONENTID");
			bw.append("\r\n");
		}
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
				if (splittedLine[7].compareTo(ISA_SCTID)==0
						&& splittedLine[2].equals("1")
						&& parentConcepts.contains(splittedLine[5])){
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

		if (descendantsFile!=null){
			for (String desc:toFile){
				bw.append(desc);
				bw.append("\r\n");
			}
			bw.close();
			bw=null;
		}
		long end1 = System.currentTimeMillis();
		long elapsed1 = (end1 - start1);
		System.out.println(lines + " lines in output file  : " + descendantsFile.getAbsolutePath());
		System.out.println("Completed in " + elapsed1 + " ms");

		return toFile;

	}


}
