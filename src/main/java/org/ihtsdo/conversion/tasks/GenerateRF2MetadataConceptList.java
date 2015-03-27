
package org.ihtsdo.conversion.tasks;

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
import org.ihtsdo.conversion.configuration.MetadataConfig;

public class GenerateRF2MetadataConceptList extends AbstractTask{


	private static Logger logger = Logger.getLogger(GenerateRF2MetadataConceptList.class.getName());
	private static final String ISA_SCTID = "116680003";
	private File relationshipFile;
	private HashSet<String> parentConcepts;
	private File descendantsFile;
	private HashSet<String> excludeParentsConcepts;
	private String metadataModelSCTID;
	private String namespaceCptSCTID;
	private String linkageCptSCTID;


	public GenerateRF2MetadataConceptList( File relationshipFile, File metadataConceptsFile) {
		super();
		try {
			getMetadataValues();
			this.parentConcepts=new HashSet<String>();
			this.parentConcepts.add(metadataModelSCTID);

			this.excludeParentsConcepts=new HashSet<String>();
			this.excludeParentsConcepts.add(namespaceCptSCTID);
			this.excludeParentsConcepts.add(linkageCptSCTID);
		} catch (Exception e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		}
		this.relationshipFile=relationshipFile;
		this.descendantsFile=metadataConceptsFile;
	}


	public void execute(){

		try {
			long start1 = System.currentTimeMillis();

			String nextLine;
			String[] splittedLine;

			if (descendantsFile.exists())
				descendantsFile.delete();

			HashSet<String> toExclude = new HashSet<String>();
			for (String key:excludeParentsConcepts){
				toExclude.add(key);
			}
			Thread currentThread = Thread.currentThread();

			HashSet<String> toFile = new HashSet<String>();
			if (relationshipFile!=null){
				while (excludeParentsConcepts.size()>0){
					if(currentThread.isInterrupted()){
						break;
					}
					HashSet<String> child = new HashSet<String>();

					FileInputStream rfis = new FileInputStream(relationshipFile	);
					InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
					BufferedReader rbr = new BufferedReader(risr);


					rbr.readLine();

					nextLine=null;
					splittedLine=null;

					while ((nextLine= rbr.readLine()) != null) {
						if(currentThread.isInterrupted()){
							rbr.close();
							rfis=null;
							risr=null;
							System.gc();
							return;
						}
						splittedLine = nextLine.split("\t",-1);
						if (splittedLine[7].compareTo(ISA_SCTID)==0
								&& excludeParentsConcepts.contains(splittedLine[5])
								&& splittedLine[2].compareTo("1")==0){
							toExclude.add(splittedLine[4]);
							child.add(splittedLine[4]);
						}
					}
					rbr.close();
					rfis=null;
					risr=null;
					System.gc();
					excludeParentsConcepts=child;
				}
				for (String key:parentConcepts){
					toFile.add(key);
				}
				while (parentConcepts.size()>0){
					if(currentThread.isInterrupted()){
						break;
					}
					HashSet<String> child = new HashSet<String>();

					FileInputStream rfis = new FileInputStream(relationshipFile	);
					InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
					BufferedReader rbr = new BufferedReader(risr);


					rbr.readLine();

					nextLine=null;
					splittedLine=null;

					while ((nextLine= rbr.readLine()) != null) {
						if(currentThread.isInterrupted()){
							rbr.close();
							rfis=null;
							risr=null;
							System.gc();
							return;
						}
						splittedLine = nextLine.split("\t",-1);
						if (splittedLine[7].compareTo(ISA_SCTID)==0
								&& parentConcepts.contains(splittedLine[5])
								&& splittedLine[2].compareTo("1")==0
								&& !toExclude.contains(splittedLine[4])){
							toFile.add(splittedLine[4]);
							child.add(splittedLine[4]);
						}
					}
					rbr.close();
					rfis=null;
					risr=null;
					System.gc();
					parentConcepts=child;
				}
			}
			if (!descendantsFile.getParentFile().exists()) {
				descendantsFile.getParentFile().mkdirs();
			}

			FileOutputStream fos = new FileOutputStream( descendantsFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			BufferedWriter bw = new BufferedWriter(osw);

			bw.append("COMPONENTID");
			bw.append("\r\n");
			int lineNum = 0;
			for (String desc:toFile){
				if(currentThread.isInterrupted()){
					break;
				}
				bw.append(desc);
				bw.append("\r\n");
				lineNum++;
			}
			bw.close();
			bw=null;
			osw=null;
			fos=null;
			long end1 = System.currentTimeMillis();
			long elapsed1 = (end1 - start1);
			logger.log(org.apache.log4j.Level.INFO,lineNum + " lines in output file  : " + descendantsFile.getAbsolutePath());
			System.out.println("Completed in " + elapsed1 + " ms");

		} catch (FileNotFoundException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (Exception e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		}
	}

	private void getMetadataValues()  throws Exception {
		MetadataConfig config =new MetadataConfig();

		metadataModelSCTID=config.getMetadataModelSCTID();
		namespaceCptSCTID=config.getNamespaceCptSCTID();
		linkageCptSCTID=config.getLinkageCptSCTID();

	}

}
