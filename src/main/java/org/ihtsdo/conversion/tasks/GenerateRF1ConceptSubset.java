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

public class GenerateRF1ConceptSubset extends AbstractTask {

	private static Logger logger = Logger.getLogger(GenerateRF1ConceptSubset.class.getName());
	private File snapshotSortedSubsetFile;
	private File outputFile;
	private String subsetId;
	private File metadataConceptListFile;
	
	public GenerateRF1ConceptSubset(File snapshotSortedSubsetFile,
			String subsetId,File metadataConceptListFile, File outputFile){
		this.snapshotSortedSubsetFile=snapshotSortedSubsetFile;
		this.subsetId=subsetId;
		this.metadataConceptListFile=metadataConceptListFile;
		this.outputFile=outputFile;
	}
	
	public void execute(){

		try {
			long start1 = System.currentTimeMillis();

			HashSet<String> metadataCpt=getMetadataConceptHash();
			FileInputStream ifis = new FileInputStream(snapshotSortedSubsetFile);
			InputStreamReader iisr = new InputStreamReader(ifis,"UTF-8");
			BufferedReader ibr = new BufferedReader(iisr);
			
			if (!outputFile.getParentFile().exists()) {
				outputFile.getParentFile().mkdirs();
			}
			
			FileOutputStream fos = new FileOutputStream( outputFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			BufferedWriter bw = new BufferedWriter(osw);

			bw.append("SUBSETID");
			bw.append("\t");
			bw.append("MEMBERID");
			bw.append("\t");
			bw.append("MEMBERSTATUS");
			bw.append("\t");
			bw.append("LINKEDID");
			bw.append("\r\n");
			
			ibr.readLine();

			String nextLine;

			String[] splittedLine;
			Thread currentThread = Thread.currentThread();
			while ((nextLine= ibr.readLine()) != null) {
				if(currentThread.isInterrupted()){
					break;
				}
				splittedLine = nextLine.split("\t",-1);

				if (metadataCpt.contains(splittedLine[5])){
					continue;
				}
				if (splittedLine[2].compareTo("1")==0){
					bw.append(subsetId);
					bw.append("\t");

					bw.append(splittedLine[5]);
					bw.append("\t");
					bw.append("1");
					bw.append("\t");
					bw.append("\r\n");
				}
			}
			
			bw.close();

			ibr.close();
			ifis=null;
			iisr=null;
			ibr=null;
			System.gc();
			
			long end1 = System.currentTimeMillis();
			long elapsed1 = (end1 - start1);
			System.out.println("Completed in " + elapsed1 + " ms");
		} catch (FileNotFoundException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (Exception e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		}
	}


	private HashSet<String> getMetadataConceptHash() throws IOException {
		HashSet<String>mtCpt=new HashSet<String>();
		FileInputStream fis = new FileInputStream(metadataConceptListFile	);
		InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
		BufferedReader br = new BufferedReader(isr);
		br.readLine();
		String nextLine;
		while ((nextLine=br.readLine())!=null){
			
			mtCpt.add(nextLine);
		}
		br.close();
		isr.close();
		fis.close();
		br=null;
		isr=null;
		fis=null;
		System.gc();
		return mtCpt;
	}
}
