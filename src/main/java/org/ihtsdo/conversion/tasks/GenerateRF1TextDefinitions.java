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

public class GenerateRF1TextDefinitions extends AbstractTask {

	private static Logger logger = Logger.getLogger(GenerateRF1TextDefinitions.class.getName());
	private File snapshotSortedTextDefinitionFile;
	private File outputFile;
	private File metadataConceptListFile;
	private File sortedRF1ConceptFile;

	public GenerateRF1TextDefinitions(
			File snapshotSortedTextDefinitionFile,
			File sortedRF1ConceptFile,
			File metadataConceptListFile,
			File outputFile) {
		super();
		this.snapshotSortedTextDefinitionFile = snapshotSortedTextDefinitionFile;
		this.sortedRF1ConceptFile=sortedRF1ConceptFile;
		this.metadataConceptListFile=metadataConceptListFile;
		this.outputFile = outputFile;
	}

	public void execute(){

		try {
			long start1 = System.currentTimeMillis();

			HashSet<String> metadataCpt=getMetadataConceptHash();
			String nextLine;
			String[] splittedLine;

			FileInputStream fis = new FileInputStream(snapshotSortedTextDefinitionFile	);
			InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
			BufferedReader br = new BufferedReader(isr);
			double lines = 0;
			br.readLine();

			FileInputStream cfis = new FileInputStream(sortedRF1ConceptFile	);
			InputStreamReader cisr = new InputStreamReader(cfis,"UTF-8");
			BufferedReader cbr = new BufferedReader(cisr);
			cbr.readLine();

			if (outputFile.exists()){
				outputFile.delete();
			}
			
			if (!outputFile.getParentFile().exists()) {
				outputFile.getParentFile().mkdirs();
			}
			
			FileOutputStream fos = new FileOutputStream( outputFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			BufferedWriter bw = new BufferedWriter(osw);

			bw.append("CONCEPTID");
			bw.append("\t");
			bw.append("SNOMEDID");
			bw.append("\t");
			bw.append("FULLYSPECIFIEDNAME");
			bw.append("\t");
			bw.append("DEFINITION");
			bw.append("\r\n");

			String c1="";
			String term="";
			String cLine=cbr.readLine();
			String[] cSplittedLine=new String[]{};
			String cid="";
			String snomedId="";
			String fsn="";
			int comp;
			if (cLine!=null){
				cSplittedLine=cLine.split("\t",-1);
				cid=cSplittedLine[0];
			}
			while ((nextLine= br.readLine()) != null) {
				splittedLine = nextLine.split("\t");

				c1=splittedLine[4];
				if( splittedLine[2].compareTo("1")==0 && !metadataCpt.contains(c1)){
					term=splittedLine[7];

					if (cLine!=null){
						comp=cid.compareTo(c1);
						snomedId="";
						fsn="";
						while (comp<0){
							cLine=cbr.readLine();
							if (cLine==null){
								break;
							}
							cSplittedLine=cLine.split("\t",-1);
							cid=cSplittedLine[0];
							comp=cid.compareTo(c1);
						}		
						if (comp==0){
							snomedId=cSplittedLine[4];
							fsn=cSplittedLine[2];

							bw.append(c1);
							bw.append("\t");
							bw.append(snomedId);
							bw.append("\t");
							bw.append(fsn);
							bw.append("\t");
							bw.append(term);
							bw.append("\r\n");
							lines++;
						}
					}
				}
			}
			br.close();
			cbr.close();
			bw.close();

			bw=null;
			System.gc();

			long end1 = System.currentTimeMillis();
			long elapsed1 = (end1 - start1);
			System.out.println("Lines in output file  : " + lines);
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
