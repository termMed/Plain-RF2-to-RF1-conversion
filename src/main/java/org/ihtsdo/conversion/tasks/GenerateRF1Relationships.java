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
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.ihtsdo.conversion.configuration.MetadataConfig;

public class GenerateRF1Relationships extends AbstractTask {

	private static Logger logger = Logger.getLogger(GenerateRF1Relationships.class.getName());
	private File snapshotSortedRelationshipFile;
	private File relationshipOutputFile;

	private HashMap<String,String> RF1charType;
	private String RF2_ISA;
	private File snapshotSortedRetiredIsasRelationshipFile;
	private File metadataConceptListFile;

	public GenerateRF1Relationships(
			File snapshotSortedRelationshipFile,
			File snapshotSortedRetiredIsasRelationshipFile,
			File metadataConceptListFile,
			File relationshipOutputFile) {
		super();
		this.snapshotSortedRelationshipFile = snapshotSortedRelationshipFile;
		this.snapshotSortedRetiredIsasRelationshipFile=snapshotSortedRetiredIsasRelationshipFile;
		this.metadataConceptListFile=metadataConceptListFile;
		this.relationshipOutputFile = relationshipOutputFile;
	}

	public void execute(){

		try {
			long start1 = System.currentTimeMillis();

			getMetadataValues();

			HashSet<String> metadataCpt=getMetadataConceptHash();
			String nextLine;
			String[] splittedLine;

			FileInputStream fis = new FileInputStream(snapshotSortedRelationshipFile	);
			InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
			BufferedReader br = new BufferedReader(isr);
			br.readLine();

			double lines = 0;
			if (relationshipOutputFile.exists()){
				relationshipOutputFile.delete();
			}

			if (!relationshipOutputFile.getParentFile().exists()) {
				relationshipOutputFile.getParentFile().mkdirs();
			}

			FileOutputStream fos = new FileOutputStream( relationshipOutputFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			BufferedWriter bw = new BufferedWriter(osw);

			bw.append("RELATIONSHIPID");
			bw.append("\t");
			bw.append("CONCEPTID1");
			bw.append("\t");
			bw.append("RELATIONSHIPTYPE");
			bw.append("\t");
			bw.append("CONCEPTID2");
			bw.append("\t");
			bw.append("CHARACTERISTICTYPE");
			bw.append("\t");
			bw.append("REFINABILITY");
			bw.append("\t");
			bw.append("RELATIONSHIPGROUP");
			bw.append("\r\n");

			String c1="";
			String rid="";
			String rType="";
			String c2="";
			String rGroup="";
			String cType="";
			String refina;

			refina="";
			//			String cptStatus;
			//			boolean activeIsa;
			while ((nextLine= br.readLine()) != null) {
				splittedLine = nextLine.split("\t");

				c1=splittedLine[4];
				rType=splittedLine[7];
				c2=splittedLine[5];

				if(splittedLine[2].compareTo("1")==0 && !metadataCpt.contains(c1)){
					rid=splittedLine[0];
					rGroup=splittedLine[6];

					if (rType.compareTo(RF2_ISA)==0){
						refina="0";
						cType="0";
						if (rid.compareTo("900000000000021026")==0){
							rid="2764316020";
							c2="138875005";
						}else if(rid.compareTo("3641437023")==0){
							rid="1713614021";
							c2="370115009";
						}
					}else{
						cType=RF1charType.get(splittedLine[8]);
						if (cType.compareTo("0")==0){
							refina="1";
						}else{
							refina="0";
						}
					}
					bw.append(rid);
					bw.append("\t");
					bw.append(c1);
					bw.append("\t");
					bw.append(rType);
					bw.append("\t");
					bw.append(c2);
					bw.append("\t");
					bw.append(cType);
					bw.append("\t");
					bw.append(refina);
					bw.append("\t");
					bw.append(rGroup);
					bw.append("\r\n");
					lines++;

				}
			}
			br.close();
			System.gc();

			// Retired Isas

			if (snapshotSortedRetiredIsasRelationshipFile!=null && snapshotSortedRetiredIsasRelationshipFile.exists()){
				fis = new FileInputStream(snapshotSortedRetiredIsasRelationshipFile	);
				isr = new InputStreamReader(fis,"UTF-8");
				br = new BufferedReader(isr);
				br.readLine();
				rid=""; 
				rGroup="0";
				cType="0";
				refina="0";
				while ((nextLine= br.readLine()) != null) {
					splittedLine = nextLine.split("\t",-1);

					c1=splittedLine[4];
					if(splittedLine[2].compareTo("1")==0 && !metadataCpt.contains(c1)){

						rid=splittedLine[0]; 
						c2=splittedLine[5];
						bw.append(rid);
						bw.append("\t");
						bw.append(c1);
						bw.append("\t");
						bw.append(splittedLine[7]);
						bw.append("\t");
						bw.append(c2);
						bw.append("\t");
						bw.append(cType);
						bw.append("\t");
						bw.append(refina);
						bw.append("\t");
						bw.append(rGroup);
						bw.append("\r\n");

						lines++;
					}
				}
				br.close();
			}
			bw.close();
			bw=null;

			long end1 = System.currentTimeMillis();
			long elapsed1 = (end1 - start1);
			System.out.println("Lines in output file  : " + lines);
			System.out.println("Completed in " + elapsed1 + " ms");
		} catch (FileNotFoundException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (Exception e) {
			// TODO Auto-generated catch block
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

	private void getMetadataValues()  throws Exception {
		MetadataConfig config =new MetadataConfig();

		RF1charType=config.getRF2RF1charTypeMap();
		RF2_ISA=config.getRF2_ISA_RELATIONSHIP();

	}
}
