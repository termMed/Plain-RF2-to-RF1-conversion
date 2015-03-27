package org.ihtsdo.id.retrieve;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;

import org.ihtsdo.conversion.utils.FileSorter;
import org.ihtsdo.conversion.utils.I_Constants;



public class RetiredConceptRelationship {

	String attrFile;
	String conceptFile;
	String charType;
	String outputFile;
	OutputStreamWriter bw;
	private HashSet<String> conceptSet;
	private File sortedFile;
	private final String refsetInactConcept="900000000000489007";
	private HashMap<String, String> conceptValue;
	private String releaseDate;
	private String moduleId;
	
	public RetiredConceptRelationship(String attrFileSnapshot, String conceptFileSnapshot,
			String charType,String moduleId,String releaseDate,String outputFile) {
		super();
		this.attrFile = attrFileSnapshot;
		this.conceptFile = conceptFileSnapshot;
		this.charType = charType;
		this.outputFile= outputFile;
		this.releaseDate=releaseDate;
		this.moduleId=moduleId;
		
	}

	public void execute()throws IOException {
//		get concept hashset
		getConceptSet();
		
		getOutputWriter();
		
		sortedFile=sortFile(attrFile);
		createFile();
	}

	private File sortFile(String file) {
		File fileOri=new File(file);
		File data=new File("data");
		if (!data.exists()){
			data.mkdirs();
		}
		File temp=new File(data,"temp");
		if (!temp.exists()){
			temp.mkdirs();
		}
		File sortedFileOri= new File("data/Sorted_" + fileOri.getName());
		FileSorter sf=new FileSorter(fileOri,sortedFileOri,temp,new int[]{5,1});
		sf.execute();
		System.gc();

		return sortedFileOri;
	}

	private void createFile() throws IOException {

		FileInputStream enfis = new FileInputStream(sortedFile);
		InputStreamReader enisr = new InputStreamReader(enfis,"UTF-8");
		BufferedReader enbr = new BufferedReader(enisr);
		
		String line=enbr.readLine();
		
		String[] spl;
		conceptValue=new HashMap<String,String>();
		while((line=enbr.readLine())!=null){
			spl=line.split("\t",-1);
			if (spl[2].equals("1") && spl[4].equals(refsetInactConcept)){
				conceptValue.put(spl[5], spl[6]);
			}
		}
		enbr.close();
		enbr=null;
		System.gc();
		String destId="";
		for(String key : conceptSet){
			if (conceptValue.containsKey(key)){
				String retValue=conceptValue.get(key);
				if (retValue.equals(I_Constants.DUPLICATE))
					destId = I_Constants.DUPLICATE_CONCEPT;
				else if (retValue.equals(I_Constants.AMBIGUOUS))
					destId = I_Constants.AMBIGUOUS_CONCEPT;
				else if (retValue.equals(I_Constants.OUTDATED))
					destId = I_Constants.OUTDATED_CONCEPT;
				else if (retValue.equals(I_Constants.ERRONEOUS))
					destId = I_Constants.ERRONEOUS_CONCEPT;
				else if (retValue.equals(I_Constants.LIMITED))
					destId = I_Constants.LIMITED_CONCEPT;
				else if (retValue.equals(I_Constants.MOVED_ELSE_WHERE))
					destId = I_Constants.MOVED_ELSEWHERE_CONCEPT;
				else 
					destId = I_Constants.REASON_NOT_STATED_CONCEPT;
			}else{
				destId = I_Constants.REASON_NOT_STATED_CONCEPT;
			}
		
			writeRF2TypeLine( releaseDate, "1", moduleId, key, destId, "0", I_Constants.ISA,
				charType, I_Constants.SOMEMODIFIER);
		}
	}

	private void writeRF2TypeLine(String releaseDate, String status,
			String moduleId, String sourceId, String destId, String group,
			String type, String charType, String modifierId) throws IOException {
		 bw.append( "\t" + releaseDate + "\t" + status + "\t" + moduleId + "\t" + sourceId + "\t" + destId + "\t" + group + "\t" + type
			+ "\t" + charType + "\t" + modifierId);
		 bw.append("\r\n");
		 
	}

	private void getConceptSet() throws IOException {
		conceptSet=new HashSet<String>();

		FileInputStream enfis = new FileInputStream(conceptFile);
		InputStreamReader enisr = new InputStreamReader(enfis,"UTF-8");
		BufferedReader enbr = new BufferedReader(enisr);
		
		String line=enbr.readLine();
		
		String[] spl;
		while((line=enbr.readLine())!=null){
			spl=line.split("\t",-1);
			if (spl[2].equals("0")){
				conceptSet.add(spl[0]);
			}
		}
		enbr.close();
		enbr=null;
		System.gc();
	}

	private void getOutputWriter() {
		try {
			bw=new OutputStreamWriter(new FileOutputStream(outputFile));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}		
	}
	
}
