package org.ihtsdo.conversion.tasks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import org.ihtsdo.conversion.utils.TClosure;
import org.ihtsdo.id.create.IdCreation;


public class GenerateRF1Qualifiers extends AbstractTask{

	private File qualStartStopFile;

	private HashMap<Integer, String> type;

	private HashMap<Integer, ArrayList<Long[]>> domain;

	private HashMap<Integer, ArrayList<String[]>> range;

	private IdCreation idc;

	private String testMode;

	public GenerateRF1Qualifiers(File snapshotConceptFile,
			File qualStartStopFile, File previousQualIdsFile,
			File currentInferRelsFile,
			IdCreation idc,File qualifiersOutputFile, String testMode) {
		super();
		this.qualStartStopFile = qualStartStopFile;
		this.previousQualIdsFile = previousQualIdsFile;
		this.currentInferRelsFile = currentInferRelsFile;
		this.snapshotConceptFile=snapshotConceptFile;
		this.qualifiersOutputFile=qualifiersOutputFile;
		this.idc=idc;
		this.testMode=testMode;
	}

	private HashMap<Integer,ArrayList<Long[]>> domaex;

	private HashMap<String, String> qualIds;

	private File previousQualIdsFile;

	private File currentInferRelsFile;

	private HashMap<Long, String> infRels;

	private TClosure tClos;

	private File snapshotConceptFile;

	private File qualifiersOutputFile;
	
	private Long cont;

	private HashSet<String> tmpRels;

	private HashSet<Long> hConcepts;

	public void setQualStartStopFile(File qualStartStop){
		qualStartStopFile=qualStartStop;
	}

	public void setPreviousQualIdsFile(File previousQualIds){
		previousQualIdsFile=previousQualIds;
	}
	public void setCurrentInferRelsFile(File currentInferRels){
		currentInferRelsFile=currentInferRels;
	}

	public enum hierCondition{Self,DescendantsOrSelf,Descendants};

	public void loadPreviousQualIds() throws IOException{

		FileInputStream rfis = new FileInputStream(previousQualIdsFile	);
		InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
		BufferedReader rbr = new BufferedReader(risr);
		rbr.readLine();
		qualIds=new HashMap<String, String>();
		String line;
		String[] spl;
		while((line=rbr.readLine())!=null){

			spl=line.split("\t",-1);
			if (spl[4].equals("1")){
				qualIds.put(spl[1] + "-" + spl[2] + "-" + spl[3], spl[0]);
			}
		}
		rbr.close();
		rbr=null;
	}

	public void loadCurrentInferRels() throws IOException{

		FileInputStream rfis = new FileInputStream(currentInferRelsFile	);
		InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
		BufferedReader rbr = new BufferedReader(risr);

		rbr.readLine();
		String line;
		String[] spl;
		infRels=new HashMap<Long,String>();
		String types=null;
		while((line=rbr.readLine())!=null){

			spl=line.split("\t",-1);
			if (spl[2].equals("1") && spl[8].equals("900000000000011006") && !spl[7].equals("116680003")){
				Long key=Long.valueOf(spl[4]);
				if (infRels.containsKey(key)){
					types=infRels.get(key);
					types+=spl[7] + "-";
				}else{
					types="-" + spl[7] + "-";
				}
				infRels.put( key,types);
			}
		}
		rbr.close();
		rbr=null;
	}

	public void loadQualStartStop( )throws IOException {

		FileInputStream rfis = new FileInputStream(qualStartStopFile	);
		InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
		BufferedReader rbr = new BufferedReader(risr);

		String line;
		String[] spl;

		type=new HashMap<Integer,String>();
		//		order=new HashMap<String,String>();
		domain=new HashMap<Integer,ArrayList<Long[]>>();
		domaex=new HashMap<Integer,ArrayList<Long[]>>();
		range=new HashMap<Integer,ArrayList<String[]>>();
		ArrayList<Long[]> inte=new ArrayList<Long[]>();
		ArrayList<String[]> stri=null;
		while((line=rbr.readLine())!=null){

			spl=line.split("\t",-1);
			Integer order=Integer.valueOf(spl[2]);
			Long rangeDomainValue=Long.parseLong(spl[6]);
			Long typeId=Long.parseLong(spl[0]);
			if (!hConcepts.contains(rangeDomainValue) || !hConcepts.contains(typeId)){
				continue;
			}
			type.put(order, spl[0]);
			if (spl[3].equals("Domain")){

				if (spl[4].equals("Include")){
					if (domain.containsKey(order)){
						inte=domain.get(order);
					}else{
						inte=new ArrayList<Long[]>();
					}

					inte.add( new Long[]{(long) hierCondition.valueOf(spl[5]).ordinal(),rangeDomainValue});
					domain.put(order,inte);

				}else if (spl[4].equals("Exclude")){

					if (domaex.containsKey(order)){
						inte=domaex.get(order);
					}else{
						inte=new ArrayList<Long[]>();
					}

					inte.add( new Long[]{(long) hierCondition.valueOf(spl[5]).ordinal(),rangeDomainValue});
					domaex.put(order,inte);
				}
			}

			if (spl[3].equals("Range")){
				if (range.containsKey(order)){
					stri=range.get(order);
				}else{
					stri=new ArrayList<String[]>();
				}
				stri.add( new String[]{spl[5],spl[6]});
				range.put(order,stri);
			}
		}
		rbr.close();
		rbr=null;
	}
	public HashMap<Integer, String> getTypeForQualifiers(){
		return type;
	}



	public String getQualifierRF1Row(Integer order2, 
			Long conceptId) throws Exception {
		StringBuffer sb=new StringBuffer("");
		String typeId=type.get(order2);

		List<String[]> stri=range.get(order2);
		String refinability = null;
		String strTypes;
		for (String[] str:stri){

			String conceptId2 = str[1];
			if (tmpRels.contains(conceptId.toString() + "-" + typeId + "-" + conceptId2)){
				continue;
			}
			if (infRels.containsKey(conceptId)){
				strTypes=infRels.get(conceptId);
				if (strTypes.contains("-" + typeId + "-")){
					continue;
				}
			}
			hierCondition hc=hierCondition.valueOf(str[0]);
			switch(hc){
			case Descendants:
				refinability="2";
				break;
			case DescendantsOrSelf:
				refinability="1";
				break;
			case Self:
				refinability="0";

			}
			tmpRels.add(conceptId.toString() + "-" + typeId + "-" + conceptId2);
			String relationshipId = getPreviousQualId(conceptId.toString(),typeId,conceptId2);
			if (relationshipId==null){
				UUID uuid=UUID.randomUUID();
				if (testMode==null){
					relationshipId=idc.getId(uuid).toString();
				}else{
					cont++;
					relationshipId=cont.toString();
				}
			}
			sb.append(relationshipId);
			sb.append("\t");
			sb.append(conceptId);
			sb.append("\t");
			sb.append(typeId);
			sb.append("\t");
			sb.append(conceptId2);
			sb.append("\t");
			sb.append("1");
			sb.append("\t");
			sb.append(refinability);
			sb.append("\t");
			sb.append("0");
			sb.append("\r\n");
		}
		return sb.toString();
	}


	private String getPreviousQualId(String conceptId, String typeId,
			String conceptId2) {
		String strKey=conceptId + "-" + typeId + "-" + conceptId2;

		return qualIds.get(strKey);
	}



	public boolean testDomain(Integer order2,Long testCpt)  throws IOException {

		List<Long[]> domaexc=domaex.get(order2);
		hierCondition[] hierCondValues = hierCondition.values();
		boolean ret=true;
		if (domaexc!=null){

			for(Long[] inte:domaexc){

				hierCondition hc=hierCondValues[inte[0].intValue()];
				switch(hc){
				case Descendants:
					ret=tClos.isAncestorOf(inte[1], testCpt);
					if (ret){
						return false;
					}
					break;
				case DescendantsOrSelf:
					ret=(inte[1].equals(testCpt) || tClos.isAncestorOf(inte[1], testCpt));
					if (ret){
						return false;
					}
					break;
				case Self:
					if (inte[1].equals(testCpt)){
						return false;
					}
				}
			}
		}
		ret=false;
		List<Long[]> domains=domain.get(order2);
		for(Long[] inte:domains){
			hierCondition hc=hierCondValues[inte[0].intValue()];

			switch(hc){
			case Descendants:
				ret=tClos.isAncestorOf(inte[1], testCpt);
				if (ret){
					return true;
				}
				break;
			case DescendantsOrSelf:
				ret=(inte[1].equals(testCpt) || tClos.isAncestorOf(inte[1], testCpt));
				if (ret){
					return true;
				}
				break;
			case Self:
				if (inte[1].equals(testCpt)){
					return true;
				}
			}
		}
		return ret;
	}

	@Override
	public void execute() {
		try {
			cont=0l;
			loadCurrentInferRels();
			if (previousQualIdsFile!=null){
				loadPreviousQualIds();
			}else{
				qualIds=new HashMap<String, String>();
			}
			loadHashConcept();
			loadQualStartStop();
			tClos=new TClosure(currentInferRelsFile.getAbsolutePath());

			FileOutputStream fos = new FileOutputStream( qualifiersOutputFile);
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
			
			analyzeConcepts(bw);
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	private void loadHashConcept() throws IOException{

		FileInputStream rfis = new FileInputStream(snapshotConceptFile	);
		InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
		BufferedReader rbr = new BufferedReader(risr);

		rbr.readLine();
		String line;
		String[] spl;
		hConcepts=new HashSet<Long>();
		while((line=rbr.readLine())!=null){

			spl=line.split("\t",-1);
			if (spl[2].equals("0"))  {

				continue;
			}
			hConcepts.add(Long.parseLong(spl[0]));
		}
		rbr.close();
		rbr=null;
		
	}
	private void analyzeConcepts(BufferedWriter bw) throws Exception {

		String rf1line;
		for (Long conceptId:hConcepts){

			tmpRels=new HashSet<String>();
			for (Integer order:type.keySet()){
				
				if (testDomain(order, conceptId)){
					rf1line=getQualifierRF1Row(order,  conceptId);
					if (!rf1line.trim().equals("")){
						bw.append(rf1line);
					}
				}
			}

		}
	}
}
