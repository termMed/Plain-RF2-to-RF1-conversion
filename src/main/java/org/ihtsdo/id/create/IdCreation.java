package org.ihtsdo.id.create;

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
import java.util.List;
import java.util.UUID;

import org.apache.log4j.Logger;

public class IdCreation {
	private  IdAssignmentImpl stIdGen;
	String componentFile;
	int namespaceId;
	long partitionId;
	String endPointURL;
	String username;
	String pass;
	private String releaseDate;
	private String uuidsMapFile;
	private static final Logger log = Logger.getLogger(IdCreation.class);
	
	public static void main(String[] args){
//		String componentFile="/Users/ar/git/qa2/outputfolder/outputSnapshot/sct2_Description_SpanishExtensionSnapshot-es_INT_20141031.txt";
//		String uuidsMapFile="/Users/ar/git/qa2/outputfolder/uuids_sctid_desc_map.txt";
//		int namespaceId=0;
//		Long partitionId=1l;
//		String endPointURL="http://mgr.servers.aceworkspace.net:50042/axis2/services/id_generator";
//		String username="userName";
//		String pass="passw";
//		String releaseDate="20141031";
//		IdCreation idc=new IdCreation(componentFile, uuidsMapFile, namespaceId, partitionId, endPointURL, username, pass, releaseDate);
//		try {
//			idc.execute();
//			componentFile="/Users/ar/git/qa2/outputfolder/sct2_Description_SpanishExtensionSnapshot-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,0);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputSnapshot/sct2_Description_SpanishExtensionSnapshot-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,0);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputFull/sct2_Description_SpanishExtensionFull-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,0);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputDelta/sct2_Description_SpanishExtensionDelta-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,0);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputSnapshot/der2_cRefset_LanguageSpanishExtensionSnapshot-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,5);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputFull/der2_cRefset_LanguageSpanishExtensionFull-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,5);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputDelta/der2_cRefset_LanguageSpanishExtensionDelta-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,5);
//			
//			idc=null;
//			
//
//			componentFile="/Users/ar/git/qa2/outputfolder/outputSnapshot/sct2_TextDefinition_SpanishExtensionSnapshot-es_INT_20141031.txt";
//			uuidsMapFile="/Users/ar/git/qa2/outputfolder/uuids_sctid_textDef_map.txt";
//			idc=new IdCreation(componentFile, uuidsMapFile, namespaceId, partitionId, endPointURL, username, pass, releaseDate);
//
////			idc.execute();
//			componentFile="/Users/ar/git/qa2/outputfolder/outputSnapshot/sct2_TextDefinition_SpanishExtensionSnapshot-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,0);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputFull/sct2_TextDefinition_SpanishExtensionFull-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,0);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputDelta/sct2_TextDefinition_SpanishExtensionDelta-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,0);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputSnapshot/der2_cRefset_LanguageSpanishExtensionSnapshot-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,5);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputFull/der2_cRefset_LanguageSpanishExtensionFull-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,5);
//			componentFile="/Users/ar/git/qa2/outputfolder/outputDelta/der2_cRefset_LanguageSpanishExtensionDelta-es_INT_20141031.txt";
//			idc.replaceSctIds(componentFile, uuidsMapFile,5);
//			
//			idc=null;
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
	}
	public void createIdsFromFile(String componentFile,String uuidsMapFile, int namespaceId,
			long partitionId, String endPointURL, String username, String pass,
			String releaseDate, String testMode) throws IOException{

		this.componentFile = componentFile;
		this.namespaceId = namespaceId;
		this.partitionId = partitionId;
		this.endPointURL = endPointURL;
		this.username = username;
		this.pass = pass;
		this.releaseDate = releaseDate;
		
		IdAssignmentBI idAssignment = new IdAssignmentImpl(endPointURL,username,pass);

		File relFile=new File(componentFile);
		FileInputStream rfis = new FileInputStream(componentFile);
		InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
		BufferedReader rbr = new BufferedReader(risr);
		String header=rbr.readLine();

		String line;
		String[] spl;
		int i;
		UUID uuid;
		List<UUID> list = new ArrayList<UUID>();
		HashMap<String, UUID> uuidSctId = new HashMap<String,UUID>();

		while((line=rbr.readLine())!=null){
			spl=line.split("\t",-1);
			if (spl[0].contains("-")){
				uuid=UUID.randomUUID();
				list.add(uuid);
				uuidSctId.put(spl[0],uuid);

			}
		}
		rbr.close();
		rbr=null;
		HashMap<UUID, Long> sctIdMap = new HashMap<UUID,Long>();
		String sPart=("0" + String.valueOf(partitionId)).substring(0, 2);
		try {
			if (testMode==null){
			sctIdMap = idAssignment.createSCTIDList(list, namespaceId, sPart, releaseDate, releaseDate, releaseDate);
			}else{
				sctIdMap=assignDummyIds(list);
			}
		} catch (Exception cE) {
			log.error("Message : SCTID creation error for list " , cE);
		}
		File finalRelFile=new File(relFile.getParent(),"newIds_" + relFile.getName());
		rfis = new FileInputStream(componentFile);
		risr = new InputStreamReader(rfis,"UTF-8");
		rbr = new BufferedReader(risr);
		header=rbr.readLine();


		FileOutputStream fos = new FileOutputStream( finalRelFile);
		OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
		BufferedWriter bw = new BufferedWriter(osw);
		bw.append(header);
		bw.append("\r\n");


		File mapFile=new File(uuidsMapFile);
		FileOutputStream fosm = new FileOutputStream( mapFile);
		OutputStreamWriter oswm = new OutputStreamWriter(fosm,"UTF-8");
		BufferedWriter bwm = new BufferedWriter(oswm);
		bwm.append("id");
		bwm.append("\t");
		bwm.append("uuid");
		bwm.append("\t");
		bwm.append("sctId");
		bwm.append("\r\n");

		while((line=rbr.readLine())!=null){
			spl=line.split("\t",-1);
			if (uuidSctId.containsKey(spl[0])){
				uuid=uuidSctId.get(spl[0]);
				Long id=sctIdMap.get(uuid);

				bwm.append(spl[0]);
				bwm.append("\t");
				bwm.append(uuid.toString());
				bwm.append("\t");
				if (id!=null){
					bwm.append(id.toString());
					bwm.append("\r\n");

					bw.append(id.toString());
					for (i=1;i<spl.length;i++){
						bw.append("\t");
						bw.append(spl[i]);
					}
					bw.append("\r\n");
				}else{
					bwm.append("null");
					bwm.append("\r\n");

					bw.append(line);
					bw.append("\r\n");
				}
			}else{
				bw.append(line);
				bw.append("\r\n");
			}
		}
		bwm.close();
		bwm=null;
		bw.close();
		bw=null;
		rbr.close();
		rbr=null;

		if (relFile.exists()){
			String inputFile=relFile.getAbsolutePath();
			File reconFile=new File(relFile.getParent(),"Previous_" + relFile.getName());
			relFile.renameTo(reconFile);
			File outFile= new File(inputFile);
			finalRelFile.renameTo(outFile);
			
		}
	}

	private HashMap<UUID, Long> assignDummyIds(List<UUID> list) {
		HashMap<UUID, Long> ret=new HashMap<UUID, Long>();
		Long cont=1l;
		for (UUID uuid:list){
			ret.put(uuid, cont);
			cont++;
		}
		return ret;
	}
	public void replaceSctIds(String componentFile ,String uuidsMapFile, int index ) throws IOException{

		FileInputStream rfis = new FileInputStream(uuidsMapFile);
		InputStreamReader risr = new InputStreamReader(rfis,"UTF-8");
		BufferedReader rbr = new BufferedReader(risr);
		String header=rbr.readLine();
		String line;
		String[] spl;
		HashMap<String, String> uuidSctId = new HashMap<String,String>();
		while((line=rbr.readLine())!=null){
			spl=line.split("\t",-1);
			uuidSctId.put(spl[0],spl[2]);
		}
		rbr.close();


		File relFile=new File(componentFile);
		File finalRelFile=new File(relFile.getParent(),"newIds_" + relFile.getName());

		FileOutputStream fos = new FileOutputStream( finalRelFile);
		OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
		BufferedWriter bw = new BufferedWriter(osw);

		rfis = new FileInputStream(componentFile);
		risr = new InputStreamReader(rfis,"UTF-8");
		rbr = new BufferedReader(risr);
		header=rbr.readLine();

		bw.append(header);
		bw.append("\r\n");
		String sctid;

		while((line=rbr.readLine())!=null){
			spl=line.split("\t",-1);
			if (uuidSctId.containsKey(spl[index])){
				sctid=uuidSctId.get(spl[index]);
				for (int i=0;i<spl.length;i++){
					if (i==index){
						bw.append(sctid);
					}else{
						bw.append(spl[i]);
					}
					
					if (i==spl.length-1){
						bw.append("\r\n");
					}else{
						bw.append("\t");
					}
				}
			}else{
				bw.append(line);
				bw.append("\r\n");
			}
		}
		bw.close();
		bw=null;
		rbr.close();
		rbr=null;
		

		if (relFile.exists()){
			String inputFile=relFile.getAbsolutePath();
			File reconFile=new File(relFile.getParent(),"Previous_" + relFile.getName());
			relFile.renameTo(reconFile);
			File outFile= new File(inputFile);
			finalRelFile.renameTo(outFile);
			
		}
	}

	public IdCreation(){}

	public IdCreation( int namespaceId,
			long partitionId, String endPointURL, String username, String pass,
			String releaseDate){

		this.namespaceId = namespaceId;
		this.partitionId = partitionId;
		this.endPointURL = endPointURL;
		this.username = username;
		this.pass = pass;
		this.releaseDate = releaseDate;
	}
	public Long getId(UUID uuid) throws Exception{
		IdAssignmentImpl idGen = getIdGeneratorClient();
		Long sctId = idGen.createSCTID(uuid, namespaceId, String.valueOf(partitionId), releaseDate, releaseDate,"98");
		return sctId;
	}

	private IdAssignmentImpl getIdGeneratorClient(){
		if (stIdGen==null ){
			stIdGen = new IdAssignmentImpl(this.endPointURL, this.username, this.pass);
		}
		return stIdGen;
	}
	public Long getId(UUID uuid, int namespaceId,
			String partitionId, String endPointURL, String username, String pass,
			String releaseDate) {
		Long sctId=null;
		IdAssignmentBI idAssignment = new IdAssignmentImpl(endPointURL,username,pass);
		try {
			sctId = idAssignment.createSCTID(uuid, namespaceId, partitionId, releaseDate, releaseDate,"99");
		} catch (Exception cE) {
			log.error("Message : SCTID creation error for list " , cE);
		}
		
		return sctId;
	}
}
