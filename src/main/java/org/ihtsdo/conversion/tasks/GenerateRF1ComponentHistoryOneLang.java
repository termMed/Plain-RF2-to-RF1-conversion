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
import org.ihtsdo.conversion.utils.FileSorter;

public class GenerateRF1ComponentHistoryOneLang extends AbstractTask {


	private static Logger logger = Logger.getLogger(GenerateRF1ComponentHistoryOneLang.class.getName());
	private static String MAX_DATE_PREV_COMPO = "20030101";
	private static final String MAX_EFF_TIME = "99999999";
	private static final String MAX_ID = "99999999999999999999999";
	private File sortedConceptFile;
	private File sortedDescriptionFile;
	private File outputFile;
	private HashMap<String, String> RF2RF1InactStatMap;
	private File sortedConceptInactFile;
	private File sortedDescriptionInactFile;
	private File metadataConceptListFile;
	private BufferedReader dbr;
	private BufferedReader ibr;
	private BufferedReader br;
	private String RF2_FSN;
	private File previousComponentListFile;
	private BufferedWriter bw;
	private File sortedlanguagesFile;
	private File tempFolder;
	private BufferedReader ebr;
	private String line;
	private String[] spLine;
	private String[] lang;
	private String RF2_PREFERRED;
	private String RF1_SYNONYM;
	private String RF1_PREFERRED;
	private String date;
	private String[] fsnData;
	private String[] propData;
	private String[] inacStatData;
	private String[] dTData;
	private String[] descData;
	private String[] prevCptLine;
	private String[] prevDscLine;
	private BufferedReader pbr;
	private String[] compoData;
	private File textDefinitionRf2SnapshotFileName;
	private File tempSortingFolder;

	public GenerateRF1ComponentHistoryOneLang(File sortedConceptFile,
			File sortedDescriptionFile,File sortedConceptInactFile,
			File sortedDescriptionInactFile, File sortedlanguagesFile,
			File textDefinitionRf2SnapshotFileName,
			File metadataConceptListFile, File previousComponentListFile,
			File tempFolder, File tempSortingFolder,
			String date, String previousDate, File outputFile) {
		super();
		this.sortedConceptFile = sortedConceptFile;
		this.sortedDescriptionFile = sortedDescriptionFile;
		this.sortedConceptInactFile=sortedConceptInactFile;
		this.sortedDescriptionInactFile=sortedDescriptionInactFile;
		this.metadataConceptListFile=metadataConceptListFile;
		this.previousComponentListFile=previousComponentListFile;
		this.sortedlanguagesFile=sortedlanguagesFile;
		this.textDefinitionRf2SnapshotFileName=textDefinitionRf2SnapshotFileName;
		this.tempFolder=tempFolder;
		this.tempSortingFolder=tempSortingFolder;
		this.date=date;

		if (previousDate!=null){
			MAX_DATE_PREV_COMPO=previousDate;
		}
		this.outputFile = outputFile;
	}

	public void execute(){

		try {
			FileInputStream pfis ;
			InputStreamReader pisr;
			String nextLine;

			long start1 = System.currentTimeMillis();
			if (date.compareTo(MAX_DATE_PREV_COMPO)<=0 && previousComponentListFile==null){
				logger.log(org.apache.log4j.Level.ERROR, "Cannot generate component history because auxiliar component file doesn't exist.", null);
			}else if (date.compareTo(MAX_DATE_PREV_COMPO)<=0 && previousComponentListFile!=null){
				FileOutputStream fos = new FileOutputStream( outputFile);
				OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
				bw = new BufferedWriter(osw);

				bw.append("COMPONENTID");
				bw.append("\t");
				bw.append("RELEASEVERSION");
				bw.append("\t");
				bw.append("CHANGETYPE");
				bw.append("\t");
				bw.append("STATUS");
				bw.append("\t");
				bw.append("REASON");
				bw.append("\r\n");

				pfis = new FileInputStream(previousComponentListFile);
				pisr = new InputStreamReader(pfis,"UTF-8");
				pbr = new BufferedReader(pisr);

				String[] splitted;

				while((nextLine=pbr.readLine())!=null){
					splitted=nextLine.split("\t",-1);
					if (splitted[1].compareTo(date)<=0){
						bw.append(nextLine);
						bw.append("\r\n");
					}
				}
				bw.close();
				pbr.close();
				pbr=null;
				System.gc();

				long end1 = System.currentTimeMillis();
				long elapsed1 = (end1 - start1);
				System.out.println("Completed in " + elapsed1 + " ms");
				return;
			}

			String[] splitted;
			HashSet<String> txtDef=new HashSet<String>();
			if (textDefinitionRf2SnapshotFileName!=null && textDefinitionRf2SnapshotFileName.exists()){
				//get text definitions to exclude from language refset
				pfis = new FileInputStream(textDefinitionRf2SnapshotFileName);
				pisr = new InputStreamReader(pfis,"UTF-8");
				pbr = new BufferedReader(pisr);


				while((nextLine=pbr.readLine())!=null){
					splitted=nextLine.split("\t",-1);
					txtDef.add(splitted[0]);

				}
				pbr.close();
				pbr=null;
				System.gc();
			}
			getMetadataValues();
			HashSet<String> metadataCpt=new HashSet<String>();
			if (metadataConceptListFile!=null)
				metadataCpt=getMetadataConceptHash();

			String header="COMPONENTID	RELEASEVERSION	CHANGETYPE	STATUS	REASON";

			File tmpConceptsFile = new File(tempFolder,"prevConcepts_compHx.txt");
			File tmpDescriptionsFile = new File(tempFolder,"prevDescriptions_compHx.txt");

			FileOutputStream pcfos = new FileOutputStream( tmpConceptsFile);
			OutputStreamWriter pcosw = new OutputStreamWriter(pcfos,"UTF-8");
			BufferedWriter pcbw = new BufferedWriter(pcosw);

			pcbw.append(header);
			pcbw.append("\r\n");

			FileOutputStream pdfos = new FileOutputStream( tmpDescriptionsFile);
			OutputStreamWriter pdosw = new OutputStreamWriter(pdfos,"UTF-8");
			BufferedWriter pdbw = new BufferedWriter(pdosw);

			pdbw.append(header);
			pdbw.append("\r\n");


			if (previousComponentListFile!=null ){
				pfis = new FileInputStream(previousComponentListFile);
				pisr = new InputStreamReader(pfis,"UTF-8");
				pbr = new BufferedReader(pisr);

				while((nextLine=pbr.readLine())!=null){
					splitted=nextLine.split("\t",-1);
					if (splitted[1].compareTo(date)<=0 && splitted[1].compareTo(MAX_DATE_PREV_COMPO)<=0 ){
						if (splitted[0].charAt(splitted[0].length()-2)=='0' ){
							pcbw.append(nextLine);
							pcbw.append("\r\n");
						}else{
							pdbw.append(nextLine);
							pdbw.append("\r\n");
						}
					}
				}
				pbr.close();
				pbr=null;
				System.gc();
			}
			pcbw.close();
			pdbw.close();
			pcbw=null;
			pdbw=null;

			File pcSortedFile=new File(tempFolder,"Sort_" + tmpConceptsFile.getName());
			FileSorter pfsd=new FileSorter(tmpConceptsFile, pcSortedFile, tempSortingFolder, new int[]{0,1});
			pfsd.execute();
			pfsd=null;
			System.gc();

			if (previousComponentListFile!=null){
				pfis = new FileInputStream(pcSortedFile	);
				pisr = new InputStreamReader(pfis,"UTF-8");
				pbr = new BufferedReader(pisr);
				pbr.readLine();
			}

			FileInputStream fis = new FileInputStream(sortedConceptFile);
			InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
			br = new BufferedReader(isr);

			br.readLine();

			FileInputStream ifis ;
			InputStreamReader iisr;
			if (sortedConceptInactFile!=null){
				ifis = new FileInputStream(sortedConceptInactFile);
				iisr = new InputStreamReader(ifis,"UTF-8");
				ibr = new BufferedReader(iisr);

				ibr.readLine();
			}

			File reSortedFile=new File(sortedDescriptionFile.getParent(),"reSort_" + sortedDescriptionFile.getName());
			FileSorter fsd;
			fsd=new FileSorter(sortedDescriptionFile, reSortedFile, tempSortingFolder, new int[]{4,1});
			fsd.execute();
			fsd=null;
			System.gc();

			FileInputStream dfis = new FileInputStream(reSortedFile);
			InputStreamReader disr = new InputStreamReader(dfis,"UTF-8");
			dbr = new BufferedReader(disr);

			dbr.readLine();

			if (outputFile.exists()){
				outputFile.delete();
			}

			if (!outputFile.getParentFile().exists()) {
				outputFile.getParentFile().mkdirs();
			}

			FileOutputStream fos = new FileOutputStream( outputFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			bw = new BufferedWriter(osw);

			bw.append("COMPONENTID");
			bw.append("\t");
			bw.append("RELEASEVERSION");
			bw.append("\t");
			bw.append("CHANGETYPE");
			bw.append("\t");
			bw.append("STATUS");
			bw.append("\t");
			bw.append("REASON");
			bw.append("\r\n");

			String compoId="";
			String effTCompo="";

			String[] splittedLine;

			String fsnCptId="";
			String inactCptid="";
			String cptId="";

			String fsnEffTime="";
			String inactCptEffTime="";
			String effTime="";

			prevDscLine=new String[]{"","","","","","","","",""};
			prevCptLine=new String[]{"","","","",""};

			getNextPrevCompo();
			getNextFSN();
			getNextProp();
			getNextInStat();

			String[] concept;
			String[] prevConcept=new String[]{"","","","",""};

			boolean writePreviousCompo=false;
			boolean endOfData=false;
			String minEffTime="";
			String minCptId="";
			//			long prevChkpnt;
			while(!endOfData){
				if (compoData!=null){
					compoId=compoData[0];
					effTCompo=compoData[1];
				}else{
					compoId=MAX_ID;
				}
				if (fsnData!=null){
					fsnCptId=fsnData[4];
					fsnEffTime=fsnData[1];
				}else{
					fsnCptId=MAX_ID;
				}
				if (propData!=null){
					cptId=propData[0];
					effTime=propData[1];
				}else{
					cptId=MAX_ID;
				}
				if (inacStatData!=null){
					inactCptid=inacStatData[5];
					inactCptEffTime=inacStatData[1];
				}else{
					inactCptid=MAX_ID;
				}
				minCptId=getMinValue(fsnCptId,inactCptid,cptId,compoId);
				if (compoId.compareTo(minCptId)!=0){
					if (minCptId.compareTo(fsnCptId)!=0){
						fsnEffTime=MAX_EFF_TIME;
					}

					if (minCptId.compareTo(inactCptid)!=0){
						inactCptEffTime=MAX_EFF_TIME;
					}

					if (minCptId.compareTo(cptId)!=0){
						effTime=MAX_EFF_TIME;
					}
					minEffTime=getMinValue(fsnEffTime,inactCptEffTime,effTime,MAX_EFF_TIME);

				}else{
					minEffTime=effTCompo;
				}

				if (!metadataCpt.contains(minCptId)){
					concept=joinConceptParts(minCptId,minEffTime,prevConcept);
					writePreviousCompo=false;
					if (compoId.compareTo(minCptId)!=0){
						if (minEffTime.compareTo(MAX_DATE_PREV_COMPO)>0){
							String[] compHistory=getConceptHistory(prevConcept,concept);

							if (compHistory[2]!=null && !compHistory[2].equals(""))
								writeOut(compHistory);
						}
					}else{
						writeOut(compoData);
						writePreviousCompo=true;
					}

					prevConcept=concept;
				}
				if (writePreviousCompo)
					getNextPrevCompo();
				else
					endOfData=moveConceptPointers(minCptId,minEffTime);
			}
			br.close();
			if (ibr!=null)
				ibr.close();

			ibr=null;

			dbr.close();
			if (previousComponentListFile!=null)
				pbr.close();

			pbr=null;
			System.gc();

			fis = new FileInputStream(sortedlanguagesFile);
			isr = new InputStreamReader(fis,"UTF-8");
			br = new BufferedReader(isr);

			header=br.readLine();

			File tmpLangFile = new File(sortedlanguagesFile.getParent(),"RF2Lang.txt");

			FileOutputStream efos = new FileOutputStream( tmpLangFile);
			OutputStreamWriter eosw = new OutputStreamWriter(efos,"UTF-8");
			BufferedWriter ebw = new BufferedWriter(eosw);

			ebw.append(header);
			ebw.append("\r\n");

			while ((nextLine= br.readLine()) != null) {
				splittedLine = nextLine.split("\t",-1);

				if (!txtDef.contains(splittedLine[5])){
					ebw.append(splittedLine[1]);
					ebw.append("\t");
					ebw.append(splittedLine[2]);
					ebw.append("\t");
					ebw.append(splittedLine[5]);
					ebw.append("\t");
					ebw.append(splittedLine[6]);
					ebw.append("\r\n");
				}
			}
			br.close();
			ebw.close();

			br=null;
			ebw=null;
			System.gc();

			if (reSortedFile.exists())
				reSortedFile.delete();

			if (previousComponentListFile!=null){
				File pdSortedFile=new File(tempFolder,"Sort_" + tmpDescriptionsFile.getName());
				pfsd=new FileSorter(tmpDescriptionsFile, pdSortedFile, tempSortingFolder, new int[]{0,1});
				pfsd.execute();
				pfsd=null;
				System.gc();

				pfis = new FileInputStream(pdSortedFile	);
				pisr = new InputStreamReader(pfis,"UTF-8");
				pbr = new BufferedReader(pisr);
				pbr.readLine();
			}

			compoId="";
			effTCompo="";

			reSortedFile=new File(sortedDescriptionFile.getParent(),"reSort2_" + sortedDescriptionFile.getName());
			fsd=new FileSorter(sortedDescriptionFile, reSortedFile, tempSortingFolder, new int[]{0,1});
			fsd.execute();
			fsd=null;
			System.gc();

			dfis = new FileInputStream(reSortedFile);
			disr = new InputStreamReader(dfis,"UTF-8");
			dbr = new BufferedReader(disr);

			dbr.readLine();

			if (sortedDescriptionInactFile!=null){
				ifis = new FileInputStream(sortedDescriptionInactFile);
				iisr = new InputStreamReader(ifis,"UTF-8");
				ibr = new BufferedReader(iisr);

				ibr.readLine();
			}

			FileInputStream efis = new FileInputStream(tmpLangFile);
			InputStreamReader eisr = new InputStreamReader(efis,"UTF-8");
			ebr = new BufferedReader(eisr);

			ebr.readLine();

			getNextPrevCompo();
			getNextLang();

			prevDscLine=new String[]{"","","","","","","","",""};
			getNextDT("0","00000000");
			getNextDesc();
			getNextInStat();

			String dTDescId="";
			String inactDescId="";
			String descId="";

			String minDescId="";
			minEffTime="";
			String dTEffTime="";
			String inactDescEffTime="";

			String[] description;
			String[] prevDescription=new String[]{"","","","","","",""};
			endOfData=false;
			while(!endOfData){

				if (compoData!=null){
					compoId=compoData[0];
					effTCompo=compoData[1];
				}else{
					compoId=MAX_ID;
				}
				if (dTData[0].compareTo(MAX_ID)!=0){
					dTDescId=dTData[0];
					dTEffTime=dTData[1];
				}else{
					dTDescId=MAX_ID;
				}
				if (descData!=null){
					descId=descData[0];
					effTime=descData[1];
				}else{
					descId=MAX_ID;
				}
				if (inacStatData!=null){
					inactDescId=inacStatData[5];
					inactDescEffTime=inacStatData[1];
				}else{
					inactDescId=MAX_ID;
				}
				minDescId=getMinValue(dTDescId,inactDescId,descId,compoId);
				if (compoId.compareTo(minDescId)!=0){

					if (minDescId.compareTo(dTDescId)!=0){
						dTEffTime=MAX_EFF_TIME;
					}

					if (minDescId.compareTo(inactDescId)!=0){
						inactDescEffTime=MAX_EFF_TIME;
					}

					if (minDescId.compareTo(descId)!=0){
						effTime=MAX_EFF_TIME;
					}
					minEffTime=getMinValue(dTEffTime,inactDescEffTime,effTime,MAX_EFF_TIME);
				}else{
					minEffTime=effTCompo;
				}

				if (minDescId.compareTo(descId)==0){
					cptId=descData[4];
				}else if(minDescId.compareTo(prevDescription[0])==0){
					cptId=prevDescription[4];
				}else if (compoId.compareTo(minDescId)==0){
					cptId="";
				}

				if (!metadataCpt.contains(cptId)){

					description=joinDescriptionParts(minDescId,minEffTime,prevDescription);
					writePreviousCompo=false;
					if (compoId.compareTo(minDescId)!=0){

						if (minEffTime.compareTo(MAX_DATE_PREV_COMPO)>0){
							String[] compHistory=getDescriptionHistory(prevDescription,description);
							if (compHistory!=null){
								writeOut(compHistory);
							}

						}
					}else{
						writeOut(compoData);
						writePreviousCompo=true;
					}
					prevDescription=description;
				}
				if ( writePreviousCompo)
					getNextPrevCompo();
				else
					endOfData=moveDescriptionPointer(minDescId,minEffTime);
			}
			if (ibr!=null)
				ibr.close();

			ibr=null;
			dbr.close();
			ebr.close();
			if (previousComponentListFile!=null)
				pbr.close();

			pbr=null;
			bw.close();

			if (tmpLangFile.exists())
				tmpLangFile.delete();

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

	private void getNextPrevCompo() throws IOException {
		compoData=null;
		if(pbr!=null && (line=pbr.readLine())!=null){
			compoData=line.split("\t",-1);	
		}
	}

	private boolean moveDescriptionPointer(String minDescId, String minEffTime) throws IOException {
		if (compoData!=null && compoData[0].compareTo(minDescId)==0 && compoData[1].compareTo(minEffTime)==0){
			getNextPrevCompo();
		}
		if (dTData[0].compareTo(minDescId)==0 && dTData[1].compareTo(minEffTime)==0){
			getNextDT(minDescId, minEffTime);
		}
		if (descData!=null && descData[0].compareTo(minDescId)==0 && descData[1].compareTo(minEffTime)==0){
			getNextDesc();
		}
		if (inacStatData!=null && inacStatData[5].compareTo(minDescId)==0 && inacStatData[1].compareTo(minEffTime)==0){
			getNextInStat();
		}
		return (dTData[0].compareTo(MAX_ID)==0 && descData==null && inacStatData==null);
	}

	private String[] getDescriptionHistory(String[] prevDescription,
			String[] description) {
		String[] compo= new String[5];
		compo[0]=description[0];
		compo[1]=description[1];


		if (description[2].compareTo("")!=0){
			compo[3]=description[2];
		}else{
			if(description[3].compareTo("1")==0){
				compo[3]="0";
			}else{
				compo[3]="1";
			}
		}

		compo[2]="";
		if(description[0].equals(prevDescription[0])){
			if (description[2].compareTo(prevDescription[2])!=0){
				compo[2]="1";
				compo[4]="DESCRIPTIONSTATUS CHANGE";

			}else {
				compo[4]="";
				if (description[6].compareTo(prevDescription[6])!=0){
					compo[2]="2";
					compo[4]=", INITIALCAPITALSTATUS CHANGE";
				}
				if (description[7].compareTo(prevDescription[7])!=0){
					compo[2]="2";
					compo[4]+=", DESCRIPTIONTYPE CHANGE";
				}
				if (description[8].compareTo(prevDescription[8])!=0){
					compo[2]="2";
					compo[4]+=", LANGUAGECODE CHANGE";
				}
				if (description[4].compareTo(prevDescription[4])!=0){
					compo[2]="2";
					compo[4]+=", TERM CHANGE";
				}
				if (compo[4].startsWith(", "))
					compo[4]=compo[4].substring(2);
			}
		}else{
			compo[2]="0";
			compo[4]="";
		}
		if (compo[2].compareTo("")==0)
			return null;

		return compo;
	}

	private String[] joinDescriptionParts(String minDescId, String minEffTime,
			String[] prevDescription) {
		String[] description=new String[9];
		description[0]=minDescId;
		description[1]=minEffTime;
		if ( dTData[0].compareTo(minDescId)==0 && dTData[1].compareTo(minEffTime)==0){
			description[7]=dTData[2];
		}else if (prevDescription[0].compareTo(minDescId)==0){
			description[7]=prevDescription[7];
		}else{
			description[7]="";
		}
		if (descData!=null && descData[0].compareTo(minDescId)==0 && descData[1].compareTo(minEffTime)==0){
			description[3]=descData[2];
			description[4]=descData[4];
			description[5]=descData[7];
			description[6]=descData[8];
			description[8]=descData[5];
		}else if (prevDescription[0].compareTo(minDescId)==0){
			description[3]=prevDescription[3];
			description[4]=prevDescription[4];
			description[5]=prevDescription[5];
			description[6]=prevDescription[6];
			description[8]=prevDescription[8];
		}else{
			description[3]="";
			description[4]="";
			description[5]="";
			description[6]="";
			description[8]="";
		}
		if (compoData!=null && compoData[0].compareTo(minDescId)==0 && compoData[1].compareTo(minEffTime)==0){
			description[2]=compoData[3];
		}else if (inacStatData!=null && inacStatData[5].compareTo(minDescId)==0 && inacStatData[1].compareTo(minEffTime)==0){
			if( inacStatData[2].compareTo("1")==0){
				description[2]=RF2RF1InactStatMap.get(inacStatData[6]);
			}else if (description[3].compareTo("0")==0){
				description[2]="1";
			}else{
				description[2]="0";				
			}
		}else if (prevDescription[0].compareTo(minDescId)==0){
			if (prevDescription[3].compareTo(description[3])==0){
				description[2]=prevDescription[2];
			}else if (description[3].compareTo("0")==0){
				description[2]="1";
			}else{
				description[2]="0";				
			}
		}else{
			if (description[3].compareTo("0")==0){
				description[2]="1";
			}else{
				description[2]="0";				
			}
		}
		return description;
	}

	private void getNextDesc() throws IOException {
		spLine=null;
		while ((line=dbr.readLine())!=null){
			spLine=line.split("\t",-1);

			if (spLine[1].compareTo(date)<=0  && 
					(prevDscLine[0].compareTo(spLine[0])!=0 
							|| prevDscLine[2].compareTo(spLine[2])!=0
							|| prevDscLine[6].compareTo(spLine[6])!=0
							|| prevDscLine[8].compareTo(spLine[8])!=0
							|| prevDscLine[7].compareTo(spLine[7])!=0
							|| prevDscLine[5].compareTo(spLine[5])!=0)){
				prevDscLine=spLine;
				break;
			}else{
				spLine=null;
			}
		}

		descData=spLine;
	}

	private void getNextDT(String minDescId,String minEffTime) throws IOException {
		dTData=new String[]{"","","",""};

		if (lang!=null && lang[2].compareTo(minDescId)==0 && lang[0].compareTo(minEffTime)==0){
			getNextLang();
		}
		if (lang==null){
			lang=new String[]{MAX_EFF_TIME,"0",MAX_ID,"0"};
		}

		dTData[0]=lang[2];
		dTData[1]=lang[0];
		dTData[2]=(lang[3].compareTo(RF2_PREFERRED)==0)?RF1_PREFERRED:RF1_SYNONYM;

	}

	private void getNextLang() throws IOException {
		spLine=null;
		while ((line=ebr.readLine())!=null){
			spLine=line.split("\t",-1);

			if (spLine[0].compareTo(date)<=0)
				break;
		}
		lang= spLine;
	}

	private boolean moveConceptPointers(String minCptId, String minEffTime) throws IOException {

		if (compoData!=null && compoData[0].compareTo(minCptId)==0 && compoData[1].compareTo(minEffTime)==0){
			getNextPrevCompo();
		}
		if (fsnData!=null && fsnData[4].compareTo(minCptId)==0 && fsnData[1].compareTo(minEffTime)==0){
			getNextFSN();
		}
		if (propData!=null && propData[0].compareTo(minCptId)==0 && propData[1].compareTo(minEffTime)==0){
			getNextProp();
		}
		if (inacStatData!=null && inacStatData[5].compareTo(minCptId)==0 && inacStatData[1].compareTo(minEffTime)==0){
			getNextInStat();
		}
		return ( fsnData ==null && propData==null && inacStatData==null && compoData==null );
	}

	private void writeOut(String[] compHistory) throws IOException {
		bw.append(compHistory[0]);
		bw.append("\t");
		bw.append(compHistory[1]);
		bw.append("\t");
		bw.append(compHistory[2]);
		bw.append("\t");
		bw.append(compHistory[3]);
		bw.append("\t");
		bw.append(compHistory[4]);
		bw.append("\r\n");


	}

	private String[] getConceptHistory(String[] prevConcept, String[] concept) {

		String[] compo= new String[5];
		compo[0]=concept[0];
		compo[1]=concept[1];

		if (concept[2].compareTo("")!=0){
			compo[3]=concept[2];
		}else{
			if(concept[3].compareTo("1")==0){
				compo[3]="0";
			}else{
				compo[3]="1";
			}
		}

		if(concept[0].equals(prevConcept[0])){
			if (concept[2].compareTo(prevConcept[2])!=0){
				compo[2]="1";
				compo[4]="CONCEPTSTATUS CHANGE";

			}else if (concept[4].compareTo(prevConcept[4])!=0){
				compo[2]="2";
				compo[4]="FULLYSPECIFIEDNAME CHANGE";
			}
		}else{
			compo[2]="0";
			compo[4]="";
		}
		return compo;
	}

	private String[] joinConceptParts(String minCptId, String minEffTime,String[] prevConcept) {

		String[] concept=new String[5];
		concept[0]=minCptId;
		concept[1]=minEffTime;
		if (fsnData!=null && fsnData[4].compareTo(minCptId)==0 && fsnData[1].compareTo(minEffTime)==0){
			concept[4]=fsnData[7];
		}else if (prevConcept[0].compareTo(minCptId)==0){
			concept[4]=prevConcept[4];
		}else{
			concept[4]="";
		}
		if (propData!=null && propData[0].compareTo(minCptId)==0 && propData[1].compareTo(minEffTime)==0){
			concept[3]=propData[2];
		}else if (prevConcept[0].compareTo(minCptId)==0){
			concept[3]=prevConcept[3];
		}else{
			concept[3]="";
		}
		if (compoData!=null && compoData[0].compareTo(minCptId)==0 && compoData[1].compareTo(minEffTime)==0){
			concept[2]=compoData[3];
		}else if (inacStatData!=null && inacStatData[5].compareTo(minCptId)==0 && inacStatData[1].compareTo(minEffTime)==0){
			if ( inacStatData[2].compareTo("1")==0){
				concept[2]=RF2RF1InactStatMap.get(inacStatData[6]);
			}else{
				concept[2]="";
			}
		}else if (prevConcept[0].compareTo(minCptId)==0){

			if (prevConcept[3].compareTo(concept[3])==0){
				concept[2]=prevConcept[2];
			}else if (concept[3].compareTo("0")==0){
				concept[2]="1";
			}else{
				concept[2]="0";				
			}
		}else{
			concept[2]="";
		}
		return concept;
	}

	private void getNextInStat() throws IOException {
		if(ibr==null){
			inacStatData=null;
			return;
		}
		spLine=null;
		while ((line=ibr.readLine())!=null){
			spLine=line.split("\t",-1);

			if (spLine[1].compareTo(date)<=0 )
				break;
			else
				spLine=null;
		}
		inacStatData= spLine;
	}

	private void getNextProp() throws IOException {
		spLine=null;
		while ((line=br.readLine())!=null){
			spLine=line.split("\t",-1);
			if (spLine[1].compareTo(date)<=0  && 
					(prevCptLine[0].compareTo(spLine[0])!=0 
							|| prevCptLine[2].compareTo(spLine[2])!=0)){
				prevCptLine=spLine;
				break;

			}else{
				spLine=null;
			}
		}
		propData=spLine;
	}

	private void getNextFSN() throws IOException {
		spLine=null;
		while ((line=dbr.readLine())!=null){
			spLine=line.split("\t",-1);
			if ((spLine[6].compareTo(RF2_FSN)==0 
					&& spLine[2].compareTo("1")==0 
					&& spLine[1].compareTo(date)<=0) && 
					(prevDscLine[0].compareTo(spLine[0])!=0 
							|| prevDscLine[7].compareTo(spLine[7])!=0)){
				prevDscLine=spLine;
				break;
			}else{
				spLine=null;
			}
		}
		fsnData= spLine;
	}

	private String getMinValue(String string1, String string2,
			String string3,String string4) {

		String minValue="";
		if (string1.compareTo(string2)<0){
			minValue=string1;
		}else{
			minValue=string2;
		}

		if (string3.compareTo(minValue)<0){
			minValue=string3;
		}
		if (string4.compareTo(minValue)<0){
			minValue=string4;
		}
		return minValue;


	}

	private void getMetadataValues() throws Exception {

		MetadataConfig config =new MetadataConfig();

		RF2RF1InactStatMap=config.getRF2RF1inactStatMap();

		RF2_FSN=config.getRF2_FSN();
		RF2_PREFERRED=config.getRF2_PREFERRED();

		RF1_SYNONYM=config.getRF1_SYNONYM();
		RF1_PREFERRED=config.getRF1_PREFERRED();

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
