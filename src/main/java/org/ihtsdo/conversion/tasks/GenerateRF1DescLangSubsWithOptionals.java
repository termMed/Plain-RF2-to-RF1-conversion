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

public class GenerateRF1DescLangSubsWithOptionals extends AbstractTask{
	

	private static Logger logger = Logger.getLogger(GenerateRF1DescLangSubsWithOptionals.class.getName());
	private File metadataConceptListFile;
	private File snapshotSortedAssociationFile;
	private File outputConceptFile;

	public GenerateRF1DescLangSubsWithOptionals(File snapshotSortedLanguageFile,
			File snapshotSortedDescriptionFile, File snapshotSortedConceptFile,
			String subsetId,
			File snapshotSortedConceptInactFile, File snapshotSortedDescriptionInactFile
			,File metadataConceptListFile, File snapshotSortedAssociationFile,
			File outputLanguageSubsetFile, File outputDescriptionFile,
			File outputConceptFile) {
		super();
		this.snapshotSortedLanguageFile = snapshotSortedLanguageFile;
		this.snapshotSortedDescriptionFile = snapshotSortedDescriptionFile;
		this.snapshotSortedConceptFile = snapshotSortedConceptFile;
		this.RF1_SUBSET=subsetId;
		this.metadataConceptListFile=metadataConceptListFile;
		this.outputLanguageSubsetFile = outputLanguageSubsetFile;
		this.outputDescriptionFile = outputDescriptionFile;
		this.snapshotSortedConceptInactFile=snapshotSortedConceptInactFile;
		this.snapshotSortedDescriptionInactFile=snapshotSortedDescriptionInactFile;
		this.snapshotSortedAssociationFile=snapshotSortedAssociationFile;
		this.outputConceptFile=outputConceptFile;
	}

	private File snapshotSortedLanguageFile;
	private File snapshotSortedDescriptionFile;
	private File snapshotSortedConceptFile;
	private File snapshotSortedConceptInactFile;
	private File snapshotSortedDescriptionInactFile;

	private File outputLanguageSubsetFile;
	private File outputDescriptionFile;

	private String RF2_FSN;
	private String RF2_SYNONYM;
	private String RF2_PREFERRED;

	private String RF2_ICS_SIGNIFICANT;
	private String RF2_DEF_STATUS_PRIMITIVE;

	private String RF1_FSN;
	private String RF1_SYNONYM;
	private String RF1_PREFERRED;
	private String RF1_SUBSET;
	private HashMap<String, String> RF2RF1InactStatMap;
	private HashSet<String> RF2DescReferences;

	public void execute() {

		try {
			long start1 = System.currentTimeMillis();

			getMetadataValues();

			HashSet<String> metadataCpt=new HashSet<String>();
			if (metadataConceptListFile != null)
				metadataCpt=getMetadataConceptHash();

			FileInputStream enfis = new FileInputStream(snapshotSortedLanguageFile);
			InputStreamReader enisr = new InputStreamReader(enfis,"UTF-8");
			BufferedReader enbr = new BufferedReader(enisr);

			FileInputStream dfis = new FileInputStream(snapshotSortedDescriptionFile);
			InputStreamReader disr = new InputStreamReader(dfis,"UTF-8");
			BufferedReader dbr = new BufferedReader(disr);

			FileInputStream cfis = new FileInputStream(snapshotSortedConceptFile);
			InputStreamReader cisr = new InputStreamReader(cfis,"UTF-8");
			BufferedReader cbr = new BufferedReader(cisr);

			FileInputStream icfis = null;
			InputStreamReader icisr = null;
			BufferedReader icbr = null;
			if (snapshotSortedConceptInactFile!=null){
				icfis = new FileInputStream(snapshotSortedConceptInactFile);
				icisr = new InputStreamReader(icfis,"UTF-8");
				icbr = new BufferedReader(icisr);
				icbr.readLine();
			}
			FileInputStream idfis = null;
			InputStreamReader idisr = null;
			BufferedReader idbr = null;
			if (snapshotSortedDescriptionInactFile!=null){
				idfis = new FileInputStream(snapshotSortedDescriptionInactFile);
				idisr = new InputStreamReader(idfis,"UTF-8");
				idbr = new BufferedReader(idisr);
				idbr.readLine();
			}

			FileInputStream asfis = null;
			InputStreamReader asisr = null;
			BufferedReader asbr = null;
			if (snapshotSortedAssociationFile!=null){
				asfis = new FileInputStream(snapshotSortedAssociationFile);
				asisr = new InputStreamReader(asfis,"UTF-8");
				asbr = new BufferedReader(asisr);
				asbr.readLine();
			}

			enbr.readLine();

			dbr.readLine();

			cbr.readLine();

			if (outputLanguageSubsetFile.exists()){
				outputLanguageSubsetFile.delete();
			}
			if (!outputLanguageSubsetFile.getParentFile().exists()) {
				outputLanguageSubsetFile.getParentFile().mkdirs();
			}
			FileOutputStream gbfos = new FileOutputStream( outputLanguageSubsetFile);
			OutputStreamWriter gbosw = new OutputStreamWriter(gbfos,"UTF-8");
			BufferedWriter olbw = new BufferedWriter(gbosw);

			olbw.append("SUBSETID");
			olbw.append("\t");
			olbw.append("MEMBERID");
			olbw.append("\t");
			olbw.append("MEMBERSTATUS");
			olbw.append("\t");
			olbw.append("LINKEDID");
			olbw.append("\r\n");

			if (outputDescriptionFile.exists()){
				outputDescriptionFile.delete();
			}
			if (!outputDescriptionFile.getParentFile().exists()) {
				outputDescriptionFile.getParentFile().mkdirs();
			}
			
			FileOutputStream dfos = new FileOutputStream( outputDescriptionFile);
			OutputStreamWriter dosw = new OutputStreamWriter(dfos,"UTF-8");
			BufferedWriter dbw = new BufferedWriter(dosw);

			dbw.append("DESCRIPTIONID");
			dbw.append("\t");
			dbw.append("DESCRIPTIONSTATUS");
			dbw.append("\t");
			dbw.append("CONCEPTID");
			dbw.append("\t");
			dbw.append("TERM");
			dbw.append("\t");
			dbw.append("INITIALCAPITALSTATUS");
			dbw.append("\t");
			dbw.append("DESCRIPTIONTYPE");
			dbw.append("\t");
			dbw.append("LANGUAGECODE");
			dbw.append("\r\n");

			if (outputConceptFile.exists()){
				outputConceptFile.delete();
			}
			
			if (!outputConceptFile.getParentFile().exists()) {
				outputConceptFile.getParentFile().mkdirs();
			}
			
			FileOutputStream cfos = new FileOutputStream( outputConceptFile);
			OutputStreamWriter cosw = new OutputStreamWriter(cfos,"UTF-8");
			BufferedWriter cbw = new BufferedWriter(cosw);

			cbw.append("CONCEPTID");
			cbw.append("\t");
			cbw.append("CONCEPTSTATUS");
			cbw.append("\t");
			cbw.append("FULLYSPECIFIEDNAME");
			cbw.append("\t");
			cbw.append("CTV3ID");
			cbw.append("\t");
			cbw.append("SNOMEDID");
			cbw.append("\t");
			cbw.append("ISPRIMITIVE");
			cbw.append("\r\n");

			HashMap<String,String[]> hlang=new HashMap<String,String[]>();
			HashMap<String,String> hInactDes=new HashMap<String,String>();
			HashSet<String> hInapropDes=new HashSet<String>();

			String memberId="";
			String langStatus="";
			String RF2Accept="";
			String[] langSplittedLine;
			String langLine;
			String inactLine=null;
			String[] inactSplittedLine;
			String iMemberId="";
			String inactValue="";
			String iStatus = null;
			String inactCptStat="";
			String inactDescStat="";
			String icid="";
			String iCptValue="";

			String inCptLine=null;
			String[] inCptSplittedLine;

			int iComp=0;


			if (snapshotSortedConceptInactFile!=null){
				inCptLine=icbr.readLine();
				if (inCptLine!=null){
					//rf2 inactivation concept
					inCptSplittedLine=inCptLine.split("\t");
					inactCptStat=inCptSplittedLine[2];
					icid=inCptSplittedLine[5];
					iCptValue=inCptSplittedLine[6];
				}
			}
			if (snapshotSortedDescriptionInactFile!=null){
				while ((inactLine=idbr.readLine())!=null){
					//rf2 inactivation description
					inactSplittedLine=inactLine.split("\t");
					inactDescStat=inactSplittedLine[2];
					if (inactDescStat.compareTo("1")==0){
						iMemberId=inactSplittedLine[5];
						inactValue=inactSplittedLine[6];
						hInactDes.put(iMemberId, inactValue);
					}
				}
				idbr.close();
			}
			if (snapshotSortedAssociationFile!=null){
				while ((inactLine=asbr.readLine())!=null){
					//rf2 association reference
					inactSplittedLine=inactLine.split("\t");
					inactDescStat=inactSplittedLine[2];
					if (inactDescStat.compareTo("1")==0){

						if (RF2DescReferences.contains(inactSplittedLine[4])){
							hInapropDes.add(inactSplittedLine[5]);
						}
					}
				}
				asbr.close();
			}
			System.gc();
			
			while ((langLine=enbr.readLine())!=null){
				//rf2 language
				langSplittedLine = langLine.split("\t");
				langStatus=langSplittedLine[2];
				memberId=langSplittedLine[5];

				RF2Accept=langSplittedLine[6];

				hlang.put(memberId, new String[]{langStatus,RF2Accept});
			}
			String cid="";
			String cStatus="";
			String[] conSplittedLine;
			String conLine;


			String did="";
			String dStatus="";
			String dConceptId="";
			String language="";
			String rf2Type="";
			String term="";
			String ics="";
			String[] desSplittedLine;
			String desLine;

			String fsnTerm="";
			String preFsn="";
			String defStat;
			String membType="";
			String[] refsetMem;
		
			desLine= dbr.readLine();
			if (desLine != null) {
				//rf2 description
				desSplittedLine = desLine.split("\t");

				did=desSplittedLine[0];
				dStatus=desSplittedLine[2];
				dConceptId=desSplittedLine[4];
				language=desSplittedLine[5];
				rf2Type=desSplittedLine[6];
				term=desSplittedLine[7];
				ics=desSplittedLine[8];

				while ((conLine=cbr.readLine())!=null){
					//rf2 concept
					
					conSplittedLine = conLine.split("\t");
					cid=conSplittedLine[0];
					if (metadataCpt.contains(cid)){
						continue;
					}
					cStatus=conSplittedLine[2];

					iStatus=null;
					if (inCptLine!=null){
						iComp=icid.compareTo(cid);
						while (iComp<0){
							inCptLine=icbr.readLine();
							if (inCptLine!=null){
								//rf2 inactivation concept
								inCptSplittedLine=inCptLine.split("\t");
								inactCptStat=inCptSplittedLine[2];
								icid=inCptSplittedLine[5];
								iCptValue=inCptSplittedLine[6];
								iComp=icid.compareTo(cid);
							}else{
								break;
							}

						}
						if (iComp==0 && inactCptStat.compareTo("1")==0){
							iStatus=iCptValue;
						}
					}
					String RF1CptStatus="0";
					if (cStatus.compareTo("1")==0){
						if (iStatus!=null){
							RF1CptStatus=RF2RF1InactStatMap.get(iStatus);
						}
					}else if (iStatus==null){

						RF1CptStatus="1";
					}else{
						RF1CptStatus=RF2RF1InactStatMap.get(iStatus);
					}
					defStat=conSplittedLine[4];
					fsnTerm="";
					preFsn="";
					//descriptions belong to concept
					while (cid.compareTo(dConceptId)>=0) {
						if (cid.compareTo(dConceptId)==0){
							//only types for RF1
							if (rf2Type.compareTo(RF2_FSN)==0 || rf2Type.compareTo(RF2_SYNONYM)==0){

								membType="";
								if (rf2Type.compareTo(RF2_FSN)==0){
									membType=RF1_FSN;
								}
								//languages for description
								refsetMem=hlang.get(did);
								if (refsetMem!=null){
									langStatus=refsetMem[0]==null? "":refsetMem[0];
									RF2Accept=refsetMem[1];
								}else{
									RF2Accept=null;
									langStatus="";

								}
								inactValue=hInactDes.get(did);
								if (RF2Accept!=null){
									if (membType.compareTo(RF1_FSN)!=0){
										if (RF2Accept.compareTo(RF2_PREFERRED)==0){
											membType=RF1_PREFERRED;
										}else{
											membType=RF1_SYNONYM;
										}
									}

									if (langStatus.compareTo("1")==0 
											&& dStatus.compareTo("1")==0
											&& cStatus.compareTo("1")==0){
										olbw.append(RF1_SUBSET);
										olbw.append("\t");
										olbw.append(did);
										olbw.append("\t");
										olbw.append(membType);
										olbw.append("\t");
										olbw.append("");
										olbw.append("\r\n");
									}

								}

								dbw.append(did);
								dbw.append("\t");

								if (dStatus.compareTo("1")==0){
									if (hInapropDes.contains(did)){
										logger.log(org.apache.log4j.Level.ERROR, "did:" + did + " must not be active because it has a reference.", null);
									}
									if (cStatus.compareTo("0")==0){
										if (RF1CptStatus.compareTo("6")==0){
											dStatus="6";
										}else{
											dStatus="8";
										}
									}else{
										if (inactValue==null){
											dStatus="0";
										}else {

											dStatus=RF2RF1InactStatMap.get(inactValue);
										}

									}
								}else if (inactValue==null){
									if (hInapropDes.contains(did))
										dStatus="7";
									else
										dStatus="1";
								}else{
									dStatus=RF2RF1InactStatMap.get(inactValue);
								}
								dbw.append(dStatus);
								dbw.append("\t");

								dbw.append(dConceptId);
								dbw.append("\t");

								dbw.append(term);
								dbw.append("\t");

								dbw.append(ics.compareTo(RF2_ICS_SIGNIFICANT)!=0? "0":"1");
								dbw.append("\t");
								if (membType.compareTo("")==0){
									dbw.append("2");
								}else{
									dbw.append(membType);
								}
								dbw.append("\t");
								dbw.append(language);
								dbw.append("\r\n");
								if (membType.compareTo(RF1_FSN )==0 ){
									if ("068".indexOf(dStatus)>-1 || dStatus.equals("11")){
										fsnTerm=term;
									}else{
										preFsn=term;
									}
								}
							}
						}
						desLine= dbr.readLine();
						if (desLine==null) break;

						//rf2 description
						desSplittedLine = desLine.split("\t");

						did=desSplittedLine[0];
						dStatus=desSplittedLine[2];
						dConceptId=desSplittedLine[4];
						language=desSplittedLine[5];
						rf2Type=desSplittedLine[6];
						term=desSplittedLine[7];
						ics=desSplittedLine[8];

					}

					if (fsnTerm.compareTo("")==0 && preFsn.compareTo("")==0){
						//TODO log concept without descriptions
					}else{
						cbw.append(cid);
						cbw.append("\t");
						cbw.append(RF1CptStatus);
						cbw.append("\t");
						cbw.append(fsnTerm.compareTo("")==0? preFsn:fsnTerm);
						cbw.append("\t");
						cbw.append("");
						cbw.append("\t");
						cbw.append("");
						cbw.append("\t");
						cbw.append(defStat.compareTo(RF2_DEF_STATUS_PRIMITIVE)!=0? "0":"1");
						cbw.append("\r\n");
					}
				}
			}

			cbw.close();
			olbw.close();
			dbw.close();
			enbr.close();
			cbr.close();
			dbr.close();
			icbr.close();




			long end1 = System.currentTimeMillis();
			long elapsed1 = (end1 - start1);
			System.out.println("Completed in " + elapsed1 + " ms");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (IOException e) {
			e.printStackTrace();
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (Exception e) {
			e.printStackTrace();
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		}
	}

	private void getMetadataValues() throws Exception {
		MetadataConfig config =new MetadataConfig();

		RF2_FSN=config.getRF2_FSN();
		RF2_SYNONYM=config.getRF2_SYNONYM();
		RF2_PREFERRED=config.getRF2_PREFERRED();
		RF2_ICS_SIGNIFICANT=config.getRF2_ICS_SIGNIFICANT();
		RF1_FSN=config.getRF1_FSN();
		RF1_SYNONYM=config.getRF1_SYNONYM();
		RF1_PREFERRED=config.getRF1_PREFERRED();
		RF2RF1InactStatMap=config.getRF2RF1inactStatMap();
		RF2DescReferences=config.getRF2DescriptionReferences();
		RF2_DEF_STATUS_PRIMITIVE=config.getRF2_DEF_STATUS_PRIMITIVE();

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
