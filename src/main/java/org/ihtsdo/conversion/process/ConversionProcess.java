package org.ihtsdo.conversion.process;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.SystemConfiguration;
import org.apache.log4j.Logger;
import org.ihtsdo.conversion.configuration.MetadataConfig;
import org.ihtsdo.conversion.tasks.GenerateRF1Associations_References;
import org.ihtsdo.conversion.tasks.GenerateRF1ComponentHistoryOneLang;
import org.ihtsdo.conversion.tasks.GenerateRF1ConceptSubset;
import org.ihtsdo.conversion.tasks.GenerateRF1CoreComponentHistory;
import org.ihtsdo.conversion.tasks.GenerateRF1CptDescLangSubs;
import org.ihtsdo.conversion.tasks.GenerateRF1DescLangSubsWithOptionals;
import org.ihtsdo.conversion.tasks.GenerateRF1Qualifiers;
import org.ihtsdo.conversion.tasks.GenerateRF1Relationships;
import org.ihtsdo.conversion.tasks.GenerateRF1StatedRelationships;
import org.ihtsdo.conversion.tasks.GenerateRF1TextDefinitions;
import org.ihtsdo.conversion.tasks.GenerateRF2MetadataConceptList;
import org.ihtsdo.conversion.tasks.GenerateRF2RetiredConceptRelationship;
import org.ihtsdo.conversion.tasks.IdAssignRF2RetiredConceptIsas;
import org.ihtsdo.conversion.utils.CommonUtils;
import org.ihtsdo.conversion.utils.ConstantParamNames;
import org.ihtsdo.conversion.utils.ConversionSnapshotDelta;
import org.ihtsdo.conversion.utils.FILE_TYPE;
import org.ihtsdo.conversion.utils.FileFilterAndSorter;
import org.ihtsdo.conversion.utils.FileHelper;
import org.ihtsdo.conversion.utils.FileSorter;
import org.ihtsdo.conversion.utils.I_Constants;
import org.ihtsdo.conversion.utils.SubsetMetadata;
import org.ihtsdo.id.create.IdCreation;

public class ConversionProcess {

	private static Logger logger = Logger.getLogger(ConversionProcess.class.getName());

	public static void main(String[] args){
		String configFile;
		if (args.length==1){
			configFile=args[0];
		}else{
			configFile="config/params.properties";
		}
		ConversionProcess cp=new ConversionProcess();
		cp.setParamFile(configFile);
		try {
			cp.execute();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String paramFile = null;

	private String releaseDate;

	private File outputFolder;

	private String output;

	private File rf2SnapshotInputFolder;

	private File tempSortingFolder;

	private File tempSortedFinalfolder;

	private String rf1InputFolder;

	private String languageRefsetId;

	/** The temp folder. */
	private static File tempFolder;

	private MetadataConfig cfg;

	private File attributesFile;

	private int namespaceId;

	private String endPointURL;

	private String username;

	private String pass;

	private boolean isExtension;

	private String moduleId;

	private String previousReleaseDate;

	private String qualStartStop;

	private File rf2FullInputFolder;

	private String extensionName;

	private String langCodeInFileName;

	private String testMode;

	private BufferedWriter bws;

	private String refsetIdSubOriId;

	private HashMap<String,String> refsetIdSubsetOriginal;

	public void execute() throws IOException, Exception{

		if (!getParams()){
			return;
		}
		if (!makeTmpFolders()){
			return;
		}
		//		if (!createTmpFiles()){
		//			return;
		//		}
		if (!makeFinalFolders()){
			return;
		}
		boolean bConcDescLang=true;
		boolean bTextDefin=true;
		boolean bInferRel=true;
		boolean bAssoc=true;
		boolean bCompHx=true;
		boolean bRetStated=true;
		boolean bRetInfer=true;
		boolean bQualif=true;
		StringBuffer sb=new StringBuffer();


		refsetIdSubsetOriginal=new HashMap<String, String>();
		if (refsetIdSubOriId!=null && !refsetIdSubOriId.equals("")){
			getMappingRefsetIdSubOriId();
		}
		cfg = new MetadataConfig();

		logger.log(org.apache.log4j.Level.INFO,"Generating metadata concept list");
		File metadataConceptListFile=new File(tempFolder,"metadataConceptListFile.txt");
		File snapshotSortedRelationshipFile=null;
		String relationship=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-relationships", null, null, "stated");
		if (relationship==null){
			logger.info("Error: RF2 Relationship Snapshot file not found!");
			bInferRel=false;
			bQualif=false;
		}else{
			snapshotSortedRelationshipFile=new File(tempFolder,"rf2_ss_relationships.txt");
			ConversionSnapshotDelta.snapshotFile(new File(relationship), tempSortingFolder, tempSortedFinalfolder, snapshotSortedRelationshipFile, releaseDate, new int[]{0,1}, 0, 1);
		}
		GenerateRF2MetadataConceptList gml = new GenerateRF2MetadataConceptList(snapshotSortedRelationshipFile, metadataConceptListFile);
		gml.execute();
		gml=null;

		File snapshotSortedDescriptionFile=new File(tempFolder,"rf2_ss_descriptions.txt");
		File snapshotSortedConceptFile=new File(tempFolder,"rf2_ss_concepts.txt");

		IdCreation idc=new IdCreation();
		String subsetId;
		File snapshotSortedAssociationFile=new File(tempFolder,"rf2_ss_associations.txt");

		String targetFolderPath = output + "/RF1/SnomedCT_RF1Release_" + releaseDate ;
		File targetFolderFile = new File(targetFolderPath);
		if(!targetFolderFile.exists()){
			targetFolderFile.mkdirs();
		}
		String targetSubsetsFolder=targetFolderPath  + "/Subsets";
		File targetSubsetsFolderFile = new File(targetSubsetsFolder);
		if(!targetSubsetsFolderFile.exists()){
			targetSubsetsFolderFile.mkdirs();
		}
		String targetTerminologyFolder = targetFolderPath + "/Terminology/Content" ;
		File targetContentFolderFile = new File(targetTerminologyFolder);
		if(!targetContentFolderFile.exists()){
			targetContentFolderFile.mkdirs();
		}
		String targetHistoryFolder = targetFolderPath + "/Terminology/History" ;
		File targetHistoryFolderFile = new File(targetHistoryFolder);
		if(!targetHistoryFolderFile.exists()){
			targetHistoryFolderFile.mkdirs();
		}
		String targetTDefinFolder = targetFolderPath + "/OtherResources/TextDefinitions" ;
		File targetTDefinFolderFile = new File(targetTDefinFolder);
		if(!targetTDefinFolderFile.exists()){
			targetTDefinFolderFile.mkdirs();
		}
		String targetStatedRelsFolder = targetFolderPath + "/OtherResources/StatedRelationships" ;
		File targetStatedRelsFolderFile = new File(targetStatedRelsFolder);
		if(!targetStatedRelsFolderFile.exists()){
			targetStatedRelsFolderFile.mkdirs();
		}
		File outputDescriptionFile ;
		File outputConceptFile;
		if (isExtension){
			outputConceptFile = new File(targetContentFolderFile, "sct1_Concepts_Extension_" + extensionName + "_" + releaseDate + ".txt");
			outputDescriptionFile = new File(targetContentFolderFile, "sct1_Descriptions_" + langCodeInFileName + "_" + extensionName + "_" + releaseDate + ".txt");
		}else{
			outputConceptFile = new File(targetContentFolderFile, "sct1_Concepts_Core_INT_" + releaseDate + ".txt");
			outputDescriptionFile = new File(targetContentFolderFile, "sct1_Descriptions_en_INT_" + releaseDate + ".txt");

		}
		//create concept, description, lang subset
		String description=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-descriptions", null, null, "definition");
		File snapshotReSortedDescriptionFile=null;
		if (description==null){

			logger.info("Error: RF2 Descriptions Snapshot file not found!");

			bConcDescLang=false;

		}else{

			ConversionSnapshotDelta.snapshotFile(new File(description), tempSortingFolder, tempSortedFinalfolder, snapshotSortedDescriptionFile, releaseDate, new int[]{0,1}, 0, 1);
			snapshotReSortedDescriptionFile= new File(tempFolder,"Re" + snapshotSortedDescriptionFile.getName());
			FileSorter descFileSorter = new FileSorter(snapshotSortedDescriptionFile, snapshotReSortedDescriptionFile, tempSortingFolder, new int[] { 4,0});
			descFileSorter.execute();
			descFileSorter=null;
		}
		String concept=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-concepts", null, null, null);

		if (concept==null){

			logger.info("Error: RF2 Concept Snapshot file not found!");
			bConcDescLang=false;
			bRetStated=false;
			bRetInfer=false;
			bQualif=false;

		}else{
			ConversionSnapshotDelta.snapshotFile(new File(concept), tempSortingFolder, tempSortedFinalfolder, snapshotSortedConceptFile, releaseDate, new int[]{0,1}, 0, 1);
		}
		String attributes=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-attributevalue", null, null, null);
		File snapshotSortedConceptInactFile=null;
		File snapshotSortedDescriptionInactFile=null;
		if (attributes!=null){
			attributesFile=new File(attributes);
			snapshotSortedConceptInactFile=getInactConceptFile(attributesFile,"sortedSnapConceptInactFile.txt");
			snapshotSortedDescriptionInactFile=getInatDescriptionFile(attributesFile,"sortedSnapDescInactFile.txt");
		}else{

			logger.info("Error: RF2 Attribute-Values Refset Snapshot file not found!");

			bConcDescLang=false;
			bRetStated=false;
			bRetInfer=false;

		}

		String association=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-association", null, null, null);
		if (association!=null){
			ConversionSnapshotDelta.snapshotFile(new File(association), tempSortingFolder, tempSortedFinalfolder, snapshotSortedAssociationFile, releaseDate, new int[]{0,1}, 0, 1);
		}else{

			logger.info("Error: RF2 Associations Refset Snapshot file not found!");
			if (isExtension){
				bConcDescLang=false;
			}
			bAssoc=false;
		}

		String firstPartitionDigit; 
		if ( namespaceId==0){
			firstPartitionDigit="0";
		}else{
			firstPartitionDigit="1";
		}
		String language=null;
		File snapshotSortedLanguageFile=new File(tempFolder,"rf2_language.txt");
		File outputSubsetHeaderFile = new File(targetSubsetsFolderFile , "der1_Subsets_" + extensionName + "_" + releaseDate + ".txt");

		bws=getWriter(outputSubsetHeaderFile);
		iniSubsetHeader();
		if (isExtension){

			UUID uuid=CommonUtils.get(null,"RefsetId_" + languageRefsetId + "_" + releaseDate);
			if (testMode==null){
				Long sctId=idc.getId(uuid,namespaceId,firstPartitionDigit + "3",endPointURL,username,pass,releaseDate);
				subsetId=sctId.toString();
			}else{
				subsetId="1";
			}
			File outputLanguageSubsetFile = new File(targetSubsetsFolderFile , "der1_SubsetMembers_" + langCodeInFileName + "_" + extensionName + "_" + releaseDate + ".txt");

			language=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-language", null, null, null);

			if (language==null){

				logger.info("Error: RF2 Language Refset Snapshot file not found!");

				bConcDescLang=false;

			}else{
				ConversionSnapshotDelta.snapshotFile(new File(language), tempSortingFolder, tempSortedFinalfolder, snapshotSortedLanguageFile, releaseDate, new int[]{5,1}, 5, 1);
			}
			if (bConcDescLang){
				GenerateRF1DescLangSubsWithOptionals cdl=new GenerateRF1DescLangSubsWithOptionals( snapshotSortedLanguageFile,
						snapshotReSortedDescriptionFile, snapshotSortedConceptFile,
						subsetId,
						snapshotSortedConceptInactFile, snapshotSortedDescriptionInactFile
						, metadataConceptListFile, snapshotSortedAssociationFile,
						outputLanguageSubsetFile, outputDescriptionFile,outputConceptFile) ;
				cdl.execute();

				cdl=null;
				SubsetMetadata subsetMetadata=new SubsetMetadata();
				subsetMetadata.setSUBSETID(subsetId);
				if (refsetIdSubsetOriginal.containsKey(languageRefsetId)){
					subsetMetadata.setSUBSETORIGINALID(refsetIdSubsetOriginal.get(languageRefsetId));
				}else{
					subsetMetadata.setSUBSETORIGINALID("");
				}
				subsetMetadata.setSUBSETVERSION("1");
				subsetMetadata.setSUBSETNAME("Generated from refsetId " + languageRefsetId);
				subsetMetadata.setLANGUAGECODE(langCodeInFileName);
				subsetMetadata.setSUBSETTYPE("1");
				subsetMetadata.setREALMID("0");
				subsetMetadata.setCONTEXTID("0");
				addSubsetHeaderLine(subsetMetadata);
				sb.append("Info: Concepts generated Ok\r\n");
				sb.append("Info: Descriptions generated Ok\r\n");
				sb.append("Info: Language subset generated Ok\r\n");
			}else{
				logger.info("Warning: RF1 Concepts, descriptions and language subset files weren't generated!");
				bAssoc=false;
				bTextDefin=false;
			}
		}else{

			UUID uuid=CommonUtils.get(null,"RefsetId_" + cfg.getRF2_ENGB_REFSET() + "_" + releaseDate);
			String en_GB_subset_Id="1";
			if (testMode==null){
				Long sctId=idc.getId(uuid,namespaceId,firstPartitionDigit + "3",endPointURL,username,pass,releaseDate);
				en_GB_subset_Id=sctId.toString();
			}
			uuid=CommonUtils.get(null,"RefsetId_" + cfg.getRF2_ENUS_REFSET() + "_" + releaseDate);
			String en_US_subsetId="2";
			if (testMode==null){
				Long sctId=idc.getId(uuid,namespaceId,firstPartitionDigit + "3",endPointURL,username,pass,releaseDate);
				en_US_subsetId=sctId.toString();
			}
			language=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-language", null, null, null);
			File snapshotSortedSimpleMapFile=null;
			File snapshotSortedENLanguageFile=null;
			File snapshotSortedGBLanguageFile=null;
			if (language==null){

				logger.info("Error: RF2 Language Refset Snapshot file not found!");

				bConcDescLang=false;

			}else{

				snapshotSortedSimpleMapFile=new File(tempFolder,"rf2_simpleMap.txt");
				String simplemap=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-simplemaps", null, null, null);
				if (simplemap==null){

					logger.info("Error: RF2 Simple Map Refset Snapshot file not found!");

					bConcDescLang=false;

				}else{
					snapshotSortedENLanguageFile=new File(tempFolder,"rf2_en_language.txt");
					File sortedLangFile =new File(tempSortedFinalfolder,"sorted_rf2_en_language.txt");
					FileFilterAndSorter fs = new FileFilterAndSorter(new File(language), sortedLangFile, tempSortingFolder, new int[] { 4, 5, 1 },new Integer[]{4}, new String[] {cfg.getRF2_ENUS_REFSET()});
					fs.execute();
					fs=null;
					ConversionSnapshotDelta.snapshotFile(sortedLangFile, tempSortingFolder, tempSortedFinalfolder, snapshotSortedENLanguageFile, releaseDate, new int[]{5,1}, 5, 1);

					snapshotSortedGBLanguageFile=new File(tempFolder,"rf2_gb_language.txt");
					sortedLangFile =new File(tempSortedFinalfolder,"sorted_rf2_gb_language.txt");
					fs = new FileFilterAndSorter(new File(language), sortedLangFile, tempSortingFolder, new int[] { 4, 5, 1 },new Integer[]{4}, new String[] {cfg.getRF2_ENGB_REFSET()});
					fs.execute();
					fs=null;
					ConversionSnapshotDelta.snapshotFile(sortedLangFile, tempSortingFolder, tempSortedFinalfolder, snapshotSortedGBLanguageFile, releaseDate, new int[]{5,1}, 5, 1);

					FileSorter smFileSorter = new FileSorter(new File(simplemap), snapshotSortedSimpleMapFile, tempSortingFolder, new int[] { 5,1});
					smFileSorter.execute();
					smFileSorter=null;
				}
			}
			File outputENLanguageSubsetFile = new File(targetSubsetsFolderFile , "der1_SubsetMembers_en-US_INT_" + releaseDate + ".txt");
			File outputGBLanguageSubsetFile = new File(targetSubsetsFolderFile , "der1_SubsetMembers_en-GB_INT_" + releaseDate + ".txt");
			if (bConcDescLang){
				GenerateRF1CptDescLangSubs cdl=new GenerateRF1CptDescLangSubs( snapshotSortedENLanguageFile, snapshotSortedGBLanguageFile,
						snapshotReSortedDescriptionFile, snapshotSortedConceptFile,
						en_US_subsetId, en_GB_subset_Id,
						snapshotSortedSimpleMapFile, 
						snapshotSortedConceptInactFile, snapshotSortedDescriptionInactFile,
						metadataConceptListFile, outputGBLanguageSubsetFile,
						outputENLanguageSubsetFile, outputDescriptionFile,
						outputConceptFile) ;
				cdl.execute();

				cdl=null;
				SubsetMetadata subsetMetadata=new SubsetMetadata();
				subsetMetadata.setSUBSETID(en_US_subsetId);
				subsetMetadata.setSUBSETORIGINALID("100033");
				subsetMetadata.setSUBSETVERSION("");
				subsetMetadata.setSUBSETNAME("US English Dialect Subset");
				subsetMetadata.setLANGUAGECODE("en-US");
				subsetMetadata.setSUBSETTYPE("1");
				subsetMetadata.setREALMID("0");
				subsetMetadata.setCONTEXTID("0");
				addSubsetHeaderLine(subsetMetadata);

				subsetMetadata.setSUBSETID(en_GB_subset_Id);
				subsetMetadata.setSUBSETORIGINALID("101032");
				subsetMetadata.setSUBSETVERSION("");
				subsetMetadata.setSUBSETNAME("GB English Dialect Subset");
				subsetMetadata.setLANGUAGECODE("en-GB");
				subsetMetadata.setSUBSETTYPE("1");
				subsetMetadata.setREALMID("0");
				subsetMetadata.setCONTEXTID("0");
				addSubsetHeaderLine(subsetMetadata);
				sb.append("Info: Concepts generated Ok\r\n");
				sb.append("Info: Descriptions generated Ok\r\n");
				sb.append("Info: Language subsets generated Ok\r\n");
			}else{
				logger.info("Warning: RF1 Concepts, descriptions and language subset files weren't generated!");
				bAssoc=false;
				bTextDefin=false;
			}
		}

		//Relationships
		HashSet<File> hFile = new HashSet<File>();

		File retConceptRels= new File(tempFolder,"retConceptsRels.txt");
		File retConceptRelsWithIds= new File(tempFolder,"retConceptRelsWithIds.txt");

		File previousRf1RelationshipFile =null;

		boolean bReAssignInfer=true;
		String fFile=null;
		if (rf1InputFolder!=null && !rf1InputFolder.equals("") ){
			fFile=FileHelper.getFile( new File(rf1InputFolder), "rf1-relationships", null, null, "stated");
			if (fFile==null){

				logger.info("Warning: RF1 Previous Relationship file not found!");
				bReAssignInfer=false;
			}else{
				previousRf1RelationshipFile =new File(fFile);
			}
		}else{

			logger.info("Warning: RF1 Previous Relationship file not found!");
			bReAssignInfer=false;
		}
		String tempRelsWithId=null;
		if (bRetInfer){
			logger.info("Info: Generating RF2 Relationships of retired concepts.");
			GenerateRF2RetiredConceptRelationship rci=new GenerateRF2RetiredConceptRelationship(
					snapshotSortedConceptInactFile, 
					snapshotSortedConceptFile, 
					I_Constants.INFERRED, 
					moduleId, 
					releaseDate, 
					retConceptRels);

			rci.execute();
			rci=null;

			if (bReAssignInfer){
				logger.info("Info: Reassign Ids for Relationships of retired concepts.");
				IdAssignRF2RetiredConceptIsas idAssign=new IdAssignRF2RetiredConceptIsas(
						tempFolder, 
						releaseDate, 
						previousReleaseDate,
						retConceptRels, 
						previousRf1RelationshipFile, 
						FILE_TYPE.RF1_RELATIONSHIP,
						moduleId,
						retConceptRelsWithIds);

				idAssign.execute();
				idAssign=null;
				tempRelsWithId=retConceptRelsWithIds.getAbsolutePath();
			}else{
				tempRelsWithId=retConceptRels.getAbsolutePath();

			}
			logger.info("Info: Creating Ids for new Relationships of retired concepts.");
			idc.createIdsFromFile(
					tempRelsWithId, 
					new File(tempFolder, "uuidsMapFile.txt").getAbsolutePath(),
					namespaceId,
					Long.parseLong( firstPartitionDigit + "2"),
					endPointURL,
					username,
					pass,
					releaseDate,
					testMode);
		}else{

			logger.info("Warning: Relationships of retired concepts will be not generated.");
		}
		File tempRelsWithIdFile=null;
		if (tempRelsWithId!=null){
			tempRelsWithIdFile=new File(tempRelsWithId);
		}
		File qualStartStopFile=null;
		if (qualStartStop!=null && !qualStartStop.equals("")){
			qualStartStopFile=new File(qualStartStop);
		}
		if (qualStartStopFile==null || !qualStartStopFile.exists()){
			logger.info("Warning: Qualifiers start stop file not found.");
			bQualif=false;

		}
		File qualifiersOutputFile =new File(tempFolder,"rf1Qualifiers.txt");

		idc=new IdCreation(namespaceId, Long.parseLong( firstPartitionDigit + "2"), endPointURL, username, pass, releaseDate);

		if (bQualif){
			GenerateRF1Qualifiers q=new GenerateRF1Qualifiers(
					snapshotSortedConceptFile, 
					qualStartStopFile, 
					previousRf1RelationshipFile, 
					snapshotSortedRelationshipFile, idc, qualifiersOutputFile,testMode);

			q.execute();

			q=null;
		}
		File outputRF1Associations =new File(tempFolder,"sct1_Associations.txt");

		File outputRF1References;
		if (isExtension){
			outputRF1References = new File(targetHistoryFolderFile, "sct1_References_Extension_" + extensionName + "_" + releaseDate + ".txt");
		}else{
			outputRF1References = new File(targetHistoryFolderFile, "sct1_References_Core_INT_" + releaseDate + ".txt");
		}
		if (bAssoc){
			GenerateRF1Associations_References a =new GenerateRF1Associations_References(
					outputConceptFile,
					snapshotSortedAssociationFile,
					previousRf1RelationshipFile,
					idc,
					metadataConceptListFile,
					outputRF1Associations,
					outputRF1References,
					testMode);

			a.execute();

			a=null;
			sb.append("Info: Historical references generated Ok\r\n");
		}

		File outputRelationshipFile;
		if (isExtension){
			outputRelationshipFile = new File(targetContentFolderFile, "sct1_Relationships_Extension_" + extensionName + "_" + releaseDate + ".txt");
		}else{
			outputRelationshipFile = new File(targetContentFolderFile, "sct1_Relationships_Core_INT_" + releaseDate + ".txt");
		}
		if (bInferRel){
			GenerateRF1Relationships relRef=new GenerateRF1Relationships(
					snapshotSortedRelationshipFile, 
					tempRelsWithIdFile, 
					metadataConceptListFile, outputRelationshipFile);

			relRef.execute();

			relRef=null;
		}
		if (bAssoc || bQualif || bInferRel){

			logger.info("Info: Generating RF1 Relationship file");
			if (bAssoc){
				hFile.add(outputRF1Associations);
			}else{
				logger.info("Warning: There are not associations in RF1 Relationship file!");

			}
			if (bQualif){
				hFile.add(qualifiersOutputFile);
			}else{
				logger.info("Warning: There are not qualifiers in RF1 Relationship file!");

			}
			if (bInferRel){
				hFile.add(outputRelationshipFile);
			}else{
				logger.info("Warning: There are not inferred relationships in RF1 Relationship file!");

			}
			CommonUtils.concatFile(hFile, outputRelationshipFile);

			sb.append("Info: Relationships generated Ok\r\n");
		}else{
			logger.info("Warning: RF1 Relationship file wasn't generated.");

		}

		//Stated Relationships
		String filename=null;
		if (isExtension){
			filename="res1_StatedRelationships_Extension_" + extensionName + "_" + releaseDate + ".txt";
		}else{
			filename="res1_StatedRelationships_Core_INT_" + releaseDate + ".txt";
		}
		File outputStatedRelationshipFile = new File(targetStatedRelsFolder, filename);

		File snapshotSortedStatedRelationshipFile=new File(tempFolder,"rf2_ss_statedRelationships.txt");
		String statedRelationship=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-relationships", null, "stated", null);
		if (statedRelationship==null){
			logger.info("Error: RF2 Stated Relationship Snapshot file not found!");
			logger.info("Warning: RF1 Stated Relationship file wasn't generated.");
		}else{
			ConversionSnapshotDelta.snapshotFile(new File(statedRelationship), tempSortingFolder, tempSortedFinalfolder, snapshotSortedStatedRelationshipFile, releaseDate, new int[]{0,1}, 0, 1);

			File previousRf1StatedRelationshipFile=null;
			boolean bReAssignStated=true;
			if (rf1InputFolder!=null && !rf1InputFolder.equals("") ){
				fFile=FileHelper.getFile( new File(rf1InputFolder), "rf1-relationships", null, "stated", null);
				if (fFile==null){
					logger.info("Warning: RF1 Previous Stated Relationship file not found!");
					bReAssignStated=false;
				}else{
					previousRf1StatedRelationshipFile =new File(fFile);
				}
			}else{
				logger.info("Warning: RF1 Previous Stated Relationship file not found!");
				bReAssignStated=false;
			}

			tempRelsWithId=null;
			if (bRetStated){
				logger.info("Info: Generating RF2 Stated Relationships of retired concepts.");
				GenerateRF2RetiredConceptRelationship rci=new GenerateRF2RetiredConceptRelationship(
						snapshotSortedConceptInactFile, 
						snapshotSortedConceptFile, 
						I_Constants.STATED, 
						moduleId, 
						releaseDate, 
						retConceptRels);

				rci.execute();
				rci=null;
				if (bReAssignStated){
					logger.info("Info: Reassign Ids for Stated Relationships of retired concepts.");
					IdAssignRF2RetiredConceptIsas idAssign=new IdAssignRF2RetiredConceptIsas(
							tempFolder, 
							releaseDate, 
							previousReleaseDate,
							retConceptRels, 
							previousRf1StatedRelationshipFile, 
							FILE_TYPE.RF1_STATED_RELATIONSHIP,
							moduleId,
							retConceptRelsWithIds);

					idAssign.execute();
					idAssign=null;
					tempRelsWithId=retConceptRelsWithIds.getAbsolutePath();
				}else{
					tempRelsWithId=retConceptRels.getAbsolutePath();

				}

				logger.info("Info: Creating Ids for new Stated Relationships of retired concepts.");
				idc.createIdsFromFile(
						tempRelsWithId, 
						new File(tempFolder, "uuidsMapFile2.txt").getAbsolutePath(),
						namespaceId,
						Long.parseLong( firstPartitionDigit + "2"),
						endPointURL,
						username,
						pass,
						releaseDate,
						testMode);

			}else{

				logger.info("Warning: Stated Relationships of retired concepts will be not generated.");
			}
			tempRelsWithIdFile=null;
			if (tempRelsWithId!=null){
				tempRelsWithIdFile=new File(tempRelsWithId);
			}
			logger.info("Info: Generating RF1 Stated Relationship file.");
			GenerateRF1StatedRelationships staRel=new GenerateRF1StatedRelationships(
					snapshotSortedStatedRelationshipFile, 
					tempRelsWithIdFile, 
					metadataConceptListFile, 
					outputStatedRelationshipFile);

			staRel.execute();

			staRel=null;
			sb.append("Info: Stated Relationships generated Ok\r\n");

		}

		//Text Definitions
		File outputTextDefinitionFile;
		if (isExtension){
			outputTextDefinitionFile = new File(targetTDefinFolderFile, "sct1_TextDefinitions_" + langCodeInFileName + "_" + extensionName + "_" + releaseDate + ".txt");
		}else{
			outputTextDefinitionFile = new File(targetTDefinFolderFile, "sct1_TextDefinitions_en_INT_" + releaseDate + ".txt");
		}
		File snapshotSortedTextDefinitionFile=new File(tempFolder,"rf2_ss_TextDefinition.txt");
		fFile=FileHelper.getFile( rf2SnapshotInputFolder, "rf2-textDefinition", null, null, "description");
		File snapshotReSortedTextDefinitionFile=null;
		if (fFile==null){

			logger.info("Error: RF2 Text Definition Snapshot file not found!");

			bTextDefin=false;
		}else{
			ConversionSnapshotDelta.snapshotFile(new File(fFile), tempSortingFolder, tempSortedFinalfolder, snapshotSortedTextDefinitionFile, releaseDate, new int[]{0,1}, 0, 1);
			snapshotReSortedTextDefinitionFile= new File(tempFolder,"Re" + snapshotSortedTextDefinitionFile.getName());
			FileSorter tDefFileSorter = new FileSorter(snapshotSortedTextDefinitionFile, snapshotReSortedTextDefinitionFile, tempSortingFolder, new int[] { 4,0});
			tDefFileSorter.execute();
			tDefFileSorter=null;
		}
		if (bTextDefin){
			logger.info("Info: Generating RF1 Text Definition file file.");
			GenerateRF1TextDefinitions texDef=new GenerateRF1TextDefinitions(
					snapshotReSortedTextDefinitionFile, 
					outputConceptFile, 
					metadataConceptListFile, outputTextDefinitionFile);

			texDef.execute();

			texDef=null;
			sb.append("Info: Text definitions generated Ok\r\n");
		}else{

			logger.info("Warning: RF1 Text Definition file wasn't generated.");
		}
		//Component History

		if (rf2FullInputFolder!=null  ){
			fFile=FileHelper.getFile( rf2FullInputFolder, "rf2-concepts", null, null, null);
			File fullSortedConceptFile=new File(tempFolder,"rf2_fs_concepts.txt");
			FileSorter fs ;
			if (fFile==null){

				logger.info("Error: RF2 Concept Full file not found!");
				bCompHx=false;
			}else{
				fs = new FileSorter(new File(fFile), fullSortedConceptFile, tempSortingFolder, new int[] { 0,1});
				fs.execute();
				fs=null;
			}
			fFile=FileHelper.getFile( rf2FullInputFolder, "rf2-descriptions", null, null, null);

			File fullSortedDescriptionFile=new File(tempFolder,"rf2_fs_descriptions.txt");

			if (fFile==null){

				logger.info("Error: RF2 Description Full file not found!");
				bCompHx=false;
			}else{
				fs = new FileSorter(new File(fFile), fullSortedDescriptionFile, tempSortingFolder, new int[] { 0,1});
				fs.execute();
				fs=null;
			}
			String attributesFull=FileHelper.getFile(rf2FullInputFolder, "rf2-attributevalue", null, null, null);
			File fullSortedConceptInactFile=null;
			File fullSortedDescriptionInactFile=null;
			if (attributesFull!=null){
				File attributesFullFile=new File(attributesFull);
				fullSortedConceptInactFile=getInactConceptFile(attributesFullFile,"fullSortedConceptInactFile.txt");
				fullSortedDescriptionInactFile=getInatDescriptionFile(attributesFullFile,"fullSortedDescriptionInactFile.txt");
			}else{

				logger.info("Warning: RF2 Attribute-value Full file not found!");
			}
			File componentHxFile=null;
			if (rf1InputFolder!=null && !rf1InputFolder.equals("") ){
				String compoHx=FileHelper.getFile(new File(rf1InputFolder), "rf1-componenthistory", null, null, null);
				if (compoHx!=null){
					componentHxFile=new File(compoHx);
				}else{

					logger.info("Warning: Previous RF1 component history file not found!");
				}
			}else{
				logger.info("Warning: Previous RF1 component history file not found!");
			}
			File outputComponentHistoryFile;
			if (isExtension){
				outputComponentHistoryFile = new File(targetHistoryFolderFile, "sct1_ComponentHistory_Extension_" + extensionName + "_" + releaseDate + ".txt");
			}else{
				outputComponentHistoryFile = new File(targetHistoryFolderFile, "sct1_ComponentHistory_Core_INT_" + releaseDate + ".txt");
			}

			File fullSortedLanguageFile = new File(tempFolder , "fullSortedLanguageFile.txt");

			String languageFull=FileHelper.getFile(rf2FullInputFolder, "rf2-language", null, null, null);

			if (languageFull==null){

				logger.info("Error: RF2 Language Full file not found!");
				bCompHx=false;
			}
			if (bCompHx){
				logger.info("Info: Generating RF1 component history file.");
				if (isExtension){
					FileFilterAndSorter ffs = new FileFilterAndSorter(new File(languageFull), fullSortedLanguageFile, tempSortingFolder, new int[] { 4, 5, 1 },new Integer[]{4}, new String[] {languageRefsetId});
					ffs.execute();
					ffs=null;

					GenerateRF1ComponentHistoryOneLang comHx=new GenerateRF1ComponentHistoryOneLang(
							fullSortedConceptFile, 
							fullSortedDescriptionFile, 
							fullSortedConceptInactFile, 
							fullSortedDescriptionInactFile, 
							fullSortedLanguageFile, 
							snapshotSortedTextDefinitionFile, 
							metadataConceptListFile, 
							componentHxFile, 
							tempSortedFinalfolder, 
							tempSortingFolder, 
							releaseDate, 
							previousReleaseDate, 
							outputComponentHistoryFile);


					comHx.execute();

					comHx=null;
					sb.append("Info: Component history generated Ok\r\n");
				}else{

					fs = new FileSorter(new File(languageFull), fullSortedLanguageFile, tempSortingFolder, new int[] { 5, 1 });
					fs.execute();
					fs=null;

					GenerateRF1CoreComponentHistory comHx=new GenerateRF1CoreComponentHistory(
							fullSortedConceptFile, 
							fullSortedDescriptionFile, 
							fullSortedConceptInactFile, 
							fullSortedDescriptionInactFile, 
							fullSortedLanguageFile, 
							snapshotSortedTextDefinitionFile, 
							metadataConceptListFile, 
							componentHxFile, 
							tempSortedFinalfolder, 
							tempSortingFolder, 
							releaseDate, 
							previousReleaseDate, 
							outputComponentHistoryFile);

					comHx.execute();

					comHx=null;
					sb.append("Info: Component history generated Ok\r\n");
				}
			}else{

				logger.info("Warning: RF1 Component history file wasn't generated.");
			}
		}else{
			logger.info("Warning: Rf2 Full folder not found. RF1 Component history file wasn't generated.");
		}
		//Subsets simple
		File outputSubsetFile;
		if (isExtension){
			outputSubsetFile= new File(targetSubsetsFolderFile , "der1_SubsetMembers_Extension_" + extensionName + "_" + releaseDate + ".txt");
		}else{
			outputSubsetFile= new File(targetSubsetsFolderFile , "der1_SubsetMembers_Core_INT_" + releaseDate + ".txt");
		}
		String simple=FileHelper.getFile(rf2SnapshotInputFolder, "rf2-simple", null, null, null);
		if (simple!=null){
			File simpleFile=new File(simple);

			HashSet<String> simpleRefsetIds=getDistinctRefsetIds(simple);
			UUID uuid;
			Long sctId=2l;
			File refsetTemp;
			File outputTemp;
			if (simpleRefsetIds.size()>0){

				logger.info("Info: Generating RF1 subset members file.");
			}
			for(String refsetId:simpleRefsetIds){

				uuid=CommonUtils.get(null,"RefsetId_" + refsetId + "_" + releaseDate);
				if (testMode==null){
					sctId=idc.getId(uuid,namespaceId,firstPartitionDigit + "3",endPointURL,username,pass,releaseDate);
				}else{
					sctId++;
				}
				subsetId=sctId.toString();

				refsetTemp=new File(tempFolder, "refset_" + refsetId + ".txt");

				FileFilterAndSorter ffs = new FileFilterAndSorter(simpleFile, refsetTemp, tempSortingFolder, new int[] { 5, 1 },new Integer[]{4}, new String[] {refsetId});
				ffs.execute();
				ffs=null;

				outputTemp=new File(tempFolder, "subset_" + refsetId + ".txt");
				GenerateRF1ConceptSubset subset=new GenerateRF1ConceptSubset(refsetTemp, subsetId, metadataConceptListFile,outputTemp );

				subset.execute();

				subset=null;
				SubsetMetadata subsetMetadata=new SubsetMetadata();
				subsetMetadata.setSUBSETID(subsetId);
				if (refsetIdSubsetOriginal.containsKey(refsetId)){
					subsetMetadata.setSUBSETORIGINALID(refsetIdSubsetOriginal.get(refsetId));
				}else{
					subsetMetadata.setSUBSETORIGINALID("");
				}
				subsetMetadata.setSUBSETVERSION("");
				subsetMetadata.setSUBSETNAME("Generated from refsetId " + refsetId);
				subsetMetadata.setLANGUAGECODE("");
				subsetMetadata.setSUBSETTYPE("2");
				subsetMetadata.setREALMID("0");
				subsetMetadata.setCONTEXTID("0");
				addSubsetHeaderLine(subsetMetadata);
				hFile=new HashSet<File>();
				hFile.add(outputTemp);
				hFile.add(outputSubsetFile);
				CommonUtils.concatFile(hFile, outputSubsetFile);
			}

			if (simpleRefsetIds.size()>0){
				sb.append("Info: Subsets members generated Ok\r\n");
			}
		}else{

			logger.info("Warning: RF2 Simple Refset Snapshot file not found!");
			logger.info("Warning: RF1 Subset members file wasn't generated.");
		}
		bws.close();
		logger.info("***************************************************************************");
		logger.info("\r\n" + sb.toString());
		logger.info("End.");
		logger.info("***************************************************************************");

	}

	private void getMappingRefsetIdSubOriId() throws IOException {

		FileInputStream fis = new FileInputStream(refsetIdSubOriId);
		InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
		BufferedReader br = new BufferedReader(isr);
		br.readLine();

		String[] spl;
		String line;
		while ((line=br.readLine())!=null){
			spl=line.split("\t",-1);
			refsetIdSubsetOriginal.put(spl[0], spl[1]);
		}
		br.close();

	}

	private void addSubsetHeaderLine(SubsetMetadata SubsetMetadata) throws IOException {

		bws.append(SubsetMetadata.getSUBSETID());
		bws.append("\t");
		bws.append(SubsetMetadata.getSUBSETORIGINALID());
		bws.append("\t");
		bws.append(SubsetMetadata.getSUBSETVERSION());
		bws.append("\t");
		bws.append(SubsetMetadata.getSUBSETNAME());
		bws.append("\t");
		bws.append(SubsetMetadata.getSUBSETTYPE());
		bws.append("\t");
		bws.append(SubsetMetadata.getLANGUAGECODE());
		bws.append("\t");
		bws.append(SubsetMetadata.getREALMID());
		bws.append("\t");
		bws.append(SubsetMetadata.getCONTEXTID());
		bws.append("\r\n");

	}

	private HashSet<String> getDistinctRefsetIds(String simpleMap) throws IOException {
		HashSet<String> ret=new HashSet<String>();
		FileInputStream fis = new FileInputStream(simpleMap	);
		InputStreamReader isr = new InputStreamReader(fis,"UTF-8");
		BufferedReader br = new BufferedReader(isr);
		br.readLine();

		String[] spl;
		String line;
		while ((line=br.readLine())!=null){
			spl=line.split("\t",-1);
			if (!ret.contains(spl[4])){
				ret.add(spl[4]);
			}
		}
		br.close();

		return ret;
	}

	private File getInactConceptFile(File attributeValueFile, String filename) {

		File sortedConceptInactFile = new File(tempFolder, filename);
		logger.info("Sort and filter attributes file for inactivation concepts");
		FileFilterAndSorter cifas = new FileFilterAndSorter(attributeValueFile, sortedConceptInactFile, tempSortingFolder, new int[] { 5, 1 }, new Integer[] { 4 },
				new String[] { cfg.getRF2_INACT_CONCEPT_REFSET() });
		cifas.execute();
		cifas=null;
		return sortedConceptInactFile;
	}

	private File getInatDescriptionFile(File attributeValueFile,String filename) {
		File sortedInactivationDescriptions = new File(tempFolder, filename);
		logger.info("Sort and filter attributes file for inactivation descriptions");
		FileFilterAndSorter inactDescSf = new FileFilterAndSorter(attributeValueFile, sortedInactivationDescriptions,tempSortingFolder, new int[] { 5, 1 }, new Integer[] { 4 },
				new String[] { cfg.getRF2_INACT_DESCRIPTION_REFSET() });
		inactDescSf.execute();
		inactDescSf=null;
		return sortedInactivationDescriptions;
	}

	/**
	 * Make final folders.
	 *
	 * @return true, if successful
	 */
	private boolean makeFinalFolders(){
		try{
			outputFolder = new File(output);
			if (!outputFolder.exists()){
				outputFolder.mkdirs();
			}
		}catch(Exception e){
			logger.info("Error happened making final folders." + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Make tmp folders.
	 *
	 * @return true, if successful
	 */
	public boolean makeTmpFolders() {
		try{

			tempFolder=FileHelper.getFolder(null,"tmp",true);
			tempSortingFolder=FileHelper.getFolder(tempFolder,"tmpSorting",true);
			tempSortedFinalfolder=FileHelper.getFolder(tempFolder,"tmpSorted",true);

		}catch(Exception e){
			logger.info("Error happened making temp folders." + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * Gets the {@link File} representing the sorting folder.
	 *
	 * @return the {@link File}
	 */
	public File getSortingFolder(){
		return tempSortingFolder;
	}

	/**
	 * Gets the {@link File} representing the sorted final folder.
	 *
	 * @return the {@link File}
	 */
	public File getSortedFinalFolder(){
		return tempSortedFinalfolder;
	}

	/**
	 * Gets the {@link boolean} representing the params.
	 *
	 * @return the {@link boolean}
	 */
	public boolean getParams() {

		CompositeConfiguration config = new CompositeConfiguration();
		try {
			config.addConfiguration(new PropertiesConfiguration(paramFile));
		} catch (ConfigurationException e) {
			e.printStackTrace();
			logger.info("Error happened getting params." + e.getMessage());
			return false;
		}
		config.addConfiguration(new SystemConfiguration());
		output=config.getString(ConstantParamNames.OUTPUTPARAM);
		releaseDate=config.getString(ConstantParamNames.RELEASEDATEPARAM);
		previousReleaseDate=config.getString(ConstantParamNames.PREVIOUSDATEPARAM);
		String rf2SnapshotInput=config.getString(ConstantParamNames.SNAPSHOTINPUTFOLDERPARAM);
		extensionName=config.getString(ConstantParamNames.PREFFIXFILENAMEPARAM);
		langCodeInFileName=config.getString(ConstantParamNames.PREFFIXLANGUAGEINFILENAMEPARAM);
		String rf2FullInput=config.getString(ConstantParamNames.FULLINPUTFOLDERPARAM);
		rf1InputFolder = config.getString(ConstantParamNames.RF1INPUTFOLDERPARAM);
		languageRefsetId=config.getString(ConstantParamNames.LANGREFSETIDPARAM);
		endPointURL=config.getString(ConstantParamNames.URLIDWEBSERVICEPARAM);
		username=config.getString(ConstantParamNames.USERIDWEBSERVICEPARAM);
		pass=config.getString(ConstantParamNames.PASSIDWEBSERVICEPARAM);
		String extension=config.getString(ConstantParamNames.ISEXTENSIONPARAM);
		qualStartStop=config.getString(ConstantParamNames.QUALIFIERSTARTSTOPFILEPARAM);
		refsetIdSubOriId=config.getString(ConstantParamNames.REFSETIDSUBSETORIGINALIDPARAM);
		testMode=config.getString(ConstantParamNames.TESTMODEPARAM);
		isExtension=extension!=null && extension.toLowerCase().equals("true")?true:false;
		String namespace=config.getString(ConstantParamNames.NAMESPACEIDPARAM);
		if (!isExtension){
			namespace="0";
		}
		namespaceId=Integer.parseInt(namespace);
		logger.info("Parameters:");
		logger.info("rf1 input folder = " + rf1InputFolder);
		logger.info("rf2 snapshot input folder = " + rf2SnapshotInput);
		logger.info("rf2 full input folder = " + rf2FullInput);
		logger.info("qualifier start-stop file= " + qualStartStop);
		logger.info("refset id to subset original id mapping file= " + refsetIdSubOriId);
		logger.info("output folder= " + output);
		logger.info("release date = " + releaseDate);
		logger.info("previous release date = " + previousReleaseDate);
		logger.info("end point URL = " + endPointURL);
		logger.info("user name = " + username);
		logger.info("is extension= " + isExtension);
		logger.info("extension namespace Id = " + config.getString(ConstantParamNames.NAMESPACEIDPARAM));
		logger.info("extension language Refset Id = " + languageRefsetId);
		logger.info("extension preffix in filename = " + extensionName);
		logger.info("extension language code in filename= " + langCodeInFileName);

		if (!validOutputFolder(output) || !validDate(releaseDate) || !validDate(previousReleaseDate) 
				|| !validFolder(rf2SnapshotInput) 
				|| ((isExtension) && !validSCTId(languageRefsetId)) ){
			logger.info("Not valid params. Check them and try again! ");
			return false;
		}
		rf2SnapshotInputFolder=new File(rf2SnapshotInput);

		if (rf2FullInput!=null && !rf2FullInput.equals("") ){
			rf2FullInputFolder=new File(rf2FullInput);
		}
		if (extensionName==null){
			extensionName="";
		}
		if (langCodeInFileName==null){
			langCodeInFileName="";
		}
		return true;

	}

	/**
	 * Valid output folder.
	 *
	 * @param output2 the output2
	 * @return true, if successful
	 */
	private static boolean validOutputFolder(String output2) {
		if(output2!= null && !output2.trim().equals("")){
			return true;
		}
		return false;
	}

	/**
	 * Valid sctId.
	 *
	 * @param sctId the sctId value
	 * @return true, if successful
	 */
	private static boolean validSCTId(String sctId) {
		try{
			Long.parseLong(sctId);
		}catch (Exception e) {
			return false;

		}
		return true;
	}

	/**
	 * Valid date.
	 *
	 * @param releaseDate2 the release date2
	 * @return true, if successful
	 */
	private static boolean validDate(String releaseDate2) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		sdf.setLenient(false);
		try {
			sdf.parse(releaseDate2);
		} catch (Exception e) {
			return false;
		}

		return true;
	}

	/**
	 * Valid folder.
	 *
	 * @param output2 the output2
	 * @return true, if successful
	 */
	private static boolean validFolder(String output2) {
		if(output2 != null){
			File f = new File(output2);
			if(!f.exists()){
				return false;
			}
		}else{
			return false;
		}
		return true;
	}

	private void iniSubsetHeader() throws IOException{
		bws.append("SUBSETID	SUBSETORIGINALID	SUBSETVERSION	SUBSETNAME	SUBSETTYPE	LANGUAGECODE	REALMID	CONTEXTID");
		bws.append("\r\n");
	}

	private BufferedWriter getWriter(File outFile) throws UnsupportedEncodingException, FileNotFoundException {

		FileOutputStream tfos = new FileOutputStream( outFile);
		OutputStreamWriter tfosw = new OutputStreamWriter(tfos,"UTF-8");
		return new BufferedWriter(tfosw);

	}

	public String getParamFile() {
		return paramFile;
	}

	public void setParamFile(String paramFile) {
		this.paramFile = paramFile;
	}
}
