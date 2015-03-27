package org.ihtsdo.conversion.tasks;

import java.io.File;
import java.io.IOException;

import org.ihtsdo.conversion.utils.FILE_TYPE;
import org.ihtsdo.conversion.utils.FileSorter;
import org.ihtsdo.conversion.utils.SnapshotGeneratorMultiColumn;
import org.ihtsdo.id.retrieve.RF2RelsIDRetrieveImpl;


/**
 * Sorts input files, retrieve ids and generates snapshot retired concept isas in RF2 format.
 * 
 */
public class IdAssignRF2RetiredConceptIsas extends AbstractTask {
	private File tempDirectory;

	private String releaseDate;
	private File exportedSnapshotFile;
	
	private String tmpPostExport="tmppostexport";	
	private String tmpSort="tmpsort";
	private String tmpTmpSort="tmp";
	private String tmpSnapShot="tmpsnapshot";
	private String fullOutputFolder="Full";
	private String snapshotOutputFolder="Snapshot";
	private String endFile=".txt";
	private String snapshotSuffix="Snapshot";

	private FILE_TYPE fileType;

	private File rf2OutputRelationshipsReassign;

	private String previousReleaseDate;

	private String moduleId;

	private File previousRf1RelationshipFile;

	public void execute() {	
		
		try {

			File folderTmp=new File(tempDirectory.getAbsolutePath() + "/" + getTmpPostExport() );
			if (!folderTmp.exists()){
				folderTmp.mkdir();
			}
			File sortedfolderTmp=new File(folderTmp.getAbsolutePath() + "/" + getTmpSort());
			if (!sortedfolderTmp.exists()){
				sortedfolderTmp.mkdir();
			}

		//	Config config = JAXBUtil.getConfig("/org/ihtsdo/rf2/config/relationship.xml");

			File exportedRelationshipFile = exportedSnapshotFile ;

			File sortTmpfolderSortedTmp=new File(sortedfolderTmp.getAbsolutePath() + "/" + getTmpTmpSort());
			if (!sortTmpfolderSortedTmp.exists()){
				sortTmpfolderSortedTmp.mkdir();
			}

//			File sortedPreviousfile=new File(sortedfolderTmp,"pre_" + previousRelationshipFullFile.getName());
//			FileSorter fsc=new FileSorter(previousRelationshipFullFile, sortedPreviousfile, sortTmpfolderSortedTmp, FILE_TYPE.RF1_RELATIONSHIP.getColumnIndexes());
//			fsc.execute();
//			fsc=null;
//			System.gc();


			File sortedExportedfile=new File(sortedfolderTmp,"exp_" + exportedRelationshipFile.getName());

			FileSorter fsc=new FileSorter(exportedRelationshipFile, sortedExportedfile, sortTmpfolderSortedTmp, FILE_TYPE.RF2_RELATIONSHIP.getColumnIndexes());
			fsc.execute();
			fsc=null;
			System.gc();



			File snapshotfolderTmp=new File(folderTmp.getAbsolutePath() + "/" + getTmpSnapShot() );
			if (!snapshotfolderTmp.exists()){
				snapshotfolderTmp.mkdir();
			}
//			File snapshotSortedPreviousfile=new File(snapshotfolderTmp,"pre_" + previousRelationshipFullFile.getName());
//			SnapshotGeneratorMultiColumn sg=new SnapshotGeneratorMultiColumn(sortedPreviousfile, previousReleaseDate, FILE_TYPE.RF2_RELATIONSHIP.getSnapshotIndex(), 1, snapshotSortedPreviousfile, null, null);
//			sg.execute();
//			sg=null;
//			System.gc();


			File snapshotSortedExportedfile=new File(snapshotfolderTmp,"exp_" + exportedRelationshipFile.getName());
			SnapshotGeneratorMultiColumn sg=new SnapshotGeneratorMultiColumn(sortedExportedfile, releaseDate, FILE_TYPE.RF2_RELATIONSHIP.getSnapshotIndex(), 1, snapshotSortedExportedfile, null, null);
			sg.execute();
			sg=null;
			System.gc();


			File sortedSnapPreviousfile=new File(snapshotfolderTmp,"sortSnappre_" + previousRf1RelationshipFile.getName());	
			fsc=new FileSorter(previousRf1RelationshipFile, sortedSnapPreviousfile, sortTmpfolderSortedTmp,new int[]{1,2,3,6});
			fsc.execute();
			fsc=null;
			System.gc();
			//		

			File sortedSnapExportedfile=new File(snapshotfolderTmp,"sortSnapexp_" + exportedRelationshipFile.getName());
			fsc=new FileSorter(snapshotSortedExportedfile, sortedSnapExportedfile, sortTmpfolderSortedTmp, new int[]{4,7,5,2,1,6});
			fsc.execute();
			fsc=null;
			System.gc();

//			rf2OutputRelationshipsReassign=new File(exportedRelationshipFile.getParentFile().getAbsolutePath(),"outputRF2RetIsaRelationshipReassigned.txt");
			File outputUUIDsToAssign=new File(rf2OutputRelationshipsReassign.getParentFile().getAbsolutePath(),"outputRetIsaUUIDsToAssign.txt");
			File outputDifferences=new File(rf2OutputRelationshipsReassign.getParentFile().getAbsolutePath(),"outputRetIsaDifferences.txt");

			RF2RelsIDRetrieveImpl rIdReassign=new RF2RelsIDRetrieveImpl(sortedSnapPreviousfile,fileType,moduleId, sortedSnapExportedfile,previousReleaseDate,
					rf2OutputRelationshipsReassign, outputUUIDsToAssign, outputDifferences);

			rIdReassign.execute();
			rIdReassign=null;

//			if (rf2OutputRelationshipsReassign.exists()){
//				String strOutput=exportedRelationshipFile.getAbsolutePath();
//				exportedRelationshipFile.renameTo(new File(strOutput + ".prevToIdRetr"));
//				rf2OutputRelationshipsReassign.renameTo(new File(strOutput));
//			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	public IdAssignRF2RetiredConceptIsas(File tempDirectory,
			String releaseDate, String previousReleaseDate,
			File exportedSnapshotFile, File previousRf1RelationshipFile,
			FILE_TYPE fileType,String moduleId,File rf2OutputRelationshipsReassign) {
		super();
		this.tempDirectory = tempDirectory;
		this.releaseDate = releaseDate;
		this.exportedSnapshotFile = exportedSnapshotFile;
		this.previousRf1RelationshipFile = previousRf1RelationshipFile;
		this.fileType=fileType;
		this.rf2OutputRelationshipsReassign=rf2OutputRelationshipsReassign;
		this.previousReleaseDate=previousReleaseDate;
		this.moduleId=moduleId;
	}


	public File getSnapshotOutputFile(String parentFolder,FILE_TYPE fType,String date){
		String retFile=fType.getFileName();
		if (retFile==null){
			return null;
		}
		retFile=retFile.replace("SUFFIX",snapshotSuffix);
		retFile+="_" + date + endFile;
		return new File(parentFolder,retFile);
		
	}
	public String getTmpSnapShot() {
		return tmpSnapShot;
	}
	public String getTmpPostExport() {
		return tmpPostExport;
	}
	public String getTmpSort() {
		return tmpSort;
	}
	public String getFullOutputFolder() {
		return fullOutputFolder;
	}

	public String getSnapshotOutputFolder() {
		return snapshotOutputFolder;
	}
	public String getTmpTmpSort() {
		return tmpTmpSort;
	}
}