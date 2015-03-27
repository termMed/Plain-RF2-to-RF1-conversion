package org.ihtsdo.conversion.utils;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;


public class JoinRelease {

	public static void main (String[] args){

		String files1 = "";
		String files2 = "";
		JoinRelease jr=new JoinRelease();
		try {

//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/previous_20140731/Snapshot";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Snapshot";
//			jr.processFiles(files1,files2);
//
//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/previous_20140731/Full";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Snapshot";
//			jr.processFiles(files1,files2);
//
//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/previous_20140731/Delta";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Snapshot";
//			jr.processFiles(files1,files2);
//
//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Snapshot";
//			jr.processFiles(files1,files2);

//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/destination/Delta";
//			jr.processFiles(files1,files2);
//
//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Delta_gmdn";
//			jr.processFiles(files1,files2);
//			
//			jr.AllSnapshotAndDeltaGen("/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full", "20140731", "20150131");

//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
//			files2 = "/Users/termmed/Downloads/rels_Delta";
//			jr.joinFiles(files1,files2,"rf2-relationships",null,null,"stated");

			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Delta_gmdn";
			jr.joinFiles(files1,files2,"rf2-descriptions",null,null,null);
//			
			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/destination/Delta";
			jr.joinFiles(files1,files2,"rf2-descriptions",null,null,null);
			
			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Delta_gmdn";
			jr.joinFiles(files1,files2,"rf2-language",null,null,null);
//			
			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full";
			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/destination/Delta";
			jr.joinFiles(files1,files2,"rf2-language",null,null,null);
////			
//			
			jr.AllSnapshotAndDeltaGen("/Volumes/Macintosh HD2/DailyBuild_Gmdn/current_20150131/Full", "20140731", "20150131");

//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/previous_20140731/Snapshot";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Snapshot";
//			jr.joinFiles(files1,files2,"rf2-relationships",null,null,"stated");
//
//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/previous_20140731/Full";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Snapshot";
//			jr.joinFiles(files1,files2,"rf2-relationships",null,null,"stated");
//
//			files1 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/previous_20140731/Delta";
//			files2 = "/Volumes/Macintosh HD2/DailyBuild_Gmdn/Snapshot";
//			jr.joinFiles(files1,files2,"rf2-relationships",null,null,"stated");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private File tempSortingFolder;
	private File tempSortedFinalFolder;
	private String iniDate;
	private String endDate;
	public void processFiles(String files1, String files2) throws IOException, Exception {

		joinFiles(files1,files2,"rf2-relationships",null,null,"stated");
		joinFiles(files1,files2,"rf2-relationships",null,"stated",null);
		joinFiles(files1,files2,"rf2-textDefinition",null,null,null);
		joinFiles(files1,files2,"rf2-association-2",null,null,null);
		joinFiles(files1,files2,"rf2-attributevalue",null,null,null);
		joinFiles(files1,files2,"rf2-language",null,null,null);
		joinFiles(files1,files2,"rf2-simple",null,null,null);
		joinFiles(files1,files2,"rf2-simplemaps",null,null,null);
		joinFiles(files1,files2,"rf2-descriptions",null,null,null);
		joinFiles(files1,files2,"rf2-concepts",null,null,null);
	}

	public void AllSnapshot(String folder,String snapshotFolder,String snapshotDate) throws IOException, Exception{
		this.endDate=snapshotDate;
		tempSortingFolder=new File("tmpSg");
		if (!tempSortingFolder.exists()){
			tempSortingFolder.mkdirs();
		}
		tempSortedFinalFolder=new File("tmpSd");
		if (!tempSortedFinalFolder.exists()){
			tempSortedFinalFolder.mkdirs();
		}
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-concepts",null,null,null);
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-relationships",null,"stated",null);
//		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-relationships",null,null,"stated");
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-textDefinition",null,null,null);
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-association-2",null,null,null);
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-attributevalue",null,null,null);
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-language",null,null,null);
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-simple",null,null,null);
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-simplemaps",null,null,null);
		getFileAndSnapshot(new File (folder),new File (snapshotFolder),"rf2-descriptions",null,null,null);
		FileHelper.emptyFolder(tempSortingFolder);
		FileHelper.emptyFolder(tempSortedFinalFolder);
		
	}

	private void getFileAndSnapshot(File folderFile1,File snapFolder, String pattern, String folderDefault, String mustHave, String doesNotMustHave) throws IOException, Exception{
		String fFile=FileHelper.getFile( folderFile1, pattern, folderDefault,mustHave,doesNotMustHave);
		String sFileName= new File(fFile).getName().replaceAll("Full", "Snapshot");
		File sFile=new File(snapFolder,sFileName);

		ConversionSnapshotDelta.snapshotFile(new File(fFile), tempSortingFolder, tempSortedFinalFolder, sFile, endDate, null, 0, 0);
	}
	
	public void AllSnapshotAndDeltaGen(String folder,String iniDate,String endDate) throws IOException, Exception{
		this.iniDate=iniDate;
		this.endDate=endDate;
		tempSortingFolder=new File("tmpSg");
		if (!tempSortingFolder.exists()){
			tempSortingFolder.mkdirs();
		}
		tempSortedFinalFolder=new File("tmpSd");
		if (!tempSortedFinalFolder.exists()){
			tempSortedFinalFolder.mkdirs();
		}
		getFileAndSnapDelta(new File (folder),"rf2-concepts",null,null,null);
		getFileAndSnapDelta(new File (folder),"rf2-relationships",null,"stated",null);
		getFileAndSnapDelta(new File (folder),"rf2-relationships",null,null,"stated");
		getFileAndSnapDelta(new File (folder),"rf2-textDefinition",null,null,null);
		getFileAndSnapDelta(new File (folder),"rf2-association-2",null,null,null);
		getFileAndSnapDelta(new File (folder),"rf2-attributevalue",null,null,null);
		getFileAndSnapDelta(new File (folder),"rf2-language",null,null,null);
		getFileAndSnapDelta(new File (folder),"rf2-simple",null,null,null);
		getFileAndSnapDelta(new File (folder),"rf2-simplemaps",null,null,null);
		getFileAndSnapDelta(new File (folder),"rf2-descriptions",null,null,null);
	}
	
	private void getFileAndSnapDelta(File folderFile1, String pattern, String folderDefault, String mustHave, String doesNotMustHave) throws IOException, Exception{
		String fFile=FileHelper.getFile( folderFile1, pattern, folderDefault,mustHave,doesNotMustHave);
		String dFile= new File(fFile).getAbsolutePath().replaceAll("Full", "Delta");
		File parentDir=new File(dFile).getParentFile();
		if (!parentDir.exists()){
			parentDir.mkdirs();
		}
		String sFile= new File(fFile).getAbsolutePath().replaceAll("Full", "Snapshot");
		parentDir=new File(sFile).getParentFile();
		if (!parentDir.exists()){
			parentDir.mkdirs();
		}
		ConversionSnapshotDelta.deltaFile(new File(fFile), new File (dFile), tempSortingFolder, tempSortedFinalFolder, iniDate, endDate);

		ConversionSnapshotDelta.snapshotFile(new File(fFile), tempSortingFolder, tempSortedFinalFolder, new File(sFile), endDate, null, 0, 0);
	}
	
	
	
	private void joinFiles(String folder1,String folder2,String pattern,String folderDefault, String mustHave, String doesNotMustHave) throws IOException, Exception{
		File folderFile1=new File(folder1);
		File folderFile2=new File(folder2);
		String file1=FileHelper.getFile( folderFile1, pattern, folderDefault,mustHave,doesNotMustHave);
		String file2=FileHelper.getFile( folderFile2, pattern, folderDefault ,mustHave,doesNotMustHave);
		if (file1==null){
			System.out.println("pattern: - " + pattern);
		}
		concatenateFiles(file1,file2);
	}
	
	private void concatenateFiles(String outFile, String inFile) {
		System.out.println(outFile + " - " + inFile);
		HashSet<File> hFile=new HashSet<File>();
		File ouf=new File(outFile);
		File inf=new File(inFile);
		hFile.add(ouf);
		hFile.add(inf);
		CommonUtils.concatFile(hFile, ouf);
	}
	private void concatenateFilesToOutput(String inFile1, String inFile2,String outputFolder) {
		System.out.println(inFile1 + " - " + inFile2);
		HashSet<File> hFile=new HashSet<File>();
		File inf1=new File(inFile1);
		File inf2=new File(inFile2);
		File ouf=new File(outputFolder,inf2.getName());
		hFile.add(inf1);
		hFile.add(inf2);
		CommonUtils.concatFile(hFile, ouf);
	}

	private HashSet<String> getFilesFromFolders(HashSet<String> folders) throws IOException, Exception {
		HashSet<String> result = new HashSet<String>();
		FileHelper fHelper=new FileHelper();
		for (String folder:folders){
			File dir=new File(folder);
			HashSet<String> files=new HashSet<String>();
			fHelper.findAllFiles(dir, files,null,null);
			result.addAll(files);
		}
		return result;

	}

	public void joinReleases(String baseFolder, String deltaFolder,
			String outputPath) throws IOException, Exception {

//		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-relationships",null,null,"stated");
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-relationships",null,"stated",null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-textDefinition",null,null,null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-association-2",null,null,null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-attributevalue",null,null,null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-language",null,null,null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-simple",null,null,null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-simplemaps",null,null,null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-descriptions",null,null,null);
		joinFilesToOutput(baseFolder,deltaFolder,outputPath,"rf2-concepts",null,null,null);
		
	}

	private void joinFilesToOutput(String baseFolder, String deltaFolder,
			String outputPath, String pattern,String folderDefault, String mustHave, String doesNotMustHave) throws IOException, Exception{
		File folderFile1=new File(baseFolder);
		File folderFile2=new File(deltaFolder);
		String file1=FileHelper.getFile( folderFile1, pattern, folderDefault,mustHave,doesNotMustHave);
		String file2=FileHelper.getFile( folderFile2, pattern, folderDefault ,mustHave,doesNotMustHave);
		if (file1==null){
			System.out.println("pattern: - " + pattern);
		}
		concatenateFilesToOutput(file1,file2,outputPath);
		
	}

}
