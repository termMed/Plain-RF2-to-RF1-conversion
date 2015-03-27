package org.ihtsdo.conversion.tasks;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.UUID;

import org.ihtsdo.conversion.configuration.MetadataConfig;
import org.ihtsdo.id.create.IdCreation;

public class GenerateRF1Associations_References extends AbstractTask {

	private HashMap<String, String> RF1Association;
	private File snapshotSortedAssociationFile;
	private File previousRf1RelationshipFile;
	private IdCreation idc;
	private File outputAuxAssociations;
	private File RF1SortedConceptsFile;
	private File metadataConceptListFile;
	private File referencesOutputFile;
	private HashMap<String, String[]> RF2References;
	private HashSet<String> RF2DescReferences;
	private String testMode;
	private Long cont;
	public GenerateRF1Associations_References(File RF1SortedConceptsFile,File snapshotSortedAssociationFile,
			File previousRf1RelationshipFile, IdCreation idc,File metadataConceptListFile,
			File outputAuxAssociations, File referencesOutputFile, String testMode) {

		this.RF1SortedConceptsFile=RF1SortedConceptsFile;
		this.snapshotSortedAssociationFile=snapshotSortedAssociationFile;
		this.previousRf1RelationshipFile=previousRf1RelationshipFile;
		this.idc=idc;
		this.outputAuxAssociations=outputAuxAssociations;
		this.referencesOutputFile=referencesOutputFile;
		this.metadataConceptListFile=metadataConceptListFile;
		this.testMode=testMode;
	}

	@Override
	public void execute() {

		try{
			cont=0l;
			HashMap<String,String> prevAssoIds=new HashMap<String, String>();
			FileInputStream fis ;
			InputStreamReader isr;
			BufferedReader br ;
			String line;
			String[] spl;
			String key ;
			if (previousRf1RelationshipFile!=null){
				fis = new FileInputStream(previousRf1RelationshipFile	);
				isr = new InputStreamReader(fis,"UTF-8");
				br = new BufferedReader(isr);

				br.readLine();

				while ((line=br.readLine())!=null){
					spl = line.split("\t",-1);
					if (spl[4].equals("2")){
						key = spl[1] + "-" + spl[2] + "-" + spl[3];
						prevAssoIds.put(key, spl[0]);
					}

				}
				br.close();
				br=null;
				fis=null;
				isr=null;
			}
			fis = new FileInputStream(RF1SortedConceptsFile	);
			isr = new InputStreamReader(fis,"UTF-8");
			br = new BufferedReader(isr);
			br.readLine();
			HashMap<String, String> hashCpt = new HashMap<String,String>();

			while ((line=br.readLine())!=null){
				spl=line.split("\t");
				hashCpt.put(spl[0], spl[1]);
			}
			br.close();
			br=null;
			fis=null;
			isr=null;

			getMetadataValues();
			HashSet<String> metadataCpt=getMetadataConceptHash();

			FileOutputStream fos = new FileOutputStream( outputAuxAssociations);
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

			fis = new FileInputStream(snapshotSortedAssociationFile	);
			isr = new InputStreamReader(fis,"UTF-8");
			br = new BufferedReader(isr);

			br.readLine();

			FileOutputStream rfos = new FileOutputStream( referencesOutputFile);
			OutputStreamWriter rosw = new OutputStreamWriter(rfos,"UTF-8");
			BufferedWriter rbw = new BufferedWriter(rosw);

			rbw.append("COMPONENTID");
			rbw.append("\t");
			rbw.append("REFERENCETYPE");
			rbw.append("\t");
			rbw.append("REFERENCEDID");
			rbw.append("\r\n");

			boolean isValidReference;
			String rType=null;
			String rid=""; 
			String rGroup="0";
			String cType="2";
			String refina="0";
			String assoRelId="";
			String cptStat="";
			String validSourceStat="";
			String c1;
			String c2;
			String[] refTmp ;
			while ((line=br.readLine())!=null){
				spl = line.split("\t");
				c1=spl[5];

				if(spl[2].compareTo("1")==0 && !metadataCpt.contains(c1)){

					c2=spl[6];
					isValidReference=false;
					if (RF2References.containsKey(spl[4])){
						refTmp = RF2References.get(spl[4]);
						validSourceStat=refTmp[0];
						cptStat=hashCpt.get(c1);
						if ((cptStat!=null && cptStat.compareTo(validSourceStat)==0 ) 
								|| RF2DescReferences.contains(spl[4])){
							//									is a reference
							isValidReference=true;
							rbw.append(c1);
							rbw.append("\t");
							rbw.append(refTmp[1]);
							rbw.append("\t");
							rbw.append(c2);
							rbw.append("\r\n");

						}
					}
					if (!isValidReference){
						rType=RF1Association.get(spl[4]);
						assoRelId="";
						rid=spl[0]; 
						key=c1 + "-" + rType + "-" + c2;
						assoRelId=prevAssoIds.get(key);
						if (assoRelId==null){
							Long assoId=null;
							if (testMode==null){
								assoId=idc.getId(UUID.fromString(rid));
							}else{
								cont++;
								assoId=cont;
							}
							if (assoId!=null){
								assoRelId=assoId.toString();
							}
						}
						bw.append(assoRelId);
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

					}
				}
			}
			br.close();
			br=null;
			fis=null;
			isr=null;
			bw.close();
			rbw.close();
			hashCpt=null;
		}catch(Exception e){

		}
	}

	private void getMetadataValues()  throws Exception {
		MetadataConfig config =new MetadataConfig();

		RF1Association=config.getRF2RF1AssociationMap();
		RF2References=config.getRF2References();
		RF2DescReferences=config.getRF2DescriptionReferences();

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
