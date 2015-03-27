package org.ihtsdo.conversion.configuration;


import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeSet;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class MetadataConfig {


	private static Logger logger = Logger.getLogger(MetadataConfig.class.getName());
	private String RF2_FSN;
	private String RF2_SYNONYM;
	private String RF2_PREFERRED;
	private String RF2_ACCEPTABLE;
	private String RF2_EN_REFSET;

	private String RF2_ICS_SIGNIFICANT;

	private String RF2_DEF_STATUS_PRIMITIVE;

	private String RF2_CTV3ID_REFSETID;
	private String RF2_SNOMEDID_REFSETID;

	private String RF1_FSN;
	private String RF1_SYNONYM;
	private String RF1_PREFERRED;

	private String RF1_EN_SUBSET;
	private String RF1_GB_SUBSET;
	private String RF1_GBLANG_CODE;
	private String RF1_USLANG_CODE;
	private String RF1_ENLANG_CODE;

	private XMLConfiguration xmlConfig;
	private String RF1_SUBSET_DEFINED;
	private String RF2_ENUS_REFSET;
	private String RF2_ENGB_REFSET;
	
	private String RF2_INACT_CONCEPT_REFSET;
	private String RF2_INACT_DESCRIPTION_REFSET;

	private String RF2_REFINABILITY_REFSETID;
	//	refina 900000000000488004

	private HashMap<String,String> RF2RF1RefinaMap;
	private HashMap<String,String> RF2RF1charTypeMap;
	private HashMap<String, String> RF2RF1inactStatMap;
	private HashMap<String, String> RF2RF1AssociationMap;
	private String RF2_ISA_RELATIONSHIP;
	private HashMap<String, String> RF1RF2charTypeMap;
	private String RF2ModifierSome;
	private TreeSet<String> RF1CauseInactiveConcept;
	private String RF1_ISA_RELATIONSHIP;
	private HashMap<String, String> RF1CptRetiredCauseStatusMap;
	private HashMap<String, String[]> RF2References;
	private HashMap<String, String> RF1ConceptSubsetFileNamePreffix;
	private String ICD9_MapSetId;
	private String ICDO_MapSetId;
	private String RF2_ICDO_REFSETID;
	private String RF2_ICD9_REFSETID;
	private String snomedRootSCTID;
	private String metadataModelSCTID;
	private String namespaceCptSCTID;
	private String linkageCptSCTID;
	private String specialCptSCTID;
	private HashSet<String> RF2DescriptionReferences;


	public MetadataConfig() throws Exception {

		//		this.configFile = configFile;
		File configFile= new File("config/Metadata.xml");
//		System.out.println("file:" + configFile.getPath() + " exists:" + configFile.exists());
		try {
			xmlConfig=new XMLConfiguration(configFile);
			RF2_FSN=xmlConfig.getString("rf2.descriptionType.fsn");
			RF2_SYNONYM=xmlConfig.getString("rf2.descriptionType.synonym");
			RF2_PREFERRED=xmlConfig.getString("rf2.acceptability.preferred");
			RF2_ACCEPTABLE=xmlConfig.getString("rf2.acceptability.acceptable");
			RF2_EN_REFSET=xmlConfig.getString("rf2.enRefset");
			RF2_ENUS_REFSET=xmlConfig.getString("rf2.enUSRefset");
			RF2_ENGB_REFSET=xmlConfig.getString("rf2.enGBRefset");

			RF2_ICS_SIGNIFICANT=xmlConfig.getString("rf2.ics.significant");

			RF2_DEF_STATUS_PRIMITIVE=xmlConfig.getString("rf2.definitionStatus.primitive");

			RF2_CTV3ID_REFSETID=xmlConfig.getString("rf2.ctv3idRefset");
			RF2_SNOMEDID_REFSETID=xmlConfig.getString("rf2.snomedIdRefset");

			RF2_REFINABILITY_REFSETID=xmlConfig.getString("rf2.refinabilityRefset");

			RF1_FSN=xmlConfig.getString("rf1.fsn");
			RF1_SYNONYM=xmlConfig.getString("rf1.synonym");
			RF1_PREFERRED=xmlConfig.getString("rf1.preferred");
			RF1_SUBSET_DEFINED=xmlConfig.getString("rf1.subsetDefined");
			RF1_EN_SUBSET=xmlConfig.getString("rf1.enSubset");
			RF1_GB_SUBSET=xmlConfig.getString("rf1.gbSubset");
			RF1_GBLANG_CODE=xmlConfig.getString("rf1.gbLangCode");
			RF1_USLANG_CODE=xmlConfig.getString("rf1.usLangCode");
			RF1_ENLANG_CODE=xmlConfig.getString("rf1.enLangCode");

			RF2RF1RefinaMap=new HashMap<String, String>();
			RF2RF1RefinaMap.put(xmlConfig.getString("rf2.refinability.notRefinable"),xmlConfig.getString("rf1.refinability.notRefinable"));
			RF2RF1RefinaMap.put(xmlConfig.getString("rf2.refinability.optional"),xmlConfig.getString("rf1.refinability.optional"));
			RF2RF1RefinaMap.put(xmlConfig.getString("rf2.refinability.mandatory"),xmlConfig.getString("rf1.refinability.mandatory"));

			RF2RF1charTypeMap=new HashMap<String, String>();
			RF2RF1charTypeMap.put(xmlConfig.getString("rf2.characteristicType.DefiningRelationship"),xmlConfig.getString("rf1.characteristicType.DefiningRelationship"));
			RF2RF1charTypeMap.put(xmlConfig.getString("rf2.characteristicType.InferredRelationship"),xmlConfig.getString("rf1.characteristicType.DefiningRelationship"));
			RF2RF1charTypeMap.put(xmlConfig.getString("rf2.characteristicType.StatedRelationship"),xmlConfig.getString("rf1.characteristicType.DefiningRelationship"));
			RF2RF1charTypeMap.put(xmlConfig.getString("rf2.characteristicType.QualifyingRelationship"),xmlConfig.getString("rf1.characteristicType.QualifyingRelationship"));
			RF2RF1charTypeMap.put(xmlConfig.getString("rf2.characteristicType.AdditionalRelationship"),xmlConfig.getString("rf1.characteristicType.AdditionalRelationship"));

			RF1RF2charTypeMap=new HashMap<String, String>();
			RF1RF2charTypeMap.put(xmlConfig.getString("rf1.characteristicType.DefiningRelationship"),xmlConfig.getString("rf2.characteristicType.DefiningRelationship"));
			RF1RF2charTypeMap.put(xmlConfig.getString("rf1.characteristicType.QualifyingRelationship"),xmlConfig.getString("rf2.characteristicType.QualifyingRelationship"));
			RF1RF2charTypeMap.put(xmlConfig.getString("rf1.characteristicType.AdditionalRelationship"),xmlConfig.getString("rf2.characteristicType.AdditionalRelationship"));

			RF2ModifierSome=xmlConfig.getString("rf2.modifier.some");
			
			RF2_INACT_CONCEPT_REFSET=xmlConfig.getString("rf2.conceptInactivationRefset");
			RF2_INACT_DESCRIPTION_REFSET=xmlConfig.getString("rf2.descriptionInactivationRefset");

			
			RF2RF1inactStatMap=new HashMap<String, String>();
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.NotStatedReason"),xmlConfig.getString("rf1.inactivationStatus.NotStatedReason"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Limited"),xmlConfig.getString("rf1.inactivationStatus.Limited"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Duplicate"),xmlConfig.getString("rf1.inactivationStatus.Duplicate"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Pending_move"),xmlConfig.getString("rf1.inactivationStatus.Pending_move"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Ambiguous"),xmlConfig.getString("rf1.inactivationStatus.Ambiguous"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Moved_elsewhere"),xmlConfig.getString("rf1.inactivationStatus.Moved_elsewhere"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Concept_non-current"),xmlConfig.getString("rf1.inactivationStatus.Concept_non-current"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Outdated"),xmlConfig.getString("rf1.inactivationStatus.Outdated"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Inappropriate"),xmlConfig.getString("rf1.inactivationStatus.Inappropriate"));
			RF2RF1inactStatMap.put(xmlConfig.getString("rf2.inactivationStatus.Erroneous"),xmlConfig.getString("rf1.inactivationStatus.Erroneous"));

			RF2RF1AssociationMap=new HashMap<String, String>();

			Document doc = xmlConfig.getDocument();
			NodeList elems1 = doc.getElementsByTagName("rf1");
			Node node1=null;
			Node elem1=null;
			if (elems1.getLength()>0){
				elem1=elems1.item(0);
				NodeList nodeL1=elem1.getChildNodes();
				for (int i=0;i<nodeL1.getLength();i++){
					node1=nodeL1.item(i);
					if (node1.getNodeName().equals("association")){
						break;
					}
				}
			}
			NodeList elems2 = doc.getElementsByTagName("rf2");
			Node node2=null;
			Node elem2=null;
			if (elems2.getLength()>0){
				elem2=elems2.item(0);
				NodeList nodeL2=elem2.getChildNodes();
				for (int i=0;i<nodeL2.getLength();i++){
					node2=nodeL2.item(i);
					if (node2.getNodeName().equals("association")){
						break;
					}
				}
			}
			if (node1!=null && node1.getNodeName().equals("association") ){
				if (node2!=null && node2.getNodeName().equals("association") ){
					NodeList assocs1=node1.getChildNodes();
					NodeList assocs2=node2.getChildNodes();
					for (int i=0;i<assocs1.getLength();i++){
						Node child1=assocs1.item(i);
						if (!child1.getTextContent().trim().equals("")){
						for (int j=1;j<assocs2.getLength();j++){
							Node child2=assocs2.item(j);
							if (child2.getNodeName().equals(child1.getNodeName())){
								RF2RF1AssociationMap.put(child2.getTextContent(), child1.getTextContent());
								
							}
						}
						}
					}
				}
			}
			
//			RF2RF1AssociationMap.put(xmlConfig.getString("rf2.association.Part_Of"),xmlConfig.getString("rf1.association.Part_Of"));
//			RF2RF1AssociationMap.put(xmlConfig.getString("rf2.association.May_Be"),xmlConfig.getString("rf1.association.May_Be"));
//			RF2RF1AssociationMap.put(xmlConfig.getString("rf2.association.Was_A"),xmlConfig.getString("rf1.association.Was_A"));
//			RF2RF1AssociationMap.put(xmlConfig.getString("rf2.association.Same_As"),xmlConfig.getString("rf1.association.Same_As"));
//			RF2RF1AssociationMap.put(xmlConfig.getString("rf2.association.Replaced_By"),xmlConfig.getString("rf1.association.Replaced_By"));
//			RF2RF1AssociationMap.put(xmlConfig.getString("rf2.association.Moved_To"),xmlConfig.getString("rf1.association.Moved_To"));
//			RF2RF1AssociationMap.put(xmlConfig.getString("rf2.association.Moved_From"),xmlConfig.getString("rf1.association.Moved_From"));


			RF1CauseInactiveConcept=new TreeSet<String>();
			RF1CauseInactiveConcept.add(xmlConfig.getString("rf1.inactiveConcept.ambiguous"));
			RF1CauseInactiveConcept.add(xmlConfig.getString("rf1.inactiveConcept.duplicate"));
			RF1CauseInactiveConcept.add(xmlConfig.getString("rf1.inactiveConcept.erroneous"));
			RF1CauseInactiveConcept.add(xmlConfig.getString("rf1.inactiveConcept.limited"));
			RF1CauseInactiveConcept.add(xmlConfig.getString("rf1.inactiveConcept.moved"));
			RF1CauseInactiveConcept.add(xmlConfig.getString("rf1.inactiveConcept.outdated"));
			RF1CauseInactiveConcept.add(xmlConfig.getString("rf1.inactiveConcept.reasonNotStated"));

			RF1CptRetiredCauseStatusMap=new HashMap<String,String>();

			RF1CptRetiredCauseStatusMap.put(xmlConfig.getString("rf1.inactiveConcept.ambiguous"),xmlConfig.getString("rf1.inactivationStatus.Ambiguous"));
			RF1CptRetiredCauseStatusMap.put(xmlConfig.getString("rf1.inactiveConcept.duplicate"),xmlConfig.getString("rf1.inactivationStatus.Duplicate"));
			RF1CptRetiredCauseStatusMap.put(xmlConfig.getString("rf1.inactiveConcept.erroneous"),xmlConfig.getString("rf1.inactivationStatus.Erroneous"));
			RF1CptRetiredCauseStatusMap.put(xmlConfig.getString("rf1.inactiveConcept.limited"),xmlConfig.getString("rf1.inactivationStatus.Limited"));
			RF1CptRetiredCauseStatusMap.put(xmlConfig.getString("rf1.inactiveConcept.moved"),xmlConfig.getString("rf1.inactivationStatus.Moved_elsewhere"));
			RF1CptRetiredCauseStatusMap.put(xmlConfig.getString("rf1.inactiveConcept.outdated"),xmlConfig.getString("rf1.inactivationStatus.Outdated"));
			RF1CptRetiredCauseStatusMap.put(xmlConfig.getString("rf1.inactiveConcept.reasonNotStated"),xmlConfig.getString("rf1.inactivationStatus.NotStatedReason"));
			
			RF2_ISA_RELATIONSHIP=xmlConfig.getString("rf2.isarelationship");
			RF1_ISA_RELATIONSHIP=xmlConfig.getString("rf1.isarelationship");
			
			ICD9_MapSetId=xmlConfig.getString("rf1.crossMap.ICD9_mapSetId");
			ICDO_MapSetId=xmlConfig.getString("rf1.crossMap.ICDO_mapSetId");
		
			RF2_ICD9_REFSETID=xmlConfig.getString("rf2.ICD9Refset");
			RF2_ICDO_REFSETID=xmlConfig.getString("rf2.ICDORefset");
			
			RF2References=new HashMap<String,String[]>();
			
			RF2References.put(xmlConfig.getString("rf2.references.Replaced_By.refset"),new String[]{xmlConfig.getString("rf2.references.Replaced_By.validSourceStatus"),xmlConfig.getString("rf2.references.Replaced_By.rf1Value")});
			RF2References.put(xmlConfig.getString("rf2.references.Alternative.refset"),new String[]{xmlConfig.getString("rf2.references.Alternative.validSourceStatus"),xmlConfig.getString("rf2.references.Alternative.rf1Value")});
			RF2References.put(xmlConfig.getString("rf2.references.Refers_To.refset"),new String[]{xmlConfig.getString("rf2.references.Refers_To.validSourceStatus"),xmlConfig.getString("rf2.references.Refers_To.rf1Value")});
			
			RF2DescriptionReferences=new HashSet<String>();
			RF2DescriptionReferences.add(xmlConfig.getString("rf2.references.Refers_To.refset"));
			
			RF1ConceptSubsetFileNamePreffix=new HashMap<String,String>();
			RF1ConceptSubsetFileNamePreffix.put(xmlConfig.getString("rf1.conceptSubset.vmp.originalSubsetId"),xmlConfig.getString("rf1.conceptSubset.vmp.suffixForFile"));
			RF1ConceptSubsetFileNamePreffix.put(xmlConfig.getString("rf1.conceptSubset.vtm.originalSubsetId"),xmlConfig.getString("rf1.conceptSubset.vtm.suffixForFile"));
			RF1ConceptSubsetFileNamePreffix.put(xmlConfig.getString("rf1.conceptSubset.nonHuman.originalSubsetId"),xmlConfig.getString("rf1.conceptSubset.nonHuman.suffixForFile"));
			
			snomedRootSCTID=xmlConfig.getString("rf2.snomedRootSCTID");
			metadataModelSCTID=xmlConfig.getString("rf2.metadataModelSCTID");
			namespaceCptSCTID=xmlConfig.getString("rf2.namespaceCptSCTID");
			linkageCptSCTID=xmlConfig.getString("rf2.linkageCptSCTID");
			specialCptSCTID=xmlConfig.getString("rf2.specialCptSCTID");
		} catch (ConfigurationException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
			throw new Exception(e.getMessage());
		}
	}


	public HashMap<String, String> getRF1CptRetiredCauseStatusMap() {
		return RF1CptRetiredCauseStatusMap;
	}


	public String getRF1_ISA_RELATIONSHIP() {
		return RF1_ISA_RELATIONSHIP;
	}


	public TreeSet<String> getRF1CauseInactiveConcept() {
		return RF1CauseInactiveConcept;
	}


	public String getRF2ModifierSome() {
		return RF2ModifierSome;
	}


	public HashMap<String, String> getRF1RF2charTypeMap() {
		return RF1RF2charTypeMap;
	}


	public String getRF2_FSN() {
		return RF2_FSN;
	}


	public String getRF2_SYNONYM() {
		return RF2_SYNONYM;
	}


	public String getRF2_PREFERRED() {
		return RF2_PREFERRED;
	}


	public String getRF2_EN_REFSET() {
		return RF2_EN_REFSET;
	}


	public String getRF2_ICS_SIGNIFICANT() {
		return RF2_ICS_SIGNIFICANT;
	}


	public String getRF2_DEF_STATUS_PRIMITIVE() {
		return RF2_DEF_STATUS_PRIMITIVE;
	}


	public String getRF2_CTV3ID_REFSETID() {
		return RF2_CTV3ID_REFSETID;
	}


	public String getRF2_SNOMEDID_REFSETID() {
		return RF2_SNOMEDID_REFSETID;
	}


	public String getRF1_FSN() {
		return RF1_FSN;
	}


	public String getRF1_SYNONYM() {
		return RF1_SYNONYM;
	}


	public String getRF1_PREFERRED() {
		return RF1_PREFERRED;
	}


	public String getRF1_EN_SUBSET() {
		return RF1_EN_SUBSET;
	}


	public String getRF1_GB_SUBSET() {
		return RF1_GB_SUBSET;
	}


	public String getRF1_GBLANG_CODE() {
		return RF1_GBLANG_CODE;
	}


	public String getRF1_USLANG_CODE() {
		return RF1_USLANG_CODE;
	}


	public String getRF1_ENLANG_CODE() {
		return RF1_ENLANG_CODE;
	}


	public String getRF1_SUBSET_DEFINED() {
		return RF1_SUBSET_DEFINED;
	}


	public String getRF2_ACCEPTABLE() {
		return RF2_ACCEPTABLE;
	}


	public String getRF2_ENUS_REFSET() {
		return RF2_ENUS_REFSET;
	}


	public String getRF2_ENGB_REFSET() {
		return RF2_ENGB_REFSET;
	}


	public String getRF2_REFINABILITY_REFSETID() {
		return RF2_REFINABILITY_REFSETID;
	}


	public HashMap<String, String> getRF2RF1RefinaMap() {
		return RF2RF1RefinaMap;
	}


	public HashMap<String, String> getRF2RF1charTypeMap() {
		return RF2RF1charTypeMap;
	}


	/**
	 * @return the rF2RF1inactStatMap
	 */
	public HashMap<String, String> getRF2RF1inactStatMap() {
		return RF2RF1inactStatMap;
	}


	/**
	 * @return the rF2_INACT_CONCEPT_REFSET
	 */
	public String getRF2_INACT_CONCEPT_REFSET() {
		return RF2_INACT_CONCEPT_REFSET;
	}


	/**
	 * @return the rF2_INACT_DESCRIPTION_REFSET
	 */
	public String getRF2_INACT_DESCRIPTION_REFSET() {
		return RF2_INACT_DESCRIPTION_REFSET;
	}


	public HashMap<String, String> getRF2RF1AssociationMap() {
		return RF2RF1AssociationMap;
	}


	/**
	 * @return the rF2_ISA_RELATIONSHIP
	 */
	public String getRF2_ISA_RELATIONSHIP() {
		return RF2_ISA_RELATIONSHIP;
	}


	public String getICD9_MapSetId() {
		return ICD9_MapSetId;
	}


	public String getICDO_MapSetId() {
		return ICDO_MapSetId;
	}


	public String getRF2_ICDO_REFSETID() {
		return RF2_ICDO_REFSETID;
	}
	

	public String getRF2_ICD9_REFSETID() {
		return RF2_ICD9_REFSETID;
	}


	public HashMap<String, String[]> getRF2References() {
		return RF2References;
	}


	public String getSnomedRootSCTID() {
		return snomedRootSCTID;
	}


	public String getMetadataModelSCTID() {
		return metadataModelSCTID;
	}


	public String getNamespaceCptSCTID() {
		return namespaceCptSCTID;
	}


	public String getLinkageCptSCTID() {
		return linkageCptSCTID;
	}


	public String getSpecialCptSCTID() {
		return specialCptSCTID;
	}


	public HashMap<String, String> getRF1ConceptSubsetFileNamePreffix() {
		return RF1ConceptSubsetFileNamePreffix;
	}


	public HashSet<String> getRF2DescriptionReferences() {
		return RF2DescriptionReferences;
	}
	

}
