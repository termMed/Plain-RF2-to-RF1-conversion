package org.ihtsdo.conversion.utils;

public enum FILE_TYPE {

	RF2_CONCEPT(new int[]{0,1},new int[]{0},new Integer[]{2,3,4},"sct2_Concept_SUFFIX_NAMESPACE",1),
	RF2_DESCRIPTION(new int[]{0,1},new int[]{0},new Integer[]{2,3,4,5,6,7,8},"sct2_Description_SUFFIXLANGUAGECODE_NAMESPACE",1),
	RF2_RELATIONSHIP(new int[]{0,1},new int[]{0},new Integer[]{2,3,4,5,6,7,8,9},"sct2_Relationship_SUFFIX_NAMESPACE",1), 
	RF2_STATED_RELATIONSHIP(new int[]{0,1},new int[]{0},new Integer[]{2,3,4,5,6,7,8,9},"sct2_StatedRelationship_SUFFIX_NAMESPACE",1), 
	RF2_IDENTIFIER(new int[]{1,2},new int[]{1},new Integer[]{3,4,5},"sct2_Identifier_SUFFIX_NAMESPACE",2),
	RF2_COMPATIBILITY_IDENTIFIER(new int[]{1,2},new int[]{1},new Integer[]{5},"res2_Identifier_SUFFIX_NAMESPACE",2),
	RF2_TEXTDEFINITION(new int[]{0,1},new int[]{0},new Integer[]{2,4,5,6,7,8},"sct2_TextDefinition_SUFFIXLANGUAGECODE_NAMESPACE",1),
	RF2_LANGUAGE_REFSET(new int[]{0,1},new int[]{0},new Integer[]{2,4,5,6},"der2_cRefset_LanguageSUFFIXLANGUAGECODE_NAMESPACE",1), 
	RF2_ATTRIBUTE_VALUE(new int[]{0,1},new int[]{0},new Integer[]{2,4,5,6},"der2_cRefset_AttributeValueSUFFIX_NAMESPACE",1),
	RF2_SIMPLE_MAP(new int[]{4,5,6,1},new int[]{4,5,6},new Integer[]{2},"der2_sRefset_SimpleMapSUFFIX_NAMESPACE",1),
	RF2_SIMPLE(new int[]{4,5,1},new int[]{4,5},new Integer[]{2},"der2_Refset_SimpleSUFFIX_NAMESPACE",1),
	RF2_ASSOCIATION(new int[]{4,5,6,1},new int[]{4,5,6},new Integer[]{2},"der2_cRefset_AssociationReferenceSUFFIX_NAMESPACE",1),
	RF2_QUALIFIER(new int[]{0,1},new int[]{0},new Integer[]{2,4,5,6,7,8,9},"sct2_Qualifier_SUFFIX_NAMESPACE",1),
	RF2_ICD9_MAP(new int[]{0,1},new int[]{0},new Integer[]{2,4,5,6,7,8,9,10,11},"der2_iissscRefset_ICD9CMEquivalenceMapSUFFIX_NAMESPACE",1), 
	RF2_ISA_RETIRED(new int[]{0,1},new int[]{0},new Integer[]{2,3,4,5,6,7,8,9},"res2_RetiredIsaRelationship_SUFFIX_NAMESPACE",1), 
	RF2_ICDO_TARGETS(new int[]{6,1},new int[]{6},new Integer[]{2},"res2_CrossMapTargets_ICDO_NAMESPACE",1), 
	RF2_STATED_ISA_RETIRED(new int[]{0,1},new int[]{0},new Integer[]{2,3,4,5,6,7,8,9},"res2_RetiredStatedIsaRelationship_SUFFIX_NAMESPACE",1), 
	
	RF1_RELATIONSHIP(new int[]{0},new int[]{0},new Integer[]{2,3,4,5,6},"sct_Relationship_SUFFIX_NAMESPACE",1),
	RF1_STATED_RELATIONSHIP(new int[]{0},new int[]{0},new Integer[]{2,3,4,5,6},"sct_StatedRelationship_SUFFIX_NAMESPACE",1);
	

	private int[] columnIndexes;
	private Integer[] columnsToCompare;
	private int[] snapshotIndex;
	private String fileName;
	private int effectiveTimeColIndex;
	
	public Integer[] getColumnsToCompare() {
		return columnsToCompare;
	}

	FILE_TYPE(int[] columnIndexes,int[] snapshotIndex,Integer[] columnsToCompare,String fileName, int effectiveTimeColIndex){
		this.columnIndexes=columnIndexes;
		this.columnsToCompare=columnsToCompare;
		this.snapshotIndex=snapshotIndex;
		this.fileName=fileName;
		this.effectiveTimeColIndex=effectiveTimeColIndex;
	}

	public int[] getColumnIndexes() {
		return columnIndexes;
	}

	public int[] getSnapshotIndex() {
		return snapshotIndex;
	}

	public String getFileName() {
		return fileName;
	}
	public int getEffectiveTimeColIndex(){
		return effectiveTimeColIndex;
	}
}
