package org.ihtsdo.conversion.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;

import org.apache.log4j.Logger;

public class SubsetMetadata implements Cloneable {
	private static Logger logger = Logger.getLogger(SubsetMetadata.class.getName());
	private String conceptId;
	private String effectiveTime;
	private String active;
	private String moduleId;
	private String SUBSETID;
	private String SUBSETORIGINALID;
	private String SUBSETVERSION;
	private String SUBSETNAME;
	private String SUBSETTYPE;
	private String LANGUAGECODE;
	private String REALMID;
	private String CONTEXTID;

	public SubsetMetadata() {
		super();
	}
	public SubsetMetadata(String conceptId, String effectiveTime,
			String active, String moduleId, String sUBSETID,
			String sUBSETORIGINALID, String sUBSETVERSION, String sUBSETNAME,
			String sUBSETTYPE, String lANGUAGECODE, String rEALMID,
			String cONTEXTID) {
		super();
		this.conceptId = conceptId;
		this.effectiveTime = effectiveTime;
		this.active = active;
		this.moduleId = moduleId;
		SUBSETID = sUBSETID;
		SUBSETORIGINALID = sUBSETORIGINALID;
		SUBSETVERSION = sUBSETVERSION;
		SUBSETNAME = sUBSETNAME;
		SUBSETTYPE = sUBSETTYPE;
		LANGUAGECODE = lANGUAGECODE;
		REALMID = rEALMID;
		CONTEXTID = cONTEXTID;
	}

	public String getConceptId() {
		return conceptId;
	}

	public void setConceptId(String conceptId) {
		this.conceptId = conceptId;
	}

	public String getEffectiveTime() {
		return effectiveTime;
	}

	public void setEffectiveTime(String effectiveTime) {
		this.effectiveTime = effectiveTime;
	}

	public String getActive() {
		return active;
	}

	public void setActive(String active) {
		this.active = active;
	}

	public String getModuleId() {
		return moduleId;
	}

	public void setModuleId(String moduleId) {
		this.moduleId = moduleId;
	}

	public String getSUBSETID() {
		return SUBSETID;
	}

	public void setSUBSETID(String sUBSETID) {
		SUBSETID = sUBSETID;
	}

	public String getSUBSETORIGINALID() {
		return SUBSETORIGINALID;
	}

	public void setSUBSETORIGINALID(String sUBSETORIGINALID) {
		SUBSETORIGINALID = sUBSETORIGINALID;
	}

	public String getSUBSETVERSION() {
		return SUBSETVERSION;
	}

	public void setSUBSETVERSION(String sUBSETVERSION) {
		SUBSETVERSION = sUBSETVERSION;
	}

	public String getSUBSETNAME() {
		return SUBSETNAME;
	}

	public void setSUBSETNAME(String sUBSETNAME) {
		SUBSETNAME = sUBSETNAME;
	}

	public String getSUBSETTYPE() {
		return SUBSETTYPE;
	}

	public void setSUBSETTYPE(String sUBSETTYPE) {
		SUBSETTYPE = sUBSETTYPE;
	}

	public String getLANGUAGECODE() {
		return LANGUAGECODE;
	}

	public void setLANGUAGECODE(String lANGUAGECODE) {
		LANGUAGECODE = lANGUAGECODE;
	}

	public String getREALMID() {
		return REALMID;
	}

	public void setREALMID(String rEALMID) {
		REALMID = rEALMID;
	}

	public String getCONTEXTID() {
		return CONTEXTID;
	}

	public void setCONTEXTID(String cONTEXTID) {
		CONTEXTID = cONTEXTID;
	}

	public String toString() {
		return getSUBSETNAME();
	}
	public int compareTo(Object other) {
		final int BEFORE = -1;
		final int EQUAL = 0;
		final int AFTER = 1;

		SubsetMetadata otherSubsetMetadata = (SubsetMetadata) other;
		
		if (this.getActive().equals(otherSubsetMetadata.getActive()) &&
				this.getConceptId().equals(otherSubsetMetadata.getConceptId()) &&
				this.getCONTEXTID().equals(otherSubsetMetadata.getCONTEXTID()) &&
				this.getEffectiveTime().equals(otherSubsetMetadata.getEffectiveTime()) &&
				this.getLANGUAGECODE().equals(otherSubsetMetadata.getLANGUAGECODE()) &&
				this.getModuleId().equals(otherSubsetMetadata.getModuleId()) &&
				this.getREALMID().equals(otherSubsetMetadata.getREALMID()) &&
				this.getSUBSETID().equals(otherSubsetMetadata.getSUBSETID()) &&
				this.getSUBSETNAME().equals(otherSubsetMetadata.getSUBSETNAME()) &&
				this.getSUBSETORIGINALID().equals(otherSubsetMetadata.getSUBSETORIGINALID()) &&
				this.getSUBSETTYPE().equals(otherSubsetMetadata.getSUBSETTYPE()) &&
				this.getSUBSETVERSION().equals(otherSubsetMetadata.getSUBSETVERSION())) {
			return 0;
		} else {
			return -1;
		}
	}
	
	@Override
	public SubsetMetadata clone() throws CloneNotSupportedException {
		SubsetMetadata clone=(SubsetMetadata)super.clone();
		return clone;
	}
	
	public enum SubsetType {
		LANGUAGE(1),REALM_CONCEPT(2),REALM_DESCRIPTION(3),REALM_RELATIONSHIP(4),
		CONTEXT_CONCEPT(5),CONTEXT_DESCRIPTION(6),NAVIGATION(7),DUPLICATE_TERMS(8);

		private int code;

		private SubsetType(int c) {
			code = c;
		}

		public int getCode() {
			return code;
		}
		
		public static SubsetType getType(int c) {
			for (SubsetType loopType : SubsetType.values()) {
				if (loopType.getCode() == c) {
					return loopType;
				}
			}
			return null;
		}
		
		public static SubsetType[] getValues() {
			return values();
		}
	}

	public String getRF1HeaderFileContent(){
		StringBuffer ret=new StringBuffer();
		ret.append("SUBSETID	SUBSETORIGINALID	SUBSETVERSION	SUBSETNAME	SUBSETTYPE	LANGUAGECODE	REALMID	CONTEXTID");
		ret.append("\r\n");
		ret.append(SUBSETID);
		ret.append("\t");
		ret.append(SUBSETORIGINALID);
		ret.append("\t");
		ret.append(SUBSETVERSION);
		ret.append("\t");
		ret.append(SUBSETNAME);
		ret.append("\t");
		ret.append(SUBSETTYPE);
		ret.append("\t");
		ret.append(LANGUAGECODE);
		ret.append("\t");
		ret.append(REALMID);
		ret.append("\t");
		ret.append(CONTEXTID);
		ret.append("\r\n");
		return ret.toString();
	}

	public void writeRF1HeaderFile(File outputFile){

		FileOutputStream fos;
		try {
			if (!outputFile.getParentFile().exists()) {
				outputFile.getParentFile().mkdirs();
			}
			
			fos = new FileOutputStream( outputFile);
			OutputStreamWriter osw = new OutputStreamWriter(fos,"UTF-8");
			BufferedWriter bw = new BufferedWriter(osw);

			bw.append(getRF1HeaderFileContent());

			bw.close();
			
		} catch (FileNotFoundException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (UnsupportedEncodingException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		} catch (IOException e) {
			logger.log(org.apache.log4j.Level.ERROR, e.getMessage(), e);
		}
	}
}
