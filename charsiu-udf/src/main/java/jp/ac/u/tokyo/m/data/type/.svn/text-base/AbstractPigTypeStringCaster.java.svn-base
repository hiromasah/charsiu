package jp.ac.u.tokyo.m.data.type;

public abstract class AbstractPigTypeStringCaster<RESULT_TYPE> implements TypeStringCaster<RESULT_TYPE> {
	public RESULT_TYPE castTypeString(String aTypeString) {
		String tTypeStringLower = aTypeString.toLowerCase();
		if (tTypeStringLower.equals("float")) {
			return caseFloat();
		} else if (tTypeStringLower.equals("double")) {
			return caseDouble();
		} else if (tTypeStringLower.equals("int")) {
			return caseInt();
		} else if (tTypeStringLower.equals("long")) {
			return caseLong();
		} else {
			return caseString();
		}
	}
}
