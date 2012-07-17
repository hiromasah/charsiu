package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public class DPCColumnSchema {

	private final String mName;
	private final String mType;

	public DPCColumnSchema(String aName, String aType) {
		mName = aName;
		mType = aType;
	}

	public String getName() {
		return mName;
	}

	public String getType() {
		return mType;
	}

	@Override
	public boolean equals(Object aTarget) {
		try {
			DPCColumnSchema tTarget = (DPCColumnSchema) aTarget;
			return mName.equals(tTarget.getName());
		} catch (ClassCastException e) {
			return false;
		}
	}
}
