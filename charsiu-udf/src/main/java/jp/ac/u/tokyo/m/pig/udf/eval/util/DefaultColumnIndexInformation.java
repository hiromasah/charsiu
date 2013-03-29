package jp.ac.u.tokyo.m.pig.udf.eval.util;

import jp.ac.u.tokyo.m.pig.udf.eval.util.MulticastEvaluationConstants.AccessType;

import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class DefaultColumnIndexInformation implements ColumnIndexInformation {
	private final int mIndex;
	private final FieldSchema mFieldSchema;
	private final AccessType mAccessType;
	private ColumnIndexInformation mChild = null;

	public DefaultColumnIndexInformation(int aIndex, FieldSchema aFieldSchema, AccessType aAccessType) {
		mIndex = aIndex;
		mFieldSchema = aFieldSchema;
		mAccessType = aAccessType;
	}

	@Override
	public int getIndex() {
		return mIndex;
	}

	@Override
	public FieldSchema getFieldSchema() {
		return mFieldSchema;
	}

	@Override
	public byte getFieldType() {
		return mFieldSchema.type;
	}

	@Override
	public AccessType getAccessType() {
		return mAccessType;
	}

	@Override
	public ColumnIndexInformation getChild() {
		return mChild;
	}

	void setChild(ColumnIndexInformation aChild) {
		mChild = aChild;
	}

	@Override
	public boolean hasChild() {
		return mChild != null;
	}

	@Override
	public String toString() {
		return "ColumnName [mIndex=" + mIndex + ", mFieldSchema=" + mFieldSchema + ", mAccessType=" + mAccessType + ", mChild=" + mChild + "]";
	}

}
