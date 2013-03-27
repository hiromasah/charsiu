package jp.ac.u.tokyo.m.pig.udf.eval.util;

import jp.ac.u.tokyo.m.pig.udf.eval.util.MulticastEvaluationConstants.AccessType;

import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class StringableColumnIndexInformation implements ColumnIndexInformation {
	private final int mIndex;
	private final byte mFieldType;
	private final AccessType mAccessType;
	private final ColumnIndexInformation mChild;

	public StringableColumnIndexInformation(int aIndex, byte aFieldType, AccessType aAccessType, ColumnIndexInformation aChild) {
		mIndex = aIndex;
		mFieldType = aFieldType;
		mAccessType = aAccessType;
		mChild = aChild;
	}

	@Override
	public int getIndex() {
		return mIndex;
	}

	@Override
	public FieldSchema getFieldSchema() {
		return null;
	}

	@Override
	public byte getFieldType() {
		return mFieldType;
	}

	@Override
	public AccessType getAccessType() {
		return mAccessType;
	}

	@Override
	public ColumnIndexInformation getChild() {
		return mChild;
	}

	@Override
	public boolean hasChild() {
		return mChild != null;
	}

	@Override
	public String toString() {
		return toString(this);
	}

	public static String toString(ColumnIndexInformation aTarget) {
		String tDelimiter = MulticastEvaluationConstants.STRINGABLE_COLUMN_INDEX_INFORMATION_DELIMITER;
		ColumnIndexInformation tTargetChild = aTarget.getChild();
		StringBuilder tResultBuilder = new StringBuilder(MulticastEvaluationConstants.STRINGABLE_COLUMN_INDEX_INFORMATION_HEAD_MARKER);
		tResultBuilder.append(aTarget.getIndex());
		tResultBuilder.append(tDelimiter);
		tResultBuilder.append(aTarget.getFieldType());
		tResultBuilder.append(tDelimiter);
		tResultBuilder.append(aTarget.getAccessType().ordinal());
		tResultBuilder.append(tDelimiter);
		if (tTargetChild != null)
			tResultBuilder.append(toString(tTargetChild));
		return tResultBuilder.toString();
	}

	public static StringableColumnIndexInformation parse(String aColumnIndexInformationString) {
		String tDelimiter = MulticastEvaluationConstants.STRINGABLE_COLUMN_INDEX_INFORMATION_DELIMITER;
		AccessType[] tAccessTypeValues = AccessType.values();

		StringableColumnIndexInformation tChild = null;
		String[] tColumnIndexInformationStringElements = aColumnIndexInformationString.split(MulticastEvaluationConstants.STRINGABLE_COLUMN_INDEX_INFORMATION_HEAD_MARKER_REGEX);
		for (int tIndex = tColumnIndexInformationStringElements.length - 1; tIndex > 0; tIndex--) {
			String[] tColumnIndexInformationFields = tColumnIndexInformationStringElements[tIndex].split(tDelimiter);
			tChild = new StringableColumnIndexInformation(
					Integer.parseInt(tColumnIndexInformationFields[0]),
					Byte.parseByte(tColumnIndexInformationFields[1]),
					tAccessTypeValues[Integer.parseInt(tColumnIndexInformationFields[2])],
					tChild);
		}

		return tChild;
	}

}
