package jp.ac.u.tokyo.m.pig.udf.eval.util;

import jp.ac.u.tokyo.m.pig.udf.eval.util.MulticastEvaluationConstants.AccessType;

import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public interface ColumnIndexInformation {
	int getIndex();

	FieldSchema getFieldSchema();

	byte getFieldType();

	AccessType getAccessType();

	ColumnIndexInformation getChild();

	boolean hasChild();
}
