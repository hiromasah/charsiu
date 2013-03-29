/*
 * Copyright 2012-2013 Hiromasa Horiguchi ( The University of Tokyo )
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jp.ac.u.tokyo.m.pig.udf.eval.util;

import java.util.ArrayList;
import java.util.List;

import jp.ac.u.tokyo.m.pig.udf.eval.util.MulticastEvaluationConstants.AccessType;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class ReflectionUDFParameters {

	// -----------------------------------------------------------------------------------------------------------------

	private final List<ColumnIndexInformation> mColumnIndexs;
	private final Schema mInputSchema;

	// -----------------------------------------------------------------------------------------------------------------

	public ReflectionUDFParameters(String aReflectionUDFParametersString, FieldSchema aColumnValueFieldSchema) throws FrontendException {
		mColumnIndexs = parseReflectionUDFParameters(aReflectionUDFParametersString, aColumnValueFieldSchema);
		mInputSchema = generateInputSchema(mColumnIndexs);
	}

	static List<ColumnIndexInformation> parseReflectionUDFParameters(String aReflectionUDFParametersString, FieldSchema aColumnValueFieldSchema) throws FrontendException {
		List<ColumnIndexInformation> tColumnIndexs = new ArrayList<ColumnIndexInformation>();

		String[] tParameters = aReflectionUDFParametersString.split(MulticastEvaluationConstants.REFLECTION_UDF_PARAMETERS_SEPARATOR);
		for (String tParameterString : tParameters) {
			// 最上位要素を追加
			DefaultColumnIndexInformation tCurrentColumnIndex = new DefaultColumnIndexInformation(0, aColumnValueFieldSchema, AccessType.FLAT);
			tColumnIndexs.add(tCurrentColumnIndex);
			FieldSchema tCurrentField = aColumnValueFieldSchema;
			String[] tAddresses = tParameterString.split(MulticastEvaluationConstants.REFLECTION_UDF_PARAMETERS_ACCESSOR);
			int tAddressesLength = tAddresses.length;
			for (int tIndex = 1; tIndex < tAddressesLength; tIndex++) {
				// find a child
				final String tAddressAlias = tAddresses[tIndex].trim();
				if (MulticastEvaluationConstants.REFLECTION_UDF_PARAMETERS_DOLLAR_INDEX_PATTERN.matcher(tAddressAlias).matches()) {
					// "//$[0-9]+" pattern
					int tPosition = Integer.parseInt(tAddressAlias.substring(1));
					if (tCurrentField.type == DataType.BAG) {
						if (tIndex != tAddressesLength - 1)
							throw new IllegalArgumentException("can't access inner element in bag's child. : " + aReflectionUDFParametersString);
						FieldSchema tBagTupleFieldSchema = tCurrentField.schema.getField(0);
						if (tCurrentField.schema.size() == 1 && tBagTupleFieldSchema.type == DataType.TUPLE) {
							// bag.tuple.getFiled($x)
							try {
								FieldSchema tField = tCurrentField.schema.getField(0).schema.getField(tPosition);
								// BagTuple Scheam
								DefaultColumnIndexInformation tChildColumnIndex = new DefaultColumnIndexInformation(0, tBagTupleFieldSchema, AccessType.SUB_BAG);
								tCurrentColumnIndex.setChild(tChildColumnIndex);
								tCurrentColumnIndex = tChildColumnIndex;
								// Column Schema
								DefaultColumnIndexInformation tChildChildColumnIndex = new DefaultColumnIndexInformation(tPosition, tField, AccessType.SUB_BAG);
								tChildColumnIndex.setChild(tChildChildColumnIndex);
								tChildColumnIndex = tChildChildColumnIndex;

								tCurrentField = tField;
								break;
							} catch (Throwable e) {
								throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField, e);
							}
						} else {
							// bag.getFiled($x)
							try {
								FieldSchema tField = tCurrentField.schema.getField(tPosition);
								DefaultColumnIndexInformation tChildColumnIndex = new DefaultColumnIndexInformation(tPosition, tField, AccessType.SUB_BAG);
								tCurrentColumnIndex.setChild(tChildColumnIndex);
								tCurrentColumnIndex = tChildColumnIndex;
								tCurrentField = tField;
								break;
							} catch (Throwable e) {
								throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField, e);
							}
						}
					} else {
						// isn't bag
						try {
							FieldSchema tField = tCurrentField.schema.getField(tPosition);
							AccessType tAccessType = null;
							if (tCurrentColumnIndex.getFieldType() == DataType.TUPLE && tCurrentColumnIndex.getAccessType() == AccessType.SUB_BAG)
								tAccessType = AccessType.SUB_BAG;
							else
								tAccessType = AccessType.FLAT;
							DefaultColumnIndexInformation tChildColumnIndex = new DefaultColumnIndexInformation(tPosition, tField, tAccessType);
							tCurrentColumnIndex.setChild(tChildColumnIndex);
							tCurrentColumnIndex = tChildColumnIndex;
							tCurrentField = tField;
							continue;
						} catch (Throwable e) {
							throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField, e);
						}
					}
				} else {
					// alias pattern
					FieldSchema tField = tCurrentField.schema.getField(tAddressAlias);
					if (tField == null) {
						// Bag{Tuple( ... )} ではなく Bag{ ... } で指定された可能性が有るので1階層無視して tAddressAlias を探す
						if (tCurrentField.type == DataType.BAG) {
							FieldSchema tBagTupleFieldSchema = tCurrentField.schema.getField(0);
							try {
								tField = tBagTupleFieldSchema.schema.getField(tAddressAlias);
							} catch (Throwable e) {
								throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField, e);
							}
							if (tField != null) {
								// BagTuple Scheam
								DefaultColumnIndexInformation tChildColumnIndex = new DefaultColumnIndexInformation(0, tBagTupleFieldSchema, AccessType.SUB_BAG);
								tCurrentColumnIndex.setChild(tChildColumnIndex);
								tCurrentColumnIndex = tChildColumnIndex;
								// Column Schema
								DefaultColumnIndexInformation tChildChildColumnIndex = new DefaultColumnIndexInformation(tBagTupleFieldSchema.schema.getPosition(tAddressAlias), tField, AccessType.SUB_BAG);
								tChildColumnIndex.setChild(tChildChildColumnIndex);
								tChildColumnIndex = tChildChildColumnIndex;

								tCurrentField = tField;
								continue;
							} else
								throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField);
						} else
							throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField);
					} else {
						AccessType tAccessType = null;
						if (tCurrentColumnIndex.getFieldType() == DataType.BAG
								|| (tCurrentColumnIndex.getFieldType() == DataType.TUPLE && tCurrentColumnIndex.getAccessType() == AccessType.SUB_BAG))
							tAccessType = AccessType.SUB_BAG;
						else
							tAccessType = AccessType.FLAT;
						DefaultColumnIndexInformation tChildColumnIndex = new DefaultColumnIndexInformation(tCurrentField.schema.getPosition(tAddressAlias), tField, tAccessType);
						tCurrentColumnIndex.setChild(tChildColumnIndex);
						tCurrentColumnIndex = tChildColumnIndex;
						tCurrentField = tField;
						continue;
					}
				}
			}
		}

		return tColumnIndexs;
	}

	static Schema generateInputSchema(List<ColumnIndexInformation> aColumnIndexs) throws FrontendException {
		Schema tSchema = new Schema();
		for (ColumnIndexInformation tColumnIndex : aColumnIndexs) {
			while (tColumnIndex.hasChild()) {
				tColumnIndex = tColumnIndex.getChild();
			}
			if (tColumnIndex.getAccessType() == AccessType.FLAT)
				tSchema.add(tColumnIndex.getFieldSchema());
			else {
				tSchema.add(new FieldSchema(null, new Schema(new FieldSchema(null, tColumnIndex.getFieldType())), DataType.BAG));
			}
		}
		return tSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public Schema getInputSchema() {
		return mInputSchema;
	}

	public List<ColumnIndexInformation> getColumnIndexInformations() {
		return mColumnIndexs;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
