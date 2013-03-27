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

	private final List<ColumnIndexInformation> mColumnNames;
	private final Schema mInputSchema;

	// -----------------------------------------------------------------------------------------------------------------

	public ReflectionUDFParameters(String aReflectionUDFParametersString, FieldSchema aColumnValueFieldSchema) throws FrontendException {
		mColumnNames = parseReflectionUDFParameters(aReflectionUDFParametersString, aColumnValueFieldSchema);
		mInputSchema = generateInputSchema(mColumnNames);
	}

	// TODO $x access to column
	static List<ColumnIndexInformation> parseReflectionUDFParameters(String aReflectionUDFParametersString, FieldSchema aColumnValueFieldSchema) throws FrontendException {
		List<ColumnIndexInformation> tColumnNames = new ArrayList<ColumnIndexInformation>();

		String[] tParameters = aReflectionUDFParametersString.split(MulticastEvaluationConstants.REFLECTION_UDF_PARAMETERS_SEPARATOR);
		for (String tParameterString : tParameters) {
			// 最上位要素を追加
			DefaultColumnIndexInformation tCurrentColumnName = new DefaultColumnIndexInformation(0, aColumnValueFieldSchema, AccessType.FLAT);
			tColumnNames.add(tCurrentColumnName);
			FieldSchema tCurrentField = aColumnValueFieldSchema;
			String[] tAddresses = tParameterString.split(MulticastEvaluationConstants.REFLECTION_UDF_PARAMETERS_ACCESSOR);
			int tAddressesLength = tAddresses.length;
			for (int tIndex = 1; tIndex < tAddressesLength; tIndex++) {
				String tAddressAlias = tAddresses[tIndex];
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
							// String tChildColumnAlias = tBagTupleFieldSchema.alias == null ? "" : tBagTupleFieldSchema.alias;
							DefaultColumnIndexInformation tChildColumnName = new DefaultColumnIndexInformation(0, tBagTupleFieldSchema, AccessType.SUB_BAG);
							tCurrentColumnName.setChild(tChildColumnName);
							tCurrentColumnName = tChildColumnName;
							// Column Schema
							DefaultColumnIndexInformation tChildChildColumnName = new DefaultColumnIndexInformation(tBagTupleFieldSchema.schema.getPosition(tAddressAlias), tField, AccessType.SUB_BAG);
							tChildColumnName.setChild(tChildChildColumnName);
							tChildColumnName = tChildChildColumnName;

							tCurrentField = tField;
							continue;
						} else
							throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField);
					} else
						throw new IllegalArgumentException(tAddressAlias + " is not found in the schema : " + tCurrentField);
				} else {
					AccessType tAccessType = null;
					if (tCurrentColumnName.getFieldSchema().type == DataType.BAG
							|| (tCurrentColumnName.getFieldSchema().type == DataType.TUPLE && tCurrentColumnName.getAccessType() == AccessType.SUB_BAG))
						tAccessType = AccessType.SUB_BAG;
					else
						tAccessType = AccessType.FLAT;
					DefaultColumnIndexInformation tChildColumnName = new DefaultColumnIndexInformation(tCurrentField.schema.getPosition(tAddressAlias), tField, tAccessType);
					tCurrentColumnName.setChild(tChildColumnName);
					tCurrentColumnName = tChildColumnName;
					tCurrentField = tField;
					continue;
				}
			}
		}

		return tColumnNames;
	}

	static Schema generateInputSchema(List<ColumnIndexInformation> aColumnNames) throws FrontendException {
		Schema tSchema = new Schema();
		for (ColumnIndexInformation tColumnName : aColumnNames) {
			while (tColumnName.hasChild()) {
				tColumnName = tColumnName.getChild();
			}
			if (tColumnName.getAccessType() == AccessType.FLAT)
				tSchema.add(tColumnName.getFieldSchema());
			else {
				tSchema.add(new FieldSchema(null, new Schema(new FieldSchema(null, tColumnName.getFieldSchema().type)), DataType.BAG));
			}
		}
		return tSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

	public Schema getInputSchema() {
		return mInputSchema;
	}

	public List<ColumnIndexInformation> getColumnIndexInformations() {
		return mColumnNames;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
