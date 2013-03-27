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
import java.util.Iterator;
import java.util.List;

import jp.ac.u.tokyo.m.pig.udf.eval.util.MulticastEvaluationConstants.AccessType;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;

public class StringableColumnAccessor implements ColumnAccessor {

	private final List<ColumnIndexInformation> mColumnIndexInformations;

	public StringableColumnAccessor(List<ColumnIndexInformation> aColumnIndexInformations) {
		mColumnIndexInformations = aColumnIndexInformations;
	}

	@Override
	public Tuple generate(Object aColumnValue) {
		ArrayList<Object> tTuple = new ArrayList<Object>();
		for (ColumnIndexInformation tColumnName : mColumnIndexInformations) {
			if (tColumnName.hasChild()) {
				// TODO 例外処理
				// TODO 実装（ hasChild 時の多階層解析）
				try {
					generateChildValue(tTuple, tColumnName, aColumnValue);
				} catch (ExecException e) {
					throw new RuntimeException(e);
				}
			} else {
				// use aColumnValue
				tTuple.add(aColumnValue);
			}
		}
		return TupleFactory.getInstance().newTupleNoCopy(tTuple);
	}

	private void generateChildValue(ArrayList<Object> aResultTuple, ColumnIndexInformation aColumnName, Object aColumnValue) throws ExecException {
		DataBag tDataSourceBag = null;

		Object tDataSource = aColumnValue;
		ColumnIndexInformation tCurrentColumnName = aColumnName;
		while (tCurrentColumnName.hasChild()) {
			ColumnIndexInformation tNextColumnName = tCurrentColumnName.getChild();
			if (tNextColumnName.getAccessType() == AccessType.SUB_BAG) {
				// subbag 対象の bag を特定
				tDataSourceBag = DataType.toBag(tDataSource);
				break;
			} else {
				// TODO 実装（プロトタイプ版では１層目が Bag の物しか扱わない、ということにする）
				switch (tCurrentColumnName.getFieldSchema().type) {
				case DataType.BAG:
					// tDataSource = DataType.toBag(aColumnValue);
					break;
				case DataType.TUPLE:
					break;
				default:
					// aResultTuple.add(e);
					return;
				}
			}
		}

		// 次層が Tuple なら無視してその次の index
		ColumnIndexInformation tNextColumnName = tCurrentColumnName.getChild();
		int tChildIndex = tNextColumnName.getFieldSchema().type == DataType.TUPLE ? tNextColumnName.getChild().getIndex() : tNextColumnName.getIndex();
		Iterator<Tuple> tDataSourceBagIterator = tDataSourceBag.iterator();
		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();

		while (tDataSourceBagIterator.hasNext()) {
			Tuple tCurrentTuple = tDataSourceBagIterator.next();
			tProtoBag.add(createTuple(tCurrentTuple.get(tChildIndex)));
		}

		aResultTuple.add(DefaultBagFactory.getInstance().newDefaultBag(tProtoBag));

	}

	private Tuple createTuple(Object aValue) {
		ArrayList<Object> tTuple = new ArrayList<Object>();
		tTuple.add(aValue);
		return TupleFactory.getInstance().newTupleNoCopy(tTuple);
	}

	@Deprecated
	@Override
	public Schema getInputSchema() {
		return null;
	}

	@Override
	public List<ColumnIndexInformation> getColumnIndexInformations() {
		return mColumnIndexInformations;
	}

	@Override
	public String toString() {
		return toString(this);
	}

	public static String toString(ColumnAccessor aTarget) {
		String tDelimiter = MulticastEvaluationConstants.STRINGABLE_COLUMN_ACCESSOR_DELIMITER;
		StringBuilder tResultBuilder = new StringBuilder();
		Iterator<ColumnIndexInformation> tTargetIterator = aTarget.getColumnIndexInformations().iterator();
		if (tTargetIterator.hasNext())
			tResultBuilder.append(StringableColumnIndexInformation.toString(tTargetIterator.next()));
		while (tTargetIterator.hasNext()) {
			tResultBuilder.append(tDelimiter);
			tResultBuilder.append(StringableColumnIndexInformation.toString(tTargetIterator.next()));
		}
		return tResultBuilder.toString();
	}

	public static StringableColumnAccessor parse(String aColumnColumnAccessorString) {
		String[] tElements = aColumnColumnAccessorString.split(MulticastEvaluationConstants.STRINGABLE_COLUMN_ACCESSOR_DELIMITER);
		ArrayList<ColumnIndexInformation> tResultMember = new ArrayList<ColumnIndexInformation>();
		for (String tCurrentElement : tElements) {
			tResultMember.add(StringableColumnIndexInformation.parse(tCurrentElement));
		}
		return new StringableColumnAccessor(tResultMember);
	}

}
