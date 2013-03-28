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
		for (ColumnIndexInformation tColumnIndex : mColumnIndexInformations) {
			if (tColumnIndex.hasChild()) {
				try {
					generateChildValue(tTuple, tColumnIndex, aColumnValue);
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

	// TODO 実装（ hasChild 時の多階層解析）
	private void generateChildValue(ArrayList<Object> aResultTuple, ColumnIndexInformation aColumnIndex, Object aColumnValue) throws ExecException {
		DataBag tDataSourceBag = null;

		Object tDataSource = aColumnValue;
		ColumnIndexInformation tCurrentColumnIndex = aColumnIndex;
		while (tCurrentColumnIndex.hasChild()) {
			ColumnIndexInformation tNextColumnIndex = tCurrentColumnIndex.getChild();
			if (tNextColumnIndex.getAccessType() == AccessType.SUB_BAG) {
				// subbag 対象の bag を特定
				tDataSourceBag = DataType.toBag(tDataSource);
				break;
			} else {
				// TODO 実装（プロトタイプ版では１層目が Bag の物しか扱わない、ということにする）
				switch (tCurrentColumnIndex.getFieldType()) {
				case DataType.TUPLE:
					// tDataSource を child のものに更新、他も併せて更新
					// continue
					break;
				case DataType.INTEGER: // 他 unstructured data
					// 
					// return
					break;
				case DataType.BAG:
					// ありえない
					// subbag 以外で bag にアクセスするのはサポート外。 unsupported exception 投げる。
					// いや、flat タイプの bag アクセス？ bag そのまま返す。
					break;
				default:
					// aResultTuple.add(e);
					return;
				}
			}
		}

		// 次層が Tuple なら無視してその次の index
		ColumnIndexInformation tNextColumnIndex = tCurrentColumnIndex.getChild();
		int tChildIndex = tNextColumnIndex.getFieldType() == DataType.TUPLE ? tNextColumnIndex.getChild().getIndex() : tNextColumnIndex.getIndex();
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
