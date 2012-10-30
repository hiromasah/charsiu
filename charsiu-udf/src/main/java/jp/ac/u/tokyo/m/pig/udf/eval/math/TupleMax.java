/*
 * Copyright 2012 Hiromasa Horiguchi ( The University of Tokyo )
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

package jp.ac.u.tokyo.m.pig.udf.eval.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * From Bag of the comparable value corresponding to the order of Bag and the Bag, a comparable value outputs Tuple becoming maximum in a mass in Bag. <br>
 * <br>
 * This implementation class is in charge of a numerical comparison. <br>
 * bytearray converts it into double and compares it in this class. <br>
 * <br>
 * Bag と、その Bag の順序に対応した比較可能値の Bag から、比較可能値が最大となる Tuple を Bag にまとめて出力します。 <br>
 * <br>
 * この実装クラスでは 数値 の比較を担当します。 <br>
 * bytearray は double に変換し、このクラスで比較します。 <br>
 * それ以外の比較可能値については getArgToFuncMapping() に記述するクラスで担当します。 <br>
 */
public class TupleMax extends EvalFunc<DataBag> {

	// -----------------------------------------------------------------------------------------------------------------

	public TupleMax() {
		super();
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public DataBag exec(Tuple aInput) throws IOException {
		// invalid value | 無効値
		if (aInput == null)
			return DefaultBagFactory.getInstance().newDefaultBag();

		// processing target | 処理対象
		DataBag tTargetBag = DataType.toBag(aInput.get(0));
		DataBag tComparableValueBag = DataType.toBag(aInput.get(1));
		if (tTargetBag.size() == 0 || tComparableValueBag.size() == 0)
			return DefaultBagFactory.getInstance().newDefaultBag();

		Iterator<Tuple> tTargetBagIterator = tTargetBag.iterator();
		Iterator<Tuple> tComparableValueBagIterator = tComparableValueBag.iterator();

		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();

		Double tMaxValue = Double.NEGATIVE_INFINITY;

		while (tComparableValueBagIterator.hasNext()) {
			Tuple tCurrentTargetTuple = tTargetBagIterator.next();
			Double tCurrentValue = DataType.toDouble(tComparableValueBagIterator.next().get(0));
			if (tCurrentValue == null)
				continue;
			// add a tuple to tProtoBag if the same as existing MaxValue
			// 現 MaxValue と同じなら、タプルを tProtoBag に追加
			if (tMaxValue.equals(tCurrentValue)) {
				tProtoBag.add(tCurrentTargetTuple);
			}
			// clear tProtoBag if bigger than existing MaxValue and add a tuple to tProtoBag
			// 現 MaxValue より大きいなら、tProtoBag をクリアし、タプルを tProtoBag に追加
			else if (tMaxValue < tCurrentValue) {
				tMaxValue = tCurrentValue;
				tProtoBag.clear();
				tProtoBag.add(tCurrentTargetTuple);
			}
		}

		return DefaultBagFactory.getInstance().newDefaultBag(tProtoBag);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		String tThisClassName = this.getClass().getName();
		addFuncSpec(tFuncList, DataType.DOUBLE, tThisClassName);
		addFuncSpec(tFuncList, DataType.FLOAT, tThisClassName);
		addFuncSpec(tFuncList, DataType.LONG, tThisClassName);
		addFuncSpec(tFuncList, DataType.INTEGER, tThisClassName);
		addFuncSpec(tFuncList, DataType.BYTEARRAY, tThisClassName);
		addFuncSpec(tFuncList, DataType.CHARARRAY, TupleStringMax.class.getName());
		return tFuncList;
	}

	private void addFuncSpec(List<FuncSpec> aFuncList, byte aDataType, String aClassName) throws FrontendException {
		Schema tSchema = new Schema();
		tSchema.add(new Schema.FieldSchema(null, DataType.BAG));
		tSchema.add(new Schema.FieldSchema(null, new Schema(new Schema.FieldSchema(null, aDataType)), DataType.BAG));
		aFuncList.add(new FuncSpec(aClassName, tSchema));
	}

	// -----------------------------------------------------------------------------------------------------------------

	// Bag{ InputTuple }
	@Override
	public Schema outputSchema(Schema aInput) {
		List<FieldSchema> tInputFields = aInput.getFields();
		FieldSchema tInputTarget = tInputFields.get(0);

		Schema tBagSchema = new Schema();
		try {
			Schema tInputTargetSchema = tInputTarget.schema.getFields().get(0).schema;
			tBagSchema.add(new FieldSchema(AliasConstants.TUPLE_MAX_OUT_ALIAS_TOP, tInputTargetSchema, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}

		return tBagSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
