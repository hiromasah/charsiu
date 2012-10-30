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
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * TupleFirst outputs the tuple that is not detected null first in Bag. <br>
 * 
 * Bag 中で最初に検出された null でないタプルを出力します。 <br>
 */
public class TupleFirst extends EvalFunc<Tuple> {

	// -----------------------------------------------------------------------------------------------------------------

	public TupleFirst() {
		super();
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public Tuple exec(Tuple aInput) throws IOException {
		if (aInput == null)
			return null;

		// processing target | 処理対象
		DataBag tTargetBag = DataType.toBag(aInput.get(0));

		for (Iterator<Tuple> tTargetBagIterater = tTargetBag.iterator(); tTargetBagIterater.hasNext();) {
			Tuple tCurrentTuple = tTargetBagIterater.next();
			if (tCurrentTuple != null)
				return tCurrentTuple;
			else
				continue;
		}
		return null;
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> tFuncList = new ArrayList<FuncSpec>();
		Schema tSchema = new Schema();
		tSchema.add(new Schema.FieldSchema(null, DataType.BAG));
		tFuncList.add(new FuncSpec(this.getClass().getName(), tSchema));
		return tFuncList;
	}

	// -----------------------------------------------------------------------------------------------------------------

	// InputTuple
	@Override
	public Schema outputSchema(Schema aInput) {
		List<FieldSchema> tInputFields = aInput.getFields();
		FieldSchema tInputTarget = tInputFields.get(0);

		Schema tTupleSchema = new Schema();
		try {
			Schema tInputTargetSchema = tInputTarget.schema.getFields().get(0).schema;
			tTupleSchema.add(new FieldSchema(AliasConstants.FIRST_OUT_ALIAS_TOP, tInputTargetSchema, DataType.TUPLE));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}

		return tTupleSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
