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
 * Bag 中で最初に検出された null でないタプルを出力します。
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

		// 処理対象
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
