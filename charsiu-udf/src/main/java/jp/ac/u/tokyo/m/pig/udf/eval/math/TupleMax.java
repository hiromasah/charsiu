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
 * Bag と、その Bag の順序に対応した比較可能値の Bag から、比較可能値が最大となる Tuple を Bag にまとめて出力します。
 * 
 * この実装クラスでは 数値 の比較を担当します。
 * bytearray は double に変換し、このクラスで比較します。
 * それ以外の比較可能値については getArgToFuncMapping() に記述するクラスで担当します。
 */
public class TupleMax extends EvalFunc<DataBag> {

	// -----------------------------------------------------------------------------------------------------------------

	public TupleMax() {
		super();
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public DataBag exec(Tuple aInput) throws IOException {
		// 無効値
		if (aInput == null)
			return DefaultBagFactory.getInstance().newDefaultBag();

		// 処理対象
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
			// 現 MaxValue と同じなら、タプルを tProtoBag に追加
			if (tMaxValue.equals(tCurrentValue)) {
				tProtoBag.add(tCurrentTargetTuple);
			}
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
