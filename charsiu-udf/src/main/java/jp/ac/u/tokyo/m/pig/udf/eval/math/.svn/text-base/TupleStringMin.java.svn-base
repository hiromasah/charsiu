package jp.ac.u.tokyo.m.pig.udf.eval.math;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

/**
 * Bag と、その Bag の順序に対応した比較可能値の Bag から、比較可能値が最小となる Tuple を Bag にまとめて出力します。
 * 
 * この実装クラスでは 文字列 の比較を担当します。
 */
public class TupleStringMin extends EvalFunc<DataBag> {

	// -----------------------------------------------------------------------------------------------------------------

	public TupleStringMin() {
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

		String tMinValue = null;
		while (tTargetBagIterator.hasNext() && tComparableValueBagIterator.hasNext()) {
			Tuple tCurrentTargetTuple = tTargetBagIterator.next();
			String tCurrentValue = DataType.toString(tComparableValueBagIterator.next().get(0));
			if (tCurrentValue == null)
				continue;
			tMinValue = tCurrentValue;
			tProtoBag.add(tCurrentTargetTuple);
			break;
		}

		while (tComparableValueBagIterator.hasNext()) {
			Tuple tCurrentTargetTuple = tTargetBagIterator.next();
			String tCurrentValue = DataType.toString(tComparableValueBagIterator.next().get(0));
			if (tCurrentValue == null)
				continue;
			int tCompareResult = tCurrentValue.compareTo(tMinValue);
			// 現 MaxValue と同じなら、タプルを tProtoBag に追加
			if (tCompareResult == 0) {
				tProtoBag.add(tCurrentTargetTuple);
			}
			// 現 MaxValue より小さいなら、tProtoBag をクリアし、タプルを tProtoBag に追加
			else if (tCompareResult < 0) {
				tMinValue = tCurrentValue;
				tProtoBag.clear();
				tProtoBag.add(tCurrentTargetTuple);
			}
		}

		return DefaultBagFactory.getInstance().newDefaultBag(tProtoBag);
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
			tBagSchema.add(new FieldSchema(AliasConstants.TUPLE_MIN_OUT_ALIAS_TOP, tInputTargetSchema, DataType.BAG));
		} catch (FrontendException e) {
			throw new RuntimeException(e);
		}

		return tBagSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
