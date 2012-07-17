package jp.ac.u.tokyo.m.test;

import java.util.ArrayList;

import junit.framework.Assert;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DefaultBagFactory;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

public class TestUtil {

	// -----------------------------------------------------------------------------------------------------------------

	public static void assertEqualsPigObjects(Object aExpected, Object aActual) {
		// 同じ構造のものを作ったのだから toString() 結果も同じになるはず
		Assert.assertEquals(
				aExpected.toString(),
				aActual.toString());
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @return Tuple : (aValues[0], ... )
	 */
	public static Tuple createTuple(Object... aValues) {
		ArrayList<Object> tTuple = new ArrayList<Object>();
		for (Object tValue : aValues) {
			tTuple.add(tValue);
		}
		return TupleFactory.getInstance().newTupleNoCopy(tTuple);
	}

	/**
	 * @param aColumnSize
	 *            Tuple のカラムサイズ
	 * @param aValues
	 *            aColumnSize ずつ1つの Tuple になる
	 * @return Bag : { ... , Tuple : (aValues[n * aColumnSize], ... , aValues[n * aColumnSize + aColumnSize - 1]), ... }
	 */
	public static DataBag createBag(int aColumnSize, Object... aValues) {
		TupleFactory tTupleFactory = TupleFactory.getInstance();
		BagFactory tBagFactory = DefaultBagFactory.getInstance();

		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();

		int tValuesLength = aValues.length;
		for (int tIndex = 0; tIndex < tValuesLength;) {
			ArrayList<Object> tTuple = new ArrayList<Object>();
			for (int tTupleIndex = 0; tTupleIndex < aColumnSize; tTupleIndex++) {
				tTuple.add(aValues[tIndex++]);
			}
			tProtoBag.add(tTupleFactory.newTupleNoCopy(tTuple));
		}

		return tBagFactory.newDefaultBag(tProtoBag);
	}
	
	/**
	 * @param aTargetBag 特定カラムを抽出する Bag
	 * @param aValueIndex 抽出するカラムのインデックス
	 * @return Bag : { Tuple : (aTargetBag[0].get(aValueIndex)), ... }
	 */
	public static DataBag createValuesBag(DataBag aTargetBag, int aValueIndex) throws ExecException{
		TupleFactory tTupleFactory = TupleFactory.getInstance();
		ArrayList<Tuple> tProtoBag = new ArrayList<Tuple>();
		for (Tuple tCurrentTuple : aTargetBag) {
			ArrayList<Object> tTuple = new ArrayList<Object>();
			tTuple.add(tCurrentTuple.get(aValueIndex));
			tProtoBag.add(tTupleFactory.newTupleNoCopy(tTuple));
		}
		return DefaultBagFactory.getInstance().newDefaultBag(tProtoBag);
	}

	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * @return Schema : (aFieldSchemas[0], ... )
	 */
	public static Schema createSchema(FieldSchema... aFieldSchemas) {
		Schema tSchema = new Schema();
		for (FieldSchema tFieldSchema : aFieldSchemas) {
			tSchema.add(tFieldSchema);
		}
		return tSchema;
	}

	// -----------------------------------------------------------------------------------------------------------------

}
