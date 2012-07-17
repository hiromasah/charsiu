package jp.ac.u.tokyo.m.pig.udf.eval.join;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.eval.join.ValueJoin;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class ValueJoinTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String GROUP_FILTER_FORMAT =
			"4legs(dog[30, jp], cat[55, us], caw[13, cl]), etc(caw[22, jp], fish[99, jp], bug[99, jp])";
	private static final String ADDITIONAL_PARAMETER_DEFINITION_FORMAT =
			"num : int, location : chararray";

	private static ValueJoin mValueJoin;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() {
		mValueJoin = new ValueJoin(GROUP_FILTER_FORMAT, ADDITIONAL_PARAMETER_DEFINITION_FORMAT);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExecOutput() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(3,
						"4legs", 30, "jp"
						),
				mValueJoin.exec(TestUtil.createTuple("dog")));
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(3,
						"4legs", 55, "us"
						),
				mValueJoin.exec(TestUtil.createTuple("cat")));
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(3,
						"4legs", 13, "cl",
						"etc", 22, "jp"
						),
				mValueJoin.exec(TestUtil.createTuple("caw")));
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(3,
						"etc", 99, "jp"
						),
				mValueJoin.exec(TestUtil.createTuple("fish")));
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(3,
						"etc", 99, "jp"
						),
				mValueJoin.exec(TestUtil.createTuple("bug")));

		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(3,
						"4legs", 13, "cl",
						"etc", 22, "jp"
						),
				mValueJoin.exec(TestUtil.createTuple("cawboy")));

		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(0),
				mValueJoin.exec(TestUtil.createTuple("horse")));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchema() throws Throwable {
		Schema tTupleSchema = TestUtil.createSchema(
				new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME, DataType.CHARARRAY),
				new FieldSchema("num", DataType.INTEGER),
				new FieldSchema("location", DataType.CHARARRAY)
				);
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS, tTupleSchema, DataType.BAG)),
				mValueJoin.outputSchema(null/* unused input schema */));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
