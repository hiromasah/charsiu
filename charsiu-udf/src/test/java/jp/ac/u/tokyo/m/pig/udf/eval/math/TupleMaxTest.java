package jp.ac.u.tokyo.m.pig.udf.eval.math;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.eval.math.TupleMax;
import jp.ac.u.tokyo.m.pig.udf.eval.math.TupleStringMax;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class TupleMaxTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static TupleMax mTupleMax;
	private static TupleStringMax mTupleStringMax;

	private static DataBag INPUT_BAG = TestUtil.createBag(2,
			"dog", 1,
			"dog", 1,
			"cat", 2,
			"fish", 3,
			"bug", 4,
			"caw", 5,
			"caw", 5
			);
	private static DataBag VALUES_BAG;
	private static DataBag VALUES_BAG_STRING;

	private static Schema INPUT_SCHEMA;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() throws FrontendException, ExecException {
		mTupleMax = new TupleMax();
		mTupleStringMax = new TupleStringMax();

		VALUES_BAG = TestUtil.createValuesBag(INPUT_BAG, 1);
		VALUES_BAG_STRING = TestUtil.createValuesBag(INPUT_BAG, 0);

		INPUT_SCHEMA = TestUtil.createSchema(
				new FieldSchema("input_bag",
						TestUtil.createSchema(
								new FieldSchema("bag_tuple",
										TestUtil.createSchema(
												new FieldSchema("kind", DataType.CHARARRAY),
												new FieldSchema("num", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG)
				);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExecOutput() throws Throwable {
		TestUtil.assertEqualsPigObjects(TestUtil.createBag(2, "caw", 5, "caw", 5),
				mTupleMax.exec(TestUtil.createTuple(INPUT_BAG, VALUES_BAG)));
	}

	@Test
	public void testExecOutputString() throws Throwable {
		TestUtil.assertEqualsPigObjects(TestUtil.createBag(2, "fish", 3),
				mTupleStringMax.exec(TestUtil.createTuple(INPUT_BAG, VALUES_BAG_STRING)));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchema() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.TUPLE_MAX_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema("kind", DataType.CHARARRAY),
										new FieldSchema("num", DataType.INTEGER)
										),
								DataType.BAG)),
				mTupleMax.outputSchema(INPUT_SCHEMA));
	}

	@Test
	public void testSchemaString() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.TUPLE_MAX_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema("kind", DataType.CHARARRAY),
										new FieldSchema("num", DataType.INTEGER)
										),
								DataType.BAG)),
				mTupleStringMax.outputSchema(INPUT_SCHEMA));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
