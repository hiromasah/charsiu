package jp.ac.u.tokyo.m.pig.udf.eval.math;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.eval.math.TupleFirst;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class TupleFirstTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static TupleFirst mTupleFirst;

	private static DataBag INPUT_BAG = TestUtil.createBag(2,
			"dog", 1,
			"cat", 2,
			"fish", 3,
			"bug", 4,
			"caw", 5
			);

	private static Schema INPUT_SCHEMA;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() throws FrontendException {
		mTupleFirst = new TupleFirst();
		
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
		TestUtil.assertEqualsPigObjects(TestUtil.createTuple("dog", 1),
				mTupleFirst.exec(TestUtil.createTuple(INPUT_BAG)));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchema() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.FIRST_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema("kind", DataType.CHARARRAY),
										new FieldSchema("num", DataType.INTEGER)
										),
								DataType.TUPLE)),
				mTupleFirst.outputSchema(INPUT_SCHEMA));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
