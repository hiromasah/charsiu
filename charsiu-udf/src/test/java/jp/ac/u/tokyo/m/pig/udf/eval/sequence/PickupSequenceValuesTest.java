package jp.ac.u.tokyo.m.pig.udf.eval.sequence;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.eval.sequence.PickupSequenceValues;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class PickupSequenceValuesTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static PickupSequenceValues mPickupSequenceValuesAllValuePacketBag;
	private static PickupSequenceValues mPickupSequenceValuesAllValueFlat;
	private static PickupSequenceValues mPickupSequenceValuesFirstValueFlat;

	private static final DataBag INPUT_BAG = TestUtil.createBag(2,
			"caw", 1,
			"caw", 2,
			"caw", 2,
			"caw", 4,
			"caw", 4,
			"caw", null,
			"caw", null
			);
	private static DataBag VALUES_BAG;

	private static Schema INPUT_SCHEMA;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() throws FrontendException, ExecException {
		mPickupSequenceValuesAllValuePacketBag = new PickupSequenceValues("all_value_packet_bag");
		mPickupSequenceValuesAllValueFlat = new PickupSequenceValues("all_value_flat");
		mPickupSequenceValuesFirstValueFlat = new PickupSequenceValues("first_value_flat");

		VALUES_BAG = TestUtil.createValuesBag(INPUT_BAG, 1);

		INPUT_SCHEMA = TestUtil.createSchema(
				new FieldSchema("input_bag",
						TestUtil.createSchema(
								new FieldSchema("bag_tuple",
										TestUtil.createSchema(
												new FieldSchema("kind", DataType.CHARARRAY),
												new FieldSchema("age", DataType.INTEGER)
												),
										DataType.TUPLE)),
						DataType.BAG)
				);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExecOutputAllValuePacketBag() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(1, TestUtil.createBag(2, "caw", 1), TestUtil.createBag(2, "caw", 2, "caw", 2)),
				mPickupSequenceValuesAllValuePacketBag.exec(TestUtil.createTuple(INPUT_BAG, VALUES_BAG, 1)));
	}

	@Test
	public void testExecOutputAllValueFlat() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(2, "caw", 1, "caw", 2, "caw", 2),
				mPickupSequenceValuesAllValueFlat.exec(TestUtil.createTuple(INPUT_BAG, VALUES_BAG, 1)));
	}

	@Test
	public void testExecOutputFirstValueFlat() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createBag(2, "caw", 1, "caw", 2),
				mPickupSequenceValuesFirstValueFlat.exec(TestUtil.createTuple(INPUT_BAG, VALUES_BAG, 1)));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test(expected = IllegalArgumentException.class)
	public void testExecException() throws Throwable {
		new PickupSequenceValues("unexists_mode");
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchemaAllValuePacketBag() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.SEQUENCE_VALUES_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema(AliasConstants.SEQUENCE_VALUES_OUT_ALIAS_INNER,
												TestUtil.createSchema(
														new FieldSchema("kind", DataType.CHARARRAY),
														new FieldSchema("age", DataType.INTEGER)
														),
												DataType.BAG)
										),
								DataType.BAG)),
				mPickupSequenceValuesAllValuePacketBag.outputSchema(INPUT_SCHEMA));
	}

	@Test
	public void testSchemaValuesAllValueFlat() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.SEQUENCE_VALUES_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema("kind", DataType.CHARARRAY),
										new FieldSchema("age", DataType.INTEGER)
										),
								DataType.BAG)),
				mPickupSequenceValuesAllValueFlat.outputSchema(INPUT_SCHEMA));
	}

	@Test
	public void testSchemaFirstValueFlat() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.SEQUENCE_VALUES_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema("kind", DataType.CHARARRAY),
										new FieldSchema("age", DataType.INTEGER)
										),
								DataType.BAG)),
				mPickupSequenceValuesFirstValueFlat.outputSchema(INPUT_SCHEMA));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
