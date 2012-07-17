package jp.ac.u.tokyo.m.pig.udf.eval.group;

import jp.ac.u.tokyo.m.pig.udf.AliasConstants;
import jp.ac.u.tokyo.m.pig.udf.eval.group.InnerGroup;
import jp.ac.u.tokyo.m.test.TestUtil;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;
import org.junit.BeforeClass;
import org.junit.Test;

public class InnerGroupTest {

	// -----------------------------------------------------------------------------------------------------------------

	private static final String GROUP_FILTER_FORMAT =
			"4legs(dog[30, jp], cat[55, us], caw[13, cl]), etc(caw[22, jp], fish[99, jp], bug[99, jp])";

	private static DataBag INPUT_BAG = TestUtil.createBag(2,
			"dog", TestUtil.createBag(3,
					"4legs", 30, "jp"),
			"dog", TestUtil.createBag(3,
					"4legs", 30, "jp"),
			"cat", TestUtil.createBag(3,
					"4legs", 55, "us"),

			"fish", TestUtil.createBag(3,
					"etc", 99, "jp"),
			"bug", TestUtil.createBag(3,
					"etc", 99, "jp"),

			"caw", TestUtil.createBag(3,
					"4legs", 13, "cl",
					"etc", 22, "jp"),
			"caw", TestUtil.createBag(3,
					"4legs", 13, "cl",
					"etc", 22, "jp")
			);
	private static DataBag VALUES_BAG;

	private static Schema INPUT_SCHEMA;

	private static InnerGroup mInnerGroupModeOwnGroup;
	private static InnerGroup mInnerGroupModeAllGroup;

	// -----------------------------------------------------------------------------------------------------------------

	@BeforeClass
	public static void init() throws FrontendException, ExecException {
		// {input_bag: {bag_tuple: (kind: chararray,joined_values: {joined_bag_tuple: (group_name: chararray,num: int,location: chararray)})}}
		INPUT_SCHEMA = TestUtil.createSchema(
				new FieldSchema("input_bag",
						TestUtil.createSchema(
								new FieldSchema("bag_tuple",
										TestUtil.createSchema(
												new FieldSchema("kind", DataType.CHARARRAY),
												new FieldSchema(
														AliasConstants.VALUE_JOIN_OUT_ALIAS,
														TestUtil.createSchema(
																new FieldSchema("joined_bag_tuple",
																		TestUtil.createSchema(
																				new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME, DataType.CHARARRAY),
																				new FieldSchema("num", DataType.INTEGER),
																				new FieldSchema("location", DataType.CHARARRAY)
																				),
																		DataType.TUPLE)),
														DataType.BAG)
												),
										DataType.TUPLE)),
						DataType.BAG));

		VALUES_BAG = TestUtil.createValuesBag(INPUT_BAG, 0);

		// インスタンス化時に outputSchema で得たスキーマ情報を利用する
		InnerGroup tInnerGroupModeOwnGroup = new InnerGroup(GROUP_FILTER_FORMAT, "own_group");
		tInnerGroupModeOwnGroup.outputSchema(INPUT_SCHEMA);
		mInnerGroupModeOwnGroup = new InnerGroup(GROUP_FILTER_FORMAT, "own_group");

		tInnerGroupModeOwnGroup = new InnerGroup(GROUP_FILTER_FORMAT, "all_group");
		tInnerGroupModeOwnGroup.outputSchema(INPUT_SCHEMA);
		mInnerGroupModeAllGroup = new InnerGroup(GROUP_FILTER_FORMAT, "all_group");
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testExecOutputOwnGroup() throws Throwable {
		Tuple tExpected = TestUtil.createTuple(
				"4legs",
				TestUtil.createBag(3,
						"dog", 30, "jp",
						"dog", 30, "jp",
						"cat", 55, "us",
						"caw", 13, "cl",
						"caw", 13, "cl"),
				"etc",
				TestUtil.createBag(3,
						"fish", 99, "jp",
						"bug", 99, "jp",
						"caw", 22, "jp",
						"caw", 22, "jp")
				);
		TestUtil.assertEqualsPigObjects(tExpected,
				mInnerGroupModeOwnGroup.exec(TestUtil.createTuple(INPUT_BAG, VALUES_BAG)));
	}

	@Test
	public void testExecOutputAllGroup() throws Throwable {
		Tuple tExpected = TestUtil.createTuple(
				"4legs",
				TestUtil.createBag(2,
						"dog", TestUtil.createBag(3,
								"4legs", 30, "jp"),
						"dog", TestUtil.createBag(3,
								"4legs", 30, "jp"),
						"cat", TestUtil.createBag(3,
								"4legs", 55, "us"),
						"caw", TestUtil.createBag(3,
								"4legs", 13, "cl",
								"etc", 22, "jp"),
						"caw", TestUtil.createBag(3,
								"4legs", 13, "cl",
								"etc", 22, "jp")),
				"etc",
				TestUtil.createBag(2,
						"fish", TestUtil.createBag(3,
								"etc", 99, "jp"),
						"bug", TestUtil.createBag(3,
								"etc", 99, "jp"),

						"caw", TestUtil.createBag(3,
								"4legs", 13, "cl",
								"etc", 22, "jp"),
						"caw", TestUtil.createBag(3,
								"4legs", 13, "cl",
								"etc", 22, "jp"))
				);
		TestUtil.assertEqualsPigObjects(tExpected,
				mInnerGroupModeAllGroup.exec(TestUtil.createTuple(INPUT_BAG, VALUES_BAG)));
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test(expected = IllegalArgumentException.class)
	public void testExecException() throws Throwable {
		new InnerGroup(GROUP_FILTER_FORMAT, "unexists_mode");
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Test
	public void testSchemaOwnGroup() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_NAME_PREFIX + "4legs", DataType.CHARARRAY),
										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_BAG_PREFIX + "4legs",
												TestUtil.createSchema(
														new FieldSchema("kind", DataType.CHARARRAY),
														new FieldSchema("num", DataType.INTEGER),
														new FieldSchema("location", DataType.CHARARRAY)
														),
												DataType.BAG),

										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_NAME_PREFIX + "etc", DataType.CHARARRAY),
										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_BAG_PREFIX + "etc",
												TestUtil.createSchema(
														new FieldSchema("kind", DataType.CHARARRAY),
														new FieldSchema("num", DataType.INTEGER),
														new FieldSchema("location", DataType.CHARARRAY)
														),
												DataType.BAG)
										),
								DataType.TUPLE)),
				mInnerGroupModeOwnGroup.outputSchema(INPUT_SCHEMA));
	}

	@Test
	public void testSchemaAllGroup() throws Throwable {
		TestUtil.assertEqualsPigObjects(
				TestUtil.createSchema(
						new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_TOP,
								TestUtil.createSchema(
										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_NAME_PREFIX + "4legs", DataType.CHARARRAY),
										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_BAG_PREFIX + "4legs",
												TestUtil.createSchema(
														new FieldSchema("bag_tuple",
																TestUtil.createSchema(
																		new FieldSchema("kind", DataType.CHARARRAY),
																		new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS,
																				TestUtil.createSchema(
																						new FieldSchema("joined_bag_tuple",
																								TestUtil.createSchema(
																										new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME, DataType.CHARARRAY),
																										new FieldSchema("num", DataType.INTEGER),
																										new FieldSchema("location", DataType.CHARARRAY)
																										),
																								DataType.TUPLE)
																						),
																				DataType.BAG)
																		),
																DataType.TUPLE)
														),
												DataType.BAG),

										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_NAME_PREFIX + "etc", DataType.CHARARRAY),
										new FieldSchema(AliasConstants.INNER_GROUP_OUT_ALIAS_GROUP_BAG_PREFIX + "etc",
												TestUtil.createSchema(
														new FieldSchema("bag_tuple",
																TestUtil.createSchema(
																		new FieldSchema("kind", DataType.CHARARRAY),
																		new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS,
																				TestUtil.createSchema(
																						new FieldSchema("joined_bag_tuple",
																								TestUtil.createSchema(
																										new FieldSchema(AliasConstants.VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME, DataType.CHARARRAY),
																										new FieldSchema("num", DataType.INTEGER),
																										new FieldSchema("location", DataType.CHARARRAY)
																										),
																								DataType.TUPLE)
																						),
																				DataType.BAG)
																		),
																DataType.TUPLE)
														),
												DataType.BAG)
										),
								DataType.TUPLE)),
				mInnerGroupModeAllGroup.outputSchema(INPUT_SCHEMA));
	}

	// -----------------------------------------------------------------------------------------------------------------

}
