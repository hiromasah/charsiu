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

package jp.ac.u.tokyo.m.pig.udf;

import java.util.Properties;

import jp.ac.u.tokyo.m.resource.ResourceLoadUtil;

public class AliasConstants {

	// -----------------------------------------------------------------------------------------------------------------

	public static final String GET_DAY_SPAN_OUT_ALIAS;
	public static final String ADD_DAY_SPAN_OUT_ALIAS;

	public static final String GET_YEAR_SPAN_OUT_ALIAS;

	public static final String INNER_GROUP_OUT_ALIAS_TOP;
	public static final String INNER_GROUP_OUT_ALIAS_GROUP_NAME_PREFIX;
	public static final String INNER_GROUP_OUT_ALIAS_GROUP_BAG_PREFIX;

	public static final String VALUE_JOIN_OUT_ALIAS;
	public static final String VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME;

	public static final String SEQUENCE_VALUES_OUT_ALIAS_TOP;
	public static final String SEQUENCE_VALUES_OUT_ALIAS_INNER;

	public static final String FIRST_OUT_ALIAS_TOP;

	public static final String TUPLE_MAX_OUT_ALIAS_TOP;

	public static final String TUPLE_MIN_OUT_ALIAS_TOP;

	// -----------------------------------------------------------------------------------------------------------------

	static {
		Properties tProperties = ResourceLoadUtil.loadUnnecessaryPackagePrivateProperties(
				new AliasConstants().getClass(),
				"alias.ini");

		GET_DAY_SPAN_OUT_ALIAS = tProperties.getProperty("GetDaySpan.OutAlias", "day_span");
		ADD_DAY_SPAN_OUT_ALIAS = tProperties.getProperty("AddDaySpan.OutAlias", "added_day_span");
		GET_YEAR_SPAN_OUT_ALIAS = tProperties.getProperty("GetYearSpan.OutAlias", "year_span");

		INNER_GROUP_OUT_ALIAS_TOP = tProperties.getProperty("InnerGroup.OutAlias.Top", "innergroup");
		INNER_GROUP_OUT_ALIAS_GROUP_NAME_PREFIX = tProperties.getProperty("InnerGroup.OutAlias.GroupNamePrefix", "group_name_");
		INNER_GROUP_OUT_ALIAS_GROUP_BAG_PREFIX = tProperties.getProperty("InnerGroup.OutAlias.GroupBagPrefix", "group_bag_");

		VALUE_JOIN_OUT_ALIAS = tProperties.getProperty("ValueJoin.OutAlias", "joined_values");
		VALUE_JOIN_OUT_ALIAS_INNER_GROUP_NAME = tProperties.getProperty("ValueJoin.OutAlias.Inner.GroupName", "group_name");

		SEQUENCE_VALUES_OUT_ALIAS_TOP = tProperties.getProperty("PickupSequenceValues.OutAlias.Top", "sequence_values");
		SEQUENCE_VALUES_OUT_ALIAS_INNER = tProperties.getProperty("PickupSequenceValues.OutAlias.Inner", "sequence_member_bag");

		FIRST_OUT_ALIAS_TOP = tProperties.getProperty("First.OutAlias.Top", "first_tuple");

		TUPLE_MAX_OUT_ALIAS_TOP = tProperties.getProperty("TupleMax.OutAlias.Top", "max_tuples");

		TUPLE_MIN_OUT_ALIAS_TOP = tProperties.getProperty("TupleMin.OutAlias.Top", "min_tuples");
	}

	// -----------------------------------------------------------------------------------------------------------------

}
