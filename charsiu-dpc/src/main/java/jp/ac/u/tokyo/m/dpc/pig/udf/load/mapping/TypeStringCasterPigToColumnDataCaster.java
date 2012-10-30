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

package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

import jp.ac.u.tokyo.m.data.type.AbstractPigTypeStringCaster;

public class TypeStringCasterPigToColumnDataCaster extends AbstractPigTypeStringCaster<ColumnDataCaster> {

	public static final TypeStringCasterPigToColumnDataCaster INSTANCE = new TypeStringCasterPigToColumnDataCaster();

	private TypeStringCasterPigToColumnDataCaster() {}

	@Override
	public ColumnDataCaster caseDouble() {
		return DoubleCaster.INSTANCE;
	}

	@Override
	public ColumnDataCaster caseFloat() {
		return FloatCaster.INSTANCE;
	}

	@Override
	public ColumnDataCaster caseInt() {
		return IntegerCaster.INSTANCE;
	}

	@Override
	public ColumnDataCaster caseLong() {
		return LongCaster.INSTANCE;
	}

	@Override
	public ColumnDataCaster caseString() {
		return ThroughCaster.INSTANCE;
	}

}
