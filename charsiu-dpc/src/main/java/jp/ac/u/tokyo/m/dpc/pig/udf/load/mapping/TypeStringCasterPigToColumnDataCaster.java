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
