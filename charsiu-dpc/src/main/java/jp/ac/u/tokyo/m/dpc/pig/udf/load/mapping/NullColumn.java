package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public class NullColumn implements ResultColumn {

	public static NullColumn INSTANCE = new NullColumn();

	private NullColumn() {};

	@Override
	public Object getValue(String[] aValues) {
		return null;
	}

}
