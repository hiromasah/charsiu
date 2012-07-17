package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public class LongCaster extends AbstractNumberCaster {

	public static LongCaster INSTANCE = new LongCaster();

	private LongCaster() {};

	@Override
	public Object castNumber(String aDataString) {
		return Long.parseLong(aDataString);
	}

}
