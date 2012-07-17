package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public class IntegerCaster extends AbstractNumberCaster {

	public static IntegerCaster INSTANCE = new IntegerCaster();

	private IntegerCaster() {};

	@Override
	public Object castNumber(String aDataString) {
		return Integer.parseInt(aDataString);
	}

}
