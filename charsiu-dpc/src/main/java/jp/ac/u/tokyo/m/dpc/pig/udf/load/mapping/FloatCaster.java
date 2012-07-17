package jp.ac.u.tokyo.m.dpc.pig.udf.load.mapping;

public class FloatCaster extends AbstractNumberCaster {

	public static FloatCaster INSTANCE = new FloatCaster();

	private FloatCaster() {};

	@Override
	public Object castNumber(String aDataString) {
		return Float.parseFloat(aDataString);
	}

}
