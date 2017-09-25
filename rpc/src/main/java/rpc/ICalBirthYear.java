package rpc;

public interface ICalBirthYear {
	/**
	 * 根据姓名和年龄 计算出出生年份
	 * @param name
	 * @param age
	 * @return
	 */
	public static final long versionID=1L;
	String calBirthYear(String name,int age);
}
