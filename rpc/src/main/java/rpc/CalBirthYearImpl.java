package rpc;

import java.util.Calendar;

public class CalBirthYearImpl implements ICalBirthYear {
	
	Calendar calendar = Calendar.getInstance();

	public String calBirthYear(String name, int age) {
		int year = calendar.get(Calendar.YEAR);
		year -=age;
		return "Hello," + name + ",Your birthyear is" + year;
	}

}
