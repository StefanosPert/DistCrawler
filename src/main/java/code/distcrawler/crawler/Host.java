package code.distcrawler.crawler;

import java.util.Date;

public class Host {
	public Date date;
	public int visits;
	public Host(Date newDate,int num) {
		this.visits=num;
		this.date=newDate;
	}
}
