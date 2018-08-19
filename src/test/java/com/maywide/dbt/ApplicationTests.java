package com.maywide.dbt;

import com.maywide.dbt.core.execute.TableTransport;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ApplicationTests {

	/*@Autowired
	private JobServices jobServices;
	@Test
	public void contextLoads() {
		jobServices.excute();
		try {
			Thread.sleep(10000*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
*/
	@Autowired
    private TableTransport tableTransport;

    @Test
    public void contextLoads() {
        //tableTransport.execute();
    }
}
