package com.hsbc.gbm.surveillance.sdf.trade.processor.service;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
public class MetaDataConvertorTest {
	@SpyBean
	private IMetaDataService metaDataServicee;

	@MockBean
	private MetaDataConvertor metaDataConvertorBean;

	@Test
	public void testMetaDataConvertorWithJSONDoNothing() {
		Mockito.doNothing().when(metaDataConvertorBean).startProcess();
	}

	@Test
	public void testMetaDataConvertorWithJSONDoThrow() {
		Mockito.doThrow(new RuntimeException()).when(metaDataConvertorBean).startProcess();
	}

	@Test
	public void testMetaDataConvertorWithYMADoNothingL() {
		Mockito.doNothing().when(metaDataConvertorBean).startYmlProcess();
	}

	@Test
	public void testMetaDataConvertorWithYMALDoThrow() {
		Mockito.doThrow(new RuntimeException()).when(metaDataConvertorBean).startYmlProcess();
	}
}
