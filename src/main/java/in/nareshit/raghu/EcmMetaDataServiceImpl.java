package com.hsbc.gbm.surveillance.sdf.trade.processor.service;

import javax.transaction.Transactional;

import org.springframework.stereotype.Service;

import com.hsbc.gbm.surveillance.sdf.trade.processor.entity.MetaDataEntity;
import com.hsbc.gbm.surveillance.sdf.trade.processor.repository.EcmMetaDataRepository;

@Service 
@Transactional
public class EcmMetaDataServiceImpl  extends AbstractBaseServiceImpl<MetaDataEntity, String> implements EcmMetaDataService{

	public EcmMetaDataServiceImpl(EcmMetaDataRepository repo) {
		super(repo);
	}
	
	@Override
	public void findProductIdAndSave(MetaDataEntity entity) {
				
		if (getAbstractBaseRepository().count()==0) {
			save(entity);
		 }
		
	}

}
