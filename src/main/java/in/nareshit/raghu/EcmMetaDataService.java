package com.hsbc.gbm.surveillance.sdf.trade.processor.service;

import com.hsbc.gbm.surveillance.sdf.trade.processor.entity.MetaDataEntity;

public interface EcmMetaDataService extends AbstarctBaseService<MetaDataEntity, String> {

	public void findProductIdAndSave(MetaDataEntity entity);
}
