package com.ccp.implementations.db.dao.elasticsearch;

import com.ccp.dependency.injection.CcpModuleExporter;

public class Dao implements CcpModuleExporter  {

	@Override
	public Object export() {
		return new CcpElasticSearchDao();
	}

}
