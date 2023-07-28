package com.ccp.implementations.db.crud.elasticsearch;

import com.ccp.dependency.injection.CcpModuleExporter;

public class Crud implements CcpModuleExporter  {

	@Override
	public Object export() {
		return new CcpElasticSearchDao();
	}

}
