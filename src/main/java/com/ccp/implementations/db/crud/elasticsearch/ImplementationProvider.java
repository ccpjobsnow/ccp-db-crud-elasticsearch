package com.ccp.implementations.db.crud.elasticsearch;

import com.ccp.dependency.injection.CcpImplementationProvider;

public class ImplementationProvider implements CcpImplementationProvider  {

	@Override
	public Object getImplementation() {
		return new DbCrudElasticSearch();
	}

}
