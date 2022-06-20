package com.ccp.implementations.db.crud.elasticsearch;

import com.ccp.dependency.injection.CcpEspecification.DefaultImplementationProvider;

public class ImplementationProvider extends DefaultImplementationProvider  {

	@Override
	public Object getImplementation() {
		return new DbCrudElasticSearch();
	}

}
