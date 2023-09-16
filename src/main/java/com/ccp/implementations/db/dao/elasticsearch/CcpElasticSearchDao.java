package com.ccp.implementations.db.dao.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class CcpElasticSearchDao implements CcpInstanceProvider  {

	@Override
	public Object getInstance() {
		return new ElasticSearchDao();
	}

}
