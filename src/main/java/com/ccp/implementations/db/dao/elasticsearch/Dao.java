package com.ccp.implementations.db.dao.elasticsearch;

import com.ccp.dependency.injection.CcpInstanceProvider;

public class Dao implements CcpInstanceProvider  {

	@Override
	public Object getInstance() {
		return new CcpElasticSearchDao();
	}

}
