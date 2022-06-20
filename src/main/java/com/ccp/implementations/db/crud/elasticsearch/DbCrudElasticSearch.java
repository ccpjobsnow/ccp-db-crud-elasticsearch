package com.ccp.implementations.db.crud.elasticsearch;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpEspecification;
import com.ccp.especifications.db.crud.CcpDbCrud;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpRecordNotFound;
import com.ccp.process.CcpProcess;


class DbCrudElasticSearch implements CcpDbCrud {

	@CcpEspecification
	private CcpDbUtils dbUtils;
	
	private final CcpSourceHandler mgetHandler = new CcpSourceHandler();

	@Override
	public List<CcpMapDecorator> getManyByIds(CcpMapDecorator filterEspecifications) {
		List<CcpMapDecorator> asList = filterEspecifications.keySet().stream().map(
				table -> new CcpMapDecorator()
				.put("_id", filterEspecifications.get(table))
				.put("_index", table))
				.collect(Collectors.toList());
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", asList);
		
		List<CcpMapDecorator> collect = this.extractListFromMgetResponse(requestBody);
		
		return collect;
	}

	private List<CcpMapDecorator> extractListFromMgetResponse(CcpMapDecorator requestBody) {
		CcpMapDecorator response = this.dbUtils.executeHttpRequest("/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		
		List<CcpMapDecorator> docs = response.getAsMapList("docs");
		List<CcpMapDecorator> collect = docs.stream().map(x -> this.mgetHandler.execute(x)).collect(Collectors.toList());
		return collect;
	}

	@Override
	public List<CcpMapDecorator> getManyById(String id, String... tables) {

		List<CcpMapDecorator> asList = Arrays.asList(tables).stream().map(
				table -> new CcpMapDecorator()
				.put("_id", id)
				.put("_index", table))
				.collect(Collectors.toList());
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", asList);
		
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		
		return asMapList;
	}

	@Override
	public CcpMapDecorator getOneById(String id, String tableName) {
	
		String path = "/" + tableName + "/_doc/" + id + "/_source";
		
		CcpProcess throwNotFoundError =  x -> {
			throw new CcpRecordNotFound(tableName, id);
		};

		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpConstants.doNothing).put("404", throwNotFoundError);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.emptyJson, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public List<CcpMapDecorator> getManyByIds(String[] ids, String tableName) {
	
		List<String> asList = Arrays.asList(ids);
		CcpMapDecorator requestBody = new CcpMapDecorator().put("ids", asList);
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		return asMapList;
	}

	@Override
	public boolean exists(String id, String tableName) {
		String path = "/" + tableName + "/_doc/" + id;
		

		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.emptyJson, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	@Override
	public boolean updateOrSave(CcpMapDecorator data, String id, String tableName) {
		// TODO Auto-generated method stub
		return false;
	}

}
enum CcpHttpStatus implements CcpProcess{
	OK,
	NOT_FOUND;


	@Override
	public CcpMapDecorator execute(CcpMapDecorator values) {
		return values.put(CcpHttpStatus.class.getSimpleName(), this);
	}
	
}

