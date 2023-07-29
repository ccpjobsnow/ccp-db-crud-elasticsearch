package com.ccp.implementations.db.crud.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.especifications.db.crud.CcpDao;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpRecordNotFound;
import com.ccp.process.CcpProcess;
import com.ccp.process.ThrowException;


class CcpElasticSearchDao implements CcpDao {

	@CcpDependencyInject
	private CcpDbUtils dbUtils;
	
	private final CcpSourceHandler mgetHandler = new CcpSourceHandler();

	private List<CcpMapDecorator> extractListFromMgetResponse(CcpMapDecorator requestBody) {
		CcpMapDecorator response = this.dbUtils.executeHttpRequest("/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		
		List<CcpMapDecorator> docs = response.getAsMapList("docs");
		List<CcpMapDecorator> collect = docs.stream().map(x -> this.mgetHandler.execute(x)).collect(Collectors.toList());
		return collect;
	}

	@Override
	public List<CcpMapDecorator> getManyById(CcpMapDecorator values, CcpEntity... tables) {

		List<CcpMapDecorator> asList = Arrays.asList(tables).stream().map(
				table -> {
					String id = table.getId(values);
					String tableName = table.name();
					return new CcpMapDecorator()
					.put("_id", id)
					.put("_index", tableName);
				})
				.collect(Collectors.toList());
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", asList);
		
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		
		return asMapList;
	}

	@Override
	public CcpMapDecorator getOneById(CcpEntity tableName, CcpMapDecorator values) {
	
		String id = tableName.getId(values);
		
		String path = "/" + tableName + "/_doc/" + id + "/_source";
		
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpConstants.DO_NOTHING).put("404", new ThrowException(new CcpRecordNotFound(tableName.name(), id)));
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public List<CcpMapDecorator> getManyByIds(CcpEntity tableName, String... ids) {
	
		List<String> asList = Arrays.asList(ids);
		CcpMapDecorator requestBody = new CcpMapDecorator().put("ids", asList);
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		return asMapList;
	}

	@Override
	public boolean exists(CcpEntity tableName, CcpMapDecorator values) {
		String id = tableName.getId(values);
		
		String path = "/" + tableName + "/_doc/" + id;
		
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	@Override
	public CcpMapDecorator createOrUpdate(CcpEntity tableName, CcpMapDecorator data) {
		
		String id = tableName.getId(data);

		String path = "/" + tableName + "/_update/" + id;
		
		CcpMapDecorator requestBody = new CcpMapDecorator()
				.putSubKey("script", "lang", "painless")
				.putSubKey("script", "source", "ctx._source = params")
				.putSubKey("script", "params", data)
				.put("upsert", data)
				;
		
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("201",  CcpHttpStatus.CREATED);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "POST", handlers, requestBody, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public CcpMapDecorator delete(CcpEntity tableName, CcpMapDecorator values) {
		String id = tableName.getId(values);
		System.out.println(id);
		// TODO Auto-generated method stub
		return null;
		
	}

	@Override
	public List<CcpMapDecorator> getManyById(List<CcpMapDecorator> values, CcpEntity... tables) {
		List<CcpMapDecorator> docs = new ArrayList<CcpMapDecorator>();
		for (CcpEntity table : tables) {
			String tableName = table.name();
			for (CcpMapDecorator value : values) {
				String id = table.getId(value);
				CcpMapDecorator put = new CcpMapDecorator()
				.put("_id", id)
				.put("_index", tableName);
				docs.add(put);
			}
		}
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", docs);
		
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		
		return asMapList;
	}

}
enum CcpHttpStatus implements CcpProcess{
	OK,
	NOT_FOUND, 
	CREATED;


	@Override
	public CcpMapDecorator execute(CcpMapDecorator values) {
		return values.put(CcpHttpStatus.class.getSimpleName(), this);
	}
	
}

