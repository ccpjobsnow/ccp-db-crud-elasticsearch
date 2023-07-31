package com.ccp.implementations.db.dao.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.dependency.injection.CcpDependencyInject;
import com.ccp.especifications.db.dao.CcpDao;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.db.utils.CcpEntity;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpRecordNotFound;
import com.ccp.exceptions.db.MissingKeys;
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
	public List<CcpMapDecorator> getManyById(CcpMapDecorator values, CcpEntity... entities) {

		List<CcpMapDecorator> asList = Arrays.asList(entities).stream().map(
				entity -> {
					String id = entity.getId(values);
					String entidade = entity.name();
					return new CcpMapDecorator()
					.put("_id", id)
					.put("_index", entidade);
				})
				.collect(Collectors.toList());
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", asList);
		
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		
		return asMapList;
	}

	@Override
	public CcpMapDecorator getOneById(CcpEntity entity, CcpMapDecorator values) {
	
		String id = entity.getId(values);
		
		CcpMapDecorator oneById = this.getOneById(entity, id);
		
		return oneById;
	}

	@Override
	public CcpMapDecorator getOneById(CcpEntity entity, String id) {
		String path = "/" + entity + "/_doc/" + id + "/_source";
		
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpConstants.DO_NOTHING).put("404", new ThrowException(new CcpRecordNotFound(entity.name(), id)));
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public List<CcpMapDecorator> getManyByIds(CcpEntity entity, String... ids) {
	
		List<String> asList = Arrays.asList(ids);
		CcpMapDecorator requestBody = new CcpMapDecorator().put("ids", asList);
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		return asMapList;
	}

	@Override
	public boolean exists(CcpEntity entity, CcpMapDecorator values) {
		String id = entity.getId(values);
		
		String path = "/" + entity + "/_doc/" + id;
		
		CcpMapDecorator flows = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "HEAD", flows, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	@Override
	public CcpMapDecorator createOrUpdate(CcpEntity entity, CcpMapDecorator data) {
		
		String id = entity.getId(data);

		String path = "/" + entity + "/_update/" + id;
		
		CcpMapDecorator requestBody = new CcpMapDecorator()
				.putSubKey("script", "lang", "painless")
				.putSubKey("script", "source", "ctx._source.putAll(params);")
				.putSubKey("script", "params", data)
				.put("upsert", data)
				;
		
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("201",  CcpHttpStatus.CREATED);
		
		CcpMapDecorator response = this.dbUtils.executeHttpRequest(path, "POST", handlers, requestBody, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public CcpMapDecorator delete(CcpEntity entity, CcpMapDecorator values) {
		String id = entity.getId(values);
		System.out.println(id);
		// TODO Auto-generated method stub
		return null;
		
	}

	@Override
	public CcpMapDecorator getAllData(CcpMapDecorator values, CcpEntity... entities) {
		List<CcpMapDecorator> docs = new ArrayList<>();
		for (CcpEntity entity : entities) {
			try {
				String id = entity.getId(values);
				CcpMapDecorator doc = new CcpMapDecorator().put("_id", id).put("_index", entity.name());
				docs.add(doc);
			} catch (MissingKeys e) {
				continue;
			}
		}
		
		if(docs.isEmpty()) {
			return new CcpMapDecorator();
		}
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", docs);
		
		List<CcpMapDecorator> results = this.extractListFromMgetResponse(requestBody);

		CcpMapDecorator response = new CcpMapDecorator();
		
		for (CcpMapDecorator result : results) {
			boolean notFound = result.getAsBoolean("_found") == false;
			if(notFound) {
				continue;
			}
			String entity = result.getAsString("_index");
			response = response.put(entity, result.removeKeys("_found", "_index"));
		}
		return response;
	}
	@Override
	public List<CcpMapDecorator> getManyById(List<CcpMapDecorator> values, CcpEntity... entities) {
		List<CcpMapDecorator> docs = new ArrayList<CcpMapDecorator>();
		for (CcpEntity entity : entities) {
			String entidade = entity.name();
			for (CcpMapDecorator value : values) {
				String id = entity.getId(value);
				CcpMapDecorator put = new CcpMapDecorator()
				.put("_id", id)
				.put("_index", entidade);
				docs.add(put);
			}
		}
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", docs);
		
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		List<CcpMapDecorator> collect = asMapList.stream().filter(x -> x.getAsBoolean("_found")).map(x -> x.removeKeys("_found", "_index")).collect(Collectors.toList());
		return collect;
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

