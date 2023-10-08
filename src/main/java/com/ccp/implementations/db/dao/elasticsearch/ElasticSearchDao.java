package com.ccp.implementations.db.dao.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.dependency.injection.CcpDependencyInjection;
import com.ccp.especifications.db.dao.CcpDao;
import com.ccp.especifications.db.utils.CcpDbRequester;
import com.ccp.especifications.db.utils.CcpEntityIdGenerator;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.db.CcpEntityRecordNotFound;
import com.ccp.exceptions.db.CcpEntityMissingKeys;
import com.ccp.exceptions.process.CcpThrowException;

class ElasticSearchDao implements CcpDao {

	private final SourceHandler mgetHandler = new SourceHandler();

	private List<CcpMapDecorator> extractListFromMgetResponse(CcpMapDecorator requestBody) {
	
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest("/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		
		List<CcpMapDecorator> docs = response.getAsMapList("docs");
		List<CcpMapDecorator> collect = docs.stream().map(x -> this.mgetHandler.apply(x)).collect(Collectors.toList());
		return collect;
	}

	@Override
	public List<CcpMapDecorator> getManyById(CcpMapDecorator values, CcpEntityIdGenerator... entities) {

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
	public CcpMapDecorator getOneById(CcpEntityIdGenerator entity, CcpMapDecorator values) {
	
		String id = entity.getId(values);
		
		CcpMapDecorator oneById = this.getOneById(entity, id);
		
		return oneById;
	}

	@Override
	public CcpMapDecorator getOneById(CcpEntityIdGenerator entity, String id) {
		String path = "/" + entity + "/_source/" + id ;
		
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpConstants.DO_NOTHING).put("404", new CcpThrowException(new CcpEntityRecordNotFound(entity.name(), id)));
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public List<CcpMapDecorator> getManyByIds(CcpEntityIdGenerator entity, String... ids) {
	
		List<String> asList = Arrays.asList(ids);
		CcpMapDecorator requestBody = new CcpMapDecorator().put("ids", asList);
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		return asMapList;
	}

	@Override
	public boolean exists(CcpEntityIdGenerator entity, String id) {
		String path = "/" + entity + "/_doc/" + id;
		
		CcpMapDecorator flows = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest(path, "HEAD", flows, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	@Override
	public CcpMapDecorator createOrUpdate(CcpEntityIdGenerator entity, CcpMapDecorator data, String id) {
		CcpMapDecorator onlyExistingFields = entity.getOnlyExistingFields(data);
		String path = "/" + entity + "/_update/" + id;
		
		CcpMapDecorator requestBody = new CcpMapDecorator()
				.putSubKey("script", "lang", "painless")
				.putSubKey("script", "source", "ctx._source.putAll(params);")
				.putSubKey("script", "params", onlyExistingFields)
				.put("upsert", onlyExistingFields)
				;
		
		CcpMapDecorator handlers = new CcpMapDecorator()
				.put("409", values -> this.retryCreateOrUpdate(entity, data, id))
				.put("201",  CcpHttpStatus.CREATED)
				.put("200", CcpHttpStatus.OK)
				;
		
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest(path, "POST", handlers, requestBody, CcpHttpResponseType.singleRecord);
		return response;
	}

	private CcpMapDecorator retryCreateOrUpdate(CcpEntityIdGenerator entity, CcpMapDecorator data, String id) {
		new CcpTimeDecorator().sleep(1000);
		return this.createOrUpdate(entity, data, id);
	}

	@Override
	public boolean delete(CcpEntityIdGenerator entity, String id) {
		String path = "/" + entity + "/_doc/" + id;
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpConstants.DO_NOTHING).put("404", CcpConstants.DO_NOTHING);
		CcpDbRequester dbUtils = CcpDependencyInjection.getDependency(CcpDbRequester.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest(path, "DELETE", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		String result = response.getAsString("result");
		boolean found = "deleted".equals( result);
		return found;
	}

	@Override
	public CcpMapDecorator getAllData(CcpMapDecorator values, CcpEntityIdGenerator... entities) {
		
		CcpMapDecorator requestBody = this.getRequestBody(values, entities);
		
		List<CcpMapDecorator> results = this.extractListFromMgetResponse(requestBody);

		CcpMapDecorator response = this.extractResponse(results);

		return response;
	}

	private CcpMapDecorator extractResponse(List<CcpMapDecorator> results) {
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

	private CcpMapDecorator getRequestBody(CcpMapDecorator values, CcpEntityIdGenerator... entities) {
		List<CcpMapDecorator> docs = new ArrayList<>();
		for (CcpEntityIdGenerator entity : entities) {
			try {
				String id = entity.getId(values);
				CcpMapDecorator doc = new CcpMapDecorator().put("_id", id).put("_index", entity.name());
				docs.add(doc);
			} catch (CcpEntityMissingKeys e) {
				continue;
			}
		}
		
		if(docs.isEmpty()) {
			return new CcpMapDecorator();
		}
		
		CcpMapDecorator requestBody = new CcpMapDecorator().put("docs", docs);
		return requestBody;
	}
	@Override
	public List<CcpMapDecorator> getManyById(List<CcpMapDecorator> values, CcpEntityIdGenerator... entities) {
		List<CcpMapDecorator> docs = new ArrayList<CcpMapDecorator>();
		for (CcpEntityIdGenerator entity : entities) {
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
enum CcpHttpStatus implements  java.util.function.Function<CcpMapDecorator, CcpMapDecorator>{
	OK,
	NOT_FOUND, 
	CREATED;


	@Override
	public CcpMapDecorator apply(CcpMapDecorator values) {
		return values.put(CcpHttpStatus.class.getSimpleName(), this);
	}
	
}

