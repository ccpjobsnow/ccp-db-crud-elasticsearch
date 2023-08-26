package com.ccp.implementations.db.dao.elasticsearch;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.ccp.constantes.CcpConstants;
import com.ccp.decorators.CcpMapDecorator;
import com.ccp.decorators.CcpTimeDecorator;
import com.ccp.dependency.injection.CcpInstanceInjection;
import com.ccp.especifications.db.dao.CcpDao;
import com.ccp.especifications.db.utils.CcpDbUtils;
import com.ccp.especifications.db.utils.CcpIdGenerator;
import com.ccp.especifications.http.CcpHttpResponseType;
import com.ccp.exceptions.commons.ThrowException;
import com.ccp.exceptions.db.CcpRecordNotFound;
import com.ccp.exceptions.db.MissingKeys;


class CcpElasticSearchDao implements CcpDao {

	
	private final CcpSourceHandler mgetHandler = new CcpSourceHandler();

	private List<CcpMapDecorator> extractListFromMgetResponse(CcpMapDecorator requestBody) {
	
		CcpDbUtils dbUtils = CcpInstanceInjection.getInstance(CcpDbUtils.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest("/_mget", "POST", 200, requestBody, CcpHttpResponseType.singleRecord);
		
		List<CcpMapDecorator> docs = response.getAsMapList("docs");
		List<CcpMapDecorator> collect = docs.stream().map(x -> this.mgetHandler.apply(x)).collect(Collectors.toList());
		return collect;
	}

	@Override
	public List<CcpMapDecorator> getManyById(CcpMapDecorator values, CcpIdGenerator... entities) {

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
	public CcpMapDecorator getOneById(CcpIdGenerator entity, CcpMapDecorator values) {
	
		String id = entity.getId(values);
		
		CcpMapDecorator oneById = this.getOneById(entity, id);
		
		return oneById;
	}

	@Override
	public CcpMapDecorator getOneById(CcpIdGenerator entity, String id) {
		String path = "/" + entity + "/_doc/" + id + "/_source";
		
		CcpMapDecorator handlers = new CcpMapDecorator().put("200", CcpConstants.DO_NOTHING).put("404", new ThrowException(new CcpRecordNotFound(entity.name(), id)));
		
		CcpDbUtils dbUtils = CcpInstanceInjection.getInstance(CcpDbUtils.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest(path, "GET", handlers, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		
		return response;
	}

	@Override
	public List<CcpMapDecorator> getManyByIds(CcpIdGenerator entity, String... ids) {
	
		List<String> asList = Arrays.asList(ids);
		CcpMapDecorator requestBody = new CcpMapDecorator().put("ids", asList);
		List<CcpMapDecorator> asMapList = this.extractListFromMgetResponse(requestBody);
		return asMapList;
	}

	@Override
	public boolean exists(CcpIdGenerator entity, CcpMapDecorator values) {
		String id = entity.getId(values);
		
		String path = "/" + entity + "/_doc/" + id;
		
		CcpMapDecorator flows = new CcpMapDecorator().put("200", CcpHttpStatus.OK).put("404",  CcpHttpStatus.NOT_FOUND);
		
		CcpDbUtils dbUtils = CcpInstanceInjection.getInstance(CcpDbUtils.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest(path, "HEAD", flows, CcpConstants.EMPTY_JSON, CcpHttpResponseType.singleRecord);
		CcpHttpStatus status = response.getAsObject(CcpHttpStatus.class.getSimpleName());
		
		boolean exists = CcpHttpStatus.OK.equals(status);
		return exists;
	}

	@Override
	public CcpMapDecorator createOrUpdate(CcpIdGenerator entity, CcpMapDecorator data) {
		
		String id = entity.getId(data);

		CcpMapDecorator response = this.createOrUpdate(entity, data, id);
		
		return response;
	}

	@Override
	public CcpMapDecorator createOrUpdate(CcpIdGenerator entity, CcpMapDecorator data, String id) {
		CcpMapDecorator onlyExistingFields = entity.getOnlyExistingFields(data);
		String path = "/" + entity + "/_update/" + id;
		
		CcpMapDecorator requestBody = new CcpMapDecorator()
				.putSubKey("script", "lang", "painless")
				.putSubKey("script", "source", "ctx._source.putAll(params);")
				.putSubKey("script", "params", onlyExistingFields)
				.put("upsert", onlyExistingFields)
				;
		
		CcpMapDecorator handlers = new CcpMapDecorator()
				.put("201",  CcpHttpStatus.CREATED)
				.put("200", CcpHttpStatus.OK)
				.put("409", values -> {
						new CcpTimeDecorator().sleep(1000);
						return this.createOrUpdate(entity, data, id);
				})
				;
		
		CcpDbUtils dbUtils = CcpInstanceInjection.getInstance(CcpDbUtils.class);
		CcpMapDecorator response = dbUtils.executeHttpRequest(path, "POST", handlers, requestBody, CcpHttpResponseType.singleRecord);
		return response;
	}

	@Override
	public CcpMapDecorator delete(CcpIdGenerator entity, CcpMapDecorator values) {
		String id = entity.getId(values);
		System.out.println(id);
		// TODO Auto-generated method stub
		return null;
		
	}

	@Override
	public CcpMapDecorator getAllData(CcpMapDecorator values, CcpIdGenerator... entities) {
		List<CcpMapDecorator> docs = new ArrayList<>();
		for (CcpIdGenerator entity : entities) {
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
	public List<CcpMapDecorator> getManyById(List<CcpMapDecorator> values, CcpIdGenerator... entities) {
		List<CcpMapDecorator> docs = new ArrayList<CcpMapDecorator>();
		for (CcpIdGenerator entity : entities) {
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

