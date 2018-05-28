/*
 * Copyright 2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.elasticsearch.core;

import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.springframework.data.elasticsearch.core.query.Criteria.*;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostableQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.util.Assert;

/**
 * CriteriaQueryProcessor
 *
 * @author Rizwan Idrees
 * @author Mohsin Husen
 * @author Franck Marchand
 * @author Artur Konczak
 */
class CriteriaQueryProcessor {


	QueryBuilder createQueryFromCriteria(Criteria criteria) {
		return new NestedCriteriaQueryProcessor().createQueryFromCriteria(criteria);
	}


	private QueryBuilder createQueryFragmentForCriteria(Criteria chainedCriteria) {
		if (chainedCriteria.getQueryCriteriaEntries().isEmpty())
			return null;

		Iterator<Criteria.CriteriaEntry> it = chainedCriteria.getQueryCriteriaEntries().iterator();
		boolean singeEntryCriteria = (chainedCriteria.getQueryCriteriaEntries().size() == 1);

		String fieldName = chainedCriteria.getField().getName();
		Assert.notNull(fieldName, "Unknown field");
		QueryBuilder query = null;

		if (singeEntryCriteria) {
			Criteria.CriteriaEntry entry = it.next();
			query = processCriteriaEntry(entry, fieldName);
		} else {
			query = boolQuery();
			while (it.hasNext()) {
				Criteria.CriteriaEntry entry = it.next();
				((BoolQueryBuilder) query).must(processCriteriaEntry(entry, fieldName));
			}
		}

		addBoost(query, chainedCriteria.getBoost());
		return query;
	}


	private QueryBuilder processCriteriaEntry(Criteria.CriteriaEntry entry,/* OperationKey key, Object value,*/ String fieldName) {
		Object value = entry.getValue();
		if (value == null) {
			return null;
		}
		OperationKey key = entry.getKey();
		QueryBuilder query = null;

		String searchText = StringUtils.toString(value);

		Iterable<Object> collection = null;

		switch (key) {
			case EQUALS:
				query = queryStringQuery(searchText).field(fieldName).defaultOperator(QueryStringQueryBuilder.Operator.AND);
				break;
			case CONTAINS:
				query = queryStringQuery("*" + searchText + "*").field(fieldName).analyzeWildcard(true);
				break;
			case STARTS_WITH:
				query = queryStringQuery(searchText + "*").field(fieldName).analyzeWildcard(true);
				break;
			case ENDS_WITH:
				query = queryStringQuery("*" + searchText).field(fieldName).analyzeWildcard(true);
				break;
			case EXPRESSION:
				query = queryStringQuery(searchText).field(fieldName);
				break;
			case LESS_EQUAL:
				query = rangeQuery(fieldName).lte(value);
				break;
			case GREATER_EQUAL:
				query = rangeQuery(fieldName).gte(value);
				break;
			case BETWEEN:
				Object[] ranges = (Object[]) value;
				query = rangeQuery(fieldName).from(ranges[0]).to(ranges[1]);
				break;
			case LESS:
				query = rangeQuery(fieldName).lt(value);
				break;
			case GREATER:
				query = rangeQuery(fieldName).gt(value);
				break;
			case FUZZY:
				query = fuzzyQuery(fieldName, searchText);
				break;
			case IN:
				query = boolQuery();
				collection = (Iterable<Object>) value;
				for (Object item : collection) {
					((BoolQueryBuilder) query).should(queryStringQuery(item.toString()).field(fieldName));
				}
				break;
			case NOT_IN:
				query = boolQuery();
				collection = (Iterable<Object>) value;
				for (Object item : collection) {
					((BoolQueryBuilder) query).mustNot(queryStringQuery(item.toString()).field(fieldName));
				}
				break;
		}
		return query;
	}

	private void addBoost(QueryBuilder query, float boost) {
		if (Float.isNaN(boost)) {
			return;
		}
		if (query instanceof BoostableQueryBuilder) {
			((BoostableQueryBuilder) query).boost(boost);
		}
	}
}
