package org.springframework.data.elasticsearch.core;

import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostableQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Criteria;
import org.springframework.data.elasticsearch.core.query.Field;
import org.springframework.data.elasticsearch.core.query.NestedField;
import org.springframework.util.Assert;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import static org.elasticsearch.index.query.QueryBuilders.*;

/**
 * User: patryk
 * Date: 2015-11-26
 * Time: 8:55 AM
 */
public class NestedCriteriaQueryProcessor {

    public QueryBuilder createQueryFromCriteria(Criteria criteria) {
        if (criteria == null)
            return null;

        List<QueryBuilder> shouldQueryBuilderList = new LinkedList<QueryBuilder>();
        List<QueryBuilder> mustNotQueryBuilderList = new LinkedList<QueryBuilder>();
        List<QueryBuilder> mustQueryBuilderList = new LinkedList<QueryBuilder>();

        ListIterator<Criteria> chainIterator = criteria.getCriteriaChain().listIterator();

        QueryBuilder firstQuery = null;
        boolean negateFirstQuery = false;

        while (chainIterator.hasNext()) {
            Criteria chainedCriteria = chainIterator.next();
            QueryBuilder queryFragmentForCriteria = createQueryFragmentForCriteria(chainedCriteria);
            if (queryFragmentForCriteria != null) {
                if (firstQuery == null) {
                    firstQuery = queryFragmentForCriteria;
                    negateFirstQuery = chainedCriteria.isNegating();
                    continue;
                }
                if (chainedCriteria.isOr()) {
                    shouldQueryBuilderList.add(queryFragmentForCriteria);
                } else if (chainedCriteria.isNegating()) {
                    mustNotQueryBuilderList.add(queryFragmentForCriteria);
                } else {
                    mustQueryBuilderList.add(queryFragmentForCriteria);
                }
            }
        }

        if (firstQuery != null) {
            if (!shouldQueryBuilderList.isEmpty() && mustNotQueryBuilderList.isEmpty() && mustQueryBuilderList.isEmpty()) {
                shouldQueryBuilderList.add(0, firstQuery);
            } else {
                if (negateFirstQuery) {
                    mustNotQueryBuilderList.add(0, firstQuery);
                } else {
                    mustQueryBuilderList.add(0, firstQuery);
                }
            }
        }

        BoolQueryBuilder query = null;

        if (!shouldQueryBuilderList.isEmpty() || !mustNotQueryBuilderList.isEmpty() || !mustQueryBuilderList.isEmpty()) {

            query = boolQuery();

            for (QueryBuilder qb : shouldQueryBuilderList) {
                query.should(qb);
            }
            for (QueryBuilder qb : mustNotQueryBuilderList) {
                query.mustNot(qb);
            }
            for (QueryBuilder qb : mustQueryBuilderList) {
                query.must(qb);
            }
        }

        return query;
    }


    private QueryBuilder createQueryFragmentForCriteria(Criteria chainedCriteria) {
        if (chainedCriteria.getQueryCriteriaEntries().isEmpty())
            return null;

        Iterator<Criteria.CriteriaEntry> it = chainedCriteria.getQueryCriteriaEntries().iterator();
        boolean singeEntryCriteria = (chainedCriteria.getQueryCriteriaEntries().size() == 1);

        Field field = chainedCriteria.getField();
        String fieldName = field.getName();
        Assert.notNull(fieldName, "Unknown field");
        QueryBuilder query;

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

        if (field instanceof NestedField && ((NestedField) field).isNested()) {
            String nestedRoot = fieldName.split("\\.")[0];
            if (query != null) {
                query = nestedQuery(nestedRoot, query);
            }
        }

        addBoost(query, chainedCriteria.getBoost());
        return query;
    }


    @SuppressWarnings("unchecked")
    private QueryBuilder processCriteriaEntry(Criteria.CriteriaEntry entry, String fieldName) {
        Object value = entry.getValue();
        if (value == null) {
            return null;
        }
        Criteria.OperationKey key = entry.getKey();
        QueryBuilder query = null;

        String searchText = StringUtils.toString(value);

        Iterable<Object> collection;

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
        if (query != null) {
            if (Float.isNaN(boost)) {
                return;
            }
            if (query instanceof BoostableQueryBuilder) {
                ((BoostableQueryBuilder) query).boost(boost);
            }
        }
    }

}
