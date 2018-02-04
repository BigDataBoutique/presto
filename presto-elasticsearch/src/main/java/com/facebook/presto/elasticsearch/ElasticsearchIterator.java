package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.PrestoException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.util.Iterator;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_SCROLL_ERROR;

public class ElasticsearchIterator implements Iterator<SearchHit> {

    private final RestHighLevelClient client;
    private String scrollId;
    private SearchHits hits;
    private int currentHitsPosition = 0;
    private int totalHitsSeen = 0;

    ElasticsearchIterator(SearchHits hits, RestHighLevelClient client, String scrollId) {
        this.hits = hits;
        this.client = client;
        this.scrollId = scrollId;
    }

    @Override
    public boolean hasNext() {
        return totalHitsSeen < hits.getTotalHits();
    }

    @Override
    public SearchHit next() {
        if (currentHitsPosition >= hits.getHits().length) {
            SearchScrollRequest scrollRequest = new SearchScrollRequest(this.scrollId);
            scrollRequest.scroll(TimeValue.timeValueSeconds(60));

            SearchResponse searchScrollResponse;
            try {
                searchScrollResponse = client.searchScroll(scrollRequest);
            } catch (IOException e) {
                throw new PrestoException(ELASTICSEARCH_SCROLL_ERROR, e);
            }

            this.scrollId = searchScrollResponse.getScrollId();
            this.hits = searchScrollResponse.getHits();
            currentHitsPosition = 0;
        }
        ++totalHitsSeen;
        return hits.getAt(currentHitsPosition++);
    }
}
