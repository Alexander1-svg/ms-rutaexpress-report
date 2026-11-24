package com.ms_rutaexpress.kafka_analytics.service;

import com.ms_rutaexpress.kafka_analytics.dto.AnalyticsSummaryResponse;
import com.ms_rutaexpress.kafka_analytics.dto.RecentShipmentEventResponse;
import com.ms_rutaexpress.kafka_analytics.dto.StatusAnalyticsResponse;
import com.ms_rutaexpress.kafka_analytics.repository.AnalyticsStore;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AnalyticsQueryService {

    private final AnalyticsStore store;

    public AnalyticsQueryService(AnalyticsStore store) {
        this.store = store;
    }

    public AnalyticsSummaryResponse summary() {
        return store.snapshot();
    }

    public StatusAnalyticsResponse statuses() {
        return new StatusAnalyticsResponse(store.byStatus());
    }

    public List<RecentShipmentEventResponse> recent(int limit) {
        return store.recent(limit);
    }
}
