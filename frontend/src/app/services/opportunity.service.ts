import { Injectable } from '@angular/core';
import { HttpClient, HttpParams } from '@angular/common/http';
import { Observable } from 'rxjs';
import { environment } from '../../environments/environment';

export interface Opportunity {
    id: string;
    title: string;
    summary: string;
    description_html?: string; // Full HTML description
    external_url: string;
    source_domain: string;
    source_id: string;
    opportunity_number?: string;
    agency_name: string;
    agency_code: string;
    funder_type: string;
    amount_min: number;
    amount_max: number;
    currency: string;
    deadline_at?: string;
    next_deadline_at?: string;
    open_date?: string;
    open_at?: string;
    close_at?: string;
    expiration_at?: string;
    is_rolling: boolean;
    doc_type: string;
    opp_status?: string;
    source_status_raw?: string;
    normalized_status?: 'open' | 'upcoming' | 'closed' | 'archived' | 'needs_review';
    status_reason?: string;
    deadlines?: string[];
    is_results_page?: boolean;
    region: string;
    country: string;
    categories: string[];
    eligibility: string[];
    cfda_list?: string[];
    close_date_raw?: string; // Original deadline text
    created_at: string;
}

export interface ListResult {
    opportunities: Opportunity[];
    total: number;
    limit: number;
    offset: number;
}

export interface SearchFilters {
    q?: string;
    source?: string;
    region?: string;
    funder_type?: string;
    country?: string[];    // multi-select
    agency_name?: string[]; // multi-select
    min_amount?: number;
    max_amount?: number;
    deadline_days?: number;
    limit?: number;
    offset?: number;
    categories?: string[];
    eligibility?: string[];
    sort?: string;
    status?: string;
}

export interface Aggregation {
    value: string;
    count: number;
}
export interface AggregationResult {
    regions: Aggregation[];
    funder_types: Aggregation[];
    agencies: Aggregation[];
    countries: Aggregation[];
}

@Injectable({
    providedIn: 'root'
})
export class OpportunityService {
    private apiUrl = `${environment.apiBaseUrl}/api/v1`;

    constructor(private http: HttpClient) { }

    search(filters: SearchFilters = {}): Observable<ListResult> {
        let params = new HttpParams();
        if (filters.q) params = params.set('q', filters.q);
        if (filters.source) params = params.set('source', filters.source);
        if (filters.region) params = params.set('region', filters.region);
        if (filters.funder_type) params = params.set('funder_type', filters.funder_type);
        if (filters.country?.length) params = params.set('country', filters.country.join(','));
        if (filters.agency_name?.length) params = params.set('agency_name', filters.agency_name.join(','));
        if (filters.min_amount) params = params.set('min_amount', filters.min_amount.toString());
        if (filters.max_amount) params = params.set('max_amount', filters.max_amount.toString());
        if (filters.deadline_days) params = params.set('deadline_days', filters.deadline_days.toString());
        if (filters.limit) params = params.set('limit', filters.limit.toString());
        if (filters.offset !== undefined) params = params.set('offset', filters.offset.toString());
        if (filters.sort) params = params.set('sort', filters.sort);
        if (filters.status) params = params.set('status', filters.status);

        if (filters.categories) {
            filters.categories.forEach(c => params = params.append('categories', c));
        }
        if (filters.eligibility) {
            filters.eligibility.forEach(e => params = params.append('eligibility', e));
        }

        return this.http.get<ListResult>(`${this.apiUrl}/opportunities`, { params });
    }

    getOpportunity(id: string): Observable<Opportunity> {
        return this.http.get<Opportunity>(`${this.apiUrl}/opportunities/${id}`);
    }

    getSources(): Observable<string[]> {
        return this.http.get<string[]>(`${this.apiUrl}/sources`);
    }

    getStats(): Observable<any> {
        return this.http.get<any>(`${this.apiUrl}/stats`);
    }

    getAggregations(filters: SearchFilters = {}): Observable<AggregationResult> {
        let params = new HttpParams();
        if (filters.status) params = params.set('status', filters.status);
        if (filters.region) params = params.set('region', filters.region);
        if (filters.funder_type) params = params.set('funder_type', filters.funder_type);
        if (filters.country?.length) params = params.set('country', filters.country.join(','));
        if (filters.agency_name?.length) params = params.set('agency_name', filters.agency_name.join(','));
        return this.http.get<AggregationResult>(`${this.apiUrl}/aggregations`, { params });
    }

    // Saved Opportunities
    saveOpportunity(id: string): Observable<void> {
        return this.http.post<void>(`${this.apiUrl}/saved/${id}`, {});
    }

    unsaveOpportunity(id: string): Observable<void> {
        return this.http.delete<void>(`${this.apiUrl}/saved/${id}`);
    }

    getSavedOpportunities(): Observable<Opportunity[]> {
        return this.http.get<Opportunity[]>(`${this.apiUrl}/saved`);
    }
}
