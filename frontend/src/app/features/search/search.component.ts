import { Component, OnInit, signal, inject, computed, HostListener } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { Opportunity, OpportunityService, SearchFilters, AggregationResult } from '../../services/opportunity.service';
import { AuthService } from '../../services/auth.service';
import { ThemeService } from '../../services/theme.service';
import { Subject } from 'rxjs';
import { debounceTime, distinctUntilChanged } from 'rxjs/operators';

@Component({
    selector: 'app-search',
    standalone: true,
    imports: [CommonModule, FormsModule, RouterLink],
    templateUrl: './search.component.html',
    styleUrl: './search.component.css'
})
export class SearchComponent implements OnInit {
    private opportunityService = inject(OpportunityService);
    public authService = inject(AuthService);
    public themeService = inject(ThemeService);

    // --- Core state ---
    opportunities = signal<Opportunity[]>([]);
    isLoading = signal(false);
    errorMessage = signal('');
    totalResults = signal(0);
    currentPage = signal(1);
    pageSize = signal(15);
    totalPages = computed(() => Math.ceil(this.totalResults() / this.pageSize()));

    // Search
    query = signal('');
    private searchTerms = new Subject<string>();

    // Saved
    savedIds = signal<Set<string>>(new Set());

    // Stats & Aggregations
    stats = signal<any>({});
    aggregations = signal<AggregationResult>({
        regions: [], funder_types: [], agencies: [], countries: []
    });

    // --- Filters ---
    selectedStatus = signal('open');
    selectedRegion = signal<string[]>([]);
    selectedFunderType = signal<string[]>([]);
    selectedCountry = signal<string[]>([]);
    selectedAgency = signal<string[]>([]);
    selectedDeadline = signal(0);
    selectedAmountIdx = signal(0);
    selectedCategories = signal<string[]>([]);
    selectedSort = signal('relevance');

    // --- Sidebar UI ---
    showSidebarDeadline = signal(true);
    showSidebarAmount = signal(true);
    showSidebarAgency = signal(true);
    showSidebarRegion = signal(true);
    showSidebarCountry = signal(true);
    showSidebarCategory = signal(false);

    // Agency/Country search
    agencySearch = signal('');
    countrySearch = signal('');

    // "Show more" limits
    agencyShowAll = signal(false);
    countryShowAll = signal(false);

    // Mobile sidebar
    mobileSidebarOpen = signal(false);

    // Detail modal
    selectedOpportunity = signal<Opportunity | null>(null);
    showDetail = signal(false);

    // Sort dropdown
    showSortDropdown = signal(false);

    // Diagnostics
    showDiagnostics = signal(false);

    // --- Constants ---
    sortOptions = [
        { label: 'Best Match', value: 'relevance', icon: '✦' },
        { label: 'Newest First', value: 'newest', icon: '🕐' },
        { label: 'Deadline (Closest)', value: 'deadline', icon: '⏰' },
        { label: 'Amount (High → Low)', value: 'amount_desc', icon: '💰' },
    ];

    amountPresets = [
        { label: 'Any Amount', min: 0, max: 0 },
        { label: 'Under $50K', min: 0, max: 50000 },
        { label: '$50K – $500K', min: 50000, max: 500000 },
        { label: '$500K – $2M', min: 500000, max: 2000000 },
        { label: 'Over $2M', min: 2000000, max: 0 },
    ];

    deadlinePresets = [
        { label: 'Any time', value: 0 },
        { label: 'Next 30 days', value: 30 },
        { label: 'Next 90 days', value: 90 },
        { label: 'Next 6 months', value: 180 },
        { label: 'Next year', value: 365 },
    ];

    categoriesList: string[] = [
        "Arts & Culture", "Business & Economy", "Community Development", "Education",
        "Environment & Climate", "Health & Medical", "Housing & Infrastructure",
        "Science & Technology", "Social Justice", "International",
        "Agriculture & Food", "Sports & Recreation"
    ];

    // --- Computed: filtered facets ---
    filteredAgencies = computed(() => {
        const all = this.aggregations().agencies || [];
        const search = this.agencySearch().toLowerCase();
        let list = search ? all.filter(a => a.value.toLowerCase().includes(search)) : all;
        if (!this.agencyShowAll() && !search) list = list.slice(0, 15);
        return list;
    });

    hiddenAgencyCount = computed(() => {
        const all = this.aggregations().agencies || [];
        return Math.max(0, all.length - 15);
    });

    filteredCountries = computed(() => {
        const all = this.aggregations().countries || [];
        const search = this.countrySearch().toLowerCase();
        let list = search ? all.filter(c => c.value.toLowerCase().includes(search)) : all;
        if (!this.countryShowAll() && !search) list = list.slice(0, 10);
        return list;
    });

    hiddenCountryCount = computed(() => {
        const all = this.aggregations().countries || [];
        return Math.max(0, all.length - 10);
    });

    // --- Active filter chips ---
    activeFilters = computed(() => {
        const chips: { label: string; type: string; value: string }[] = [];
        for (const r of this.selectedRegion()) chips.push({ label: r, type: 'region', value: r });
        for (const ft of this.selectedFunderType()) chips.push({ label: ft, type: 'funderType', value: ft });
        for (const c of this.selectedCountry()) chips.push({ label: c, type: 'country', value: c });
        for (const a of this.selectedAgency()) chips.push({ label: a, type: 'agency', value: a });
        for (const cat of this.selectedCategories()) chips.push({ label: cat, type: 'category', value: cat });
        const dl = this.selectedDeadline();
        if (dl > 0) {
            const p = this.deadlinePresets.find(x => x.value === dl);
            chips.push({ label: p?.label || `${dl}d`, type: 'deadline', value: String(dl) });
        }
        const ai = this.selectedAmountIdx();
        if (ai > 0) {
            chips.push({ label: this.amountPresets[ai].label, type: 'amount', value: String(ai) });
        }
        return chips;
    });

    hasActiveFilters = computed(() => this.activeFilters().length > 0 || !!this.query());

    constructor() {
        this.searchTerms.pipe(
            debounceTime(500),
            distinctUntilChanged(),
        ).subscribe(term => {
            this.query.set(term);
            this.currentPage.set(1);
            this.loadData();
        });
    }

    ngOnInit(): void {
        this.loadData();
        this.loadStats();
        if (this.authService.isAuthenticated()) this.loadSaved();
    }

    // --- Data loading ---
    loadStats() {
        this.opportunityService.getStats().subscribe({
            next: (s) => this.stats.set(s || {}),
            error: () => this.stats.set({})
        });
    }

    loadAggregations(filters: SearchFilters = {}) {
        this.opportunityService.getAggregations(filters).subscribe({
            next: (aggs) => this.aggregations.set(aggs || { regions: [], funder_types: [], agencies: [], countries: [] }),
            error: () => { }
        });
    }

    loadSaved(): void {
        if (!this.authService.isAuthenticated()) return;
        this.opportunityService.getSavedOpportunities().subscribe({
            next: (saved) => this.savedIds.set(new Set(saved.map(s => s.id))),
            error: (err) => console.error('Failed to load saved grants', err)
        });
    }

    loadData(): void {
        this.isLoading.set(true);
        this.errorMessage.set('');

        const preset = this.amountPresets[this.selectedAmountIdx()];
        const filters: SearchFilters = {
            q: this.query() || undefined,
            region: this.selectedRegion().length > 0 ? this.selectedRegion().join(',') : undefined,
            funder_type: this.selectedFunderType().length > 0 ? this.selectedFunderType().join(',') : undefined,
            country: this.selectedCountry().length > 0 ? this.selectedCountry() : undefined,
            agency_name: this.selectedAgency().length > 0 ? this.selectedAgency() : undefined,
            min_amount: preset.min || undefined,
            max_amount: preset.max || undefined,
            deadline_days: this.selectedDeadline() || undefined,
            categories: this.selectedCategories().length > 0 ? this.selectedCategories() : undefined,
            limit: this.pageSize(),
            offset: (this.currentPage() - 1) * this.pageSize(),
            sort: this.selectedSort(),
            status: this.selectedStatus(),
        };

        this.opportunityService.search(filters).subscribe({
            next: (result) => {
                if (!result) {
                    this.errorMessage.set('No response from server');
                    this.isLoading.set(false);
                    return;
                }
                this.opportunities.set(result.opportunities || []);
                this.totalResults.set(result.total || 0);
                this.isLoading.set(false);
            },
            error: (err) => {
                this.errorMessage.set(err.status === 0
                    ? 'Cannot connect to server. Is the Go backend running?'
                    : `Error loading grants: ${err.message || 'Unknown error'}`);
                this.opportunities.set([]);
                this.totalResults.set(0);
                this.isLoading.set(false);
            }
        });

        this.loadAggregations({ status: filters.status });
    }

    // --- Search ---
    onSearch(term: string): void {
        this.searchTerms.next(term);
    }

    // --- Status tabs ---
    selectStatus(value: string): void {
        this.selectedStatus.set(value);
        this.currentPage.set(1);
        this.loadData();
    }

    // --- Sort ---
    selectSort(value: string): void {
        this.selectedSort.set(value);
        this.showSortDropdown.set(false);
        this.currentPage.set(1);
        this.loadData();
    }

    getSortLabel(): string {
        return this.sortOptions.find(o => o.value === this.selectedSort())?.label || 'Sort by';
    }

    // --- Multi-select toggle ---
    private toggleInArray(sig: ReturnType<typeof signal<string[]>>, value: string): void {
        const current = sig();
        const next = [...current];
        const idx = next.indexOf(value);
        if (idx >= 0) next.splice(idx, 1); else next.push(value);
        sig.set(next);
        this.currentPage.set(1);
        this.loadData();
    }

    toggleRegion(value: string) { this.toggleInArray(this.selectedRegion, value); }
    toggleFunderType(value: string) { this.toggleInArray(this.selectedFunderType, value); }
    toggleCountry(value: string) { this.toggleInArray(this.selectedCountry, value); }
    toggleAgency(value: string) { this.toggleInArray(this.selectedAgency, value); }
    toggleCategory(value: string) { this.toggleInArray(this.selectedCategories, value); }

    // --- Single-select for deadline/amount ---
    selectDeadline(value: number): void {
        this.selectedDeadline.set(this.selectedDeadline() === value ? 0 : value);
        this.currentPage.set(1);
        this.loadData();
    }

    selectAmount(idx: number): void {
        this.selectedAmountIdx.set(this.selectedAmountIdx() === idx ? 0 : idx);
        this.currentPage.set(1);
        this.loadData();
    }

    // --- Active filter chip removal ---
    removeFilter(chip: { type: string; value: string }): void {
        switch (chip.type) {
            case 'region': this.toggleRegion(chip.value); break;
            case 'funderType': this.toggleFunderType(chip.value); break;
            case 'country': this.toggleCountry(chip.value); break;
            case 'agency': this.toggleAgency(chip.value); break;
            case 'category': this.toggleCategory(chip.value); break;
            case 'deadline': this.selectedDeadline.set(0); this.currentPage.set(1); this.loadData(); break;
            case 'amount': this.selectedAmountIdx.set(0); this.currentPage.set(1); this.loadData(); break;
        }
    }

    clearFilters(): void {
        this.query.set('');
        this.selectedRegion.set([]);
        this.selectedFunderType.set([]);
        this.selectedCountry.set([]);
        this.selectedAgency.set([]);
        this.selectedDeadline.set(0);
        this.selectedAmountIdx.set(0);
        this.selectedCategories.set([]);
        this.selectedSort.set('relevance');
        this.agencySearch.set('');
        this.countrySearch.set('');
        this.currentPage.set(1);
        this.loadData();
    }

    // --- Sidebar section toggles ---
    toggleSidebar(section: string): void {
        switch (section) {
            case 'deadline': this.showSidebarDeadline.update(v => !v); break;
            case 'amount': this.showSidebarAmount.update(v => !v); break;
            case 'agency': this.showSidebarAgency.update(v => !v); break;
            case 'region': this.showSidebarRegion.update(v => !v); break;
            case 'country': this.showSidebarCountry.update(v => !v); break;
            case 'category': this.showSidebarCategory.update(v => !v); break;
        }
    }

    // --- Mobile ---
    toggleMobileSidebar(): void {
        this.mobileSidebarOpen.update(v => !v);
    }

    @HostListener('document:keydown.escape')
    onEsc() {
        if (this.showDetail()) this.closeDetail();
        if (this.mobileSidebarOpen()) this.mobileSidebarOpen.set(false);
    }

    // --- Diagnostics ---
    toggleDiagnostics(): void {
        const next = !this.showDiagnostics();
        this.showDiagnostics.set(next);
        if (!next && this.selectedStatus() === 'needs_review') {
            this.selectedStatus.set('open');
            this.currentPage.set(1);
            this.loadData();
        }
    }

    // --- Pagination ---
    goToPage(page: number): void {
        if (page < 1 || page > this.totalPages()) return;
        this.currentPage.set(page);
        this.loadData();
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }

    getPageNumbers(): number[] {
        const pages: number[] = [];
        const current = this.currentPage();
        const total = this.totalPages();
        const start = Math.max(1, current - 2);
        const end = Math.min(total, current + 2);
        for (let i = start; i <= end; i++) pages.push(i);
        return pages;
    }

    // --- Detail modal ---
    openDetail(opp: Opportunity): void {
        this.selectedOpportunity.set(opp);
        this.showDetail.set(true);
    }

    closeDetail(): void {
        this.showDetail.set(false);
        setTimeout(() => this.selectedOpportunity.set(null), 300);
    }

    visitGrant(url: string): void {
        window.open(url, '_blank');
    }

    // --- Save/unsave ---
    toggleSave(opp: Opportunity, event?: Event): void {
        if (event) event.stopPropagation();
        if (!this.authService.isAuthenticated()) {
            alert('Please log in to save grants.');
            return;
        }
        const isSaved = this.savedIds().has(opp.id);
        const newSet = new Set(this.savedIds());
        if (isSaved) {
            this.opportunityService.unsaveOpportunity(opp.id).subscribe({
                next: () => { newSet.delete(opp.id); this.savedIds.set(newSet); },
                error: (err) => console.error('Error unsaving', err)
            });
        } else {
            this.opportunityService.saveOpportunity(opp.id).subscribe({
                next: () => { newSet.add(opp.id); this.savedIds.set(newSet); },
                error: (err) => console.error('Error saving', err)
            });
        }
    }

    isSaved(id: string): boolean { return this.savedIds().has(id); }

    // --- Formatting ---
    formatAmount(min: number, max: number, currency: string): string {
        if (!max || max === 0) return '';
        const fmt = (n: number) => {
            if (n >= 1000000) return `${(n / 1000000).toFixed(1)}M`;
            if (n >= 1000) return `${(n / 1000).toFixed(0)}K`;
            return n.toLocaleString();
        };
        const sym = currency === 'EUR' ? '€' : currency === 'GBP' ? '£' : '$';
        if (min && min !== max) return `${sym}${fmt(min)} — ${sym}${fmt(max)}`;
        return `${sym}${fmt(max)}`;
    }

    formatShort(amount: number, currency: string): string {
        if (!amount || amount === 0) return '';
        const sym = currency === 'EUR' ? '€' : currency === 'GBP' ? '£' : '$';
        if (amount >= 1000000) return `${sym}${(amount / 1000000).toFixed(1)}M`;
        if (amount >= 1000) return `${sym}${(amount / 1000).toFixed(0)}K`;
        return `${sym}${amount.toLocaleString()}`;
    }

    getDaysUntil(dateStr: string | undefined): string {
        if (!dateStr) return '';
        const diff = Math.ceil((new Date(dateStr).getTime() - Date.now()) / (1000 * 60 * 60 * 24));
        if (diff < 0) return 'Expired';
        if (diff === 0) return 'Today!';
        if (diff === 1) return 'Tomorrow';
        if (diff <= 7) return `${diff} days`;
        if (diff <= 30) return `${Math.ceil(diff / 7)} weeks`;
        return `${Math.ceil(diff / 30)} months`;
    }

    getPrimaryDeadline(opp: Opportunity): string | undefined {
        return opp.next_deadline_at || opp.deadline_at;
    }

    getDeadlineDisplay(opp: Opportunity): string {
        if (opp.is_rolling) return 'Rolling';
        const deadline = this.getPrimaryDeadline(opp);
        if (deadline) return this.getDaysUntil(deadline);
        return 'No deadline';
    }

    getStatusLabel(opp: Opportunity): string {
        const status = (opp.normalized_status || 'needs_review').toLowerCase();
        switch (status) {
            case 'open': return 'Open';
            case 'upcoming': return 'Upcoming';
            case 'closed': return opp.is_results_page ? 'Closed · Results' : 'Closed';
            case 'archived': return opp.is_results_page ? 'Archived · Results' : 'Archived';
            default: return 'Needs review';
        }
    }

    getUrgencyClass(dateStr: string | undefined): string {
        if (!dateStr) return '';
        const diff = Math.ceil((new Date(dateStr).getTime() - Date.now()) / (1000 * 60 * 60 * 24));
        if (diff <= 7) return 'urgent';
        if (diff <= 30) return 'soon';
        return 'relaxed';
    }

    getAmountClass(amount: number): string {
        if (amount >= 1000000) return 'amount-high';
        if (amount >= 100000) return 'amount-mid';
        return 'amount-low';
    }

    getFunderClass(funderType: string): string {
        if (!funderType) return 'other';
        const t = funderType.toLowerCase();
        if (t === 'government') return 'gov';
        if (t === 'foundation') return 'foundation';
        return 'other';
    }

    getPrimaryCTA(opp: Opportunity): string {
        return opp.is_results_page ? 'View Results' : 'Apply';
    }
}

