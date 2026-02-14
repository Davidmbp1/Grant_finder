import { Injectable, signal, effect } from '@angular/core';

@Injectable({
    providedIn: 'root'
})
export class ThemeService {
    darkMode = signal<boolean>(false);

    constructor() {
        // Load saved preference
        const saved = localStorage.getItem('theme');
        if (saved) {
            this.darkMode.set(saved === 'dark');
        } else if (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches) {
            this.darkMode.set(true);
        }

        // Apply theme on change
        effect(() => {
            const isDark = this.darkMode();
            if (isDark) {
                document.documentElement.setAttribute('data-theme', 'dark');
                localStorage.setItem('theme', 'dark');
            } else {
                document.documentElement.setAttribute('data-theme', 'light');
                localStorage.setItem('theme', 'light');
            }
        });
    }

    toggle() {
        this.darkMode.update(v => !v);
    }
}
